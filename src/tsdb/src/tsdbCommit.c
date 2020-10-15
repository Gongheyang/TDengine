/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "tsdbMain.h"
#include "tchecksum.h"

#define TSDB_DATA_FILE_CHANGE 0
#define TSDB_META_FILE_CHANGE 1

typedef struct {
  int          maxIters;
  SCommitIter *pIters;
  SReadHandle *pReadH;
  SFileGroup * pFGroup;
  SBlockIdx *  pBlockIdx;
  SBlockInfo * pBlockInfo;
  SDataCols *  pDataCols;
} STSCommitHandle;

typedef struct {
  int32_t len;
  int32_t type;
  char    change[];
} STsdbFileChange;

typedef struct {
  char       oname[TSDB_FILENAME_LEN];
  char       nname[TSDB_FILENAME_LEN];
  SStoreInfo info;
} SMetaFileChange;

typedef struct {
  SFileGroup ofgroup;
  SFileGroup nfgroup;
} SDataFileChange;

int tsdbCommitData(STsdbRepo *pRepo) {
  ASSERT(pRepo->commit == 1 && pRepo->imem != NULL);

  SCommitHandle  commitHandle = {0};
  SCommitHandle *pCommitH = &commitHandle;

  pCommitH->pRepo = pRepo;

  if (tsdbStartCommit(pCommitH) < 0) return -1;

  if (tsdbCommitTimeSeriesData(pCommitH) < 0) goto _err;

  if (tsdbCommitMetaData(pCommitH) < 0) goto _err;

  tsdbEndCommit(pCommitH, false);

  return 0;

_err:
  tsdbEndCommit(pCommitH, true);
  return -1;
}

static int tsdbStartCommit(SCommitHandle *pCommitH) {
  STsdbRepo *pRepo = pCommitH->pRepo;
  SMemTable *pMem = pRepo->imem;
  STsdbCfg * pCfg = &(pRepo->config);

  tsdbInfo("vgId:%d start to commit! keyFirst %" PRId64 " keyLast %" PRId64 " numOfRows %" PRId64, REPO_ID(pRepo),
           pMem->keyFirst, pMem->keyLast, pMem->numOfRows);

  pCommitH->pModLog = tdListNew(0);
  if (pCommitH->pModLog == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  pCommitH->fd = -1;

  tsdbGetFileName(pRepo->rootDir, TSDB_FILE_TYPE_MANIFEST, pCfg->tsdbId, 0, 0, pCommitH->fname);
  pCommitH->fd = open(pCommitH->fname, O_CREAT | O_WRONLY | O_APPEND, 0755);
  if (pCommitH->fd < 0) {
    tsdbError("vgId:%d failed to open file %s since %s", REPO_ID(pRepo), pCommitH->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  return 0;

_err:
  if (pCommitH->fd >= 0) {
    close(pCommitH->fd);
    pCommitH->fd = -1;
    remove(pCommitH->fname);
  }
  tdListFree(pCommitH->pModLog);
  return -1;
}

static void tsdbEndCommit(SCommitHandle *pCommitH, bool hasError) {
  STsdbRepo *pRepo = pCommitH->pRepo;

  // TODO: append commit over flag
  if (false /* tsdbLogCommitOver(pCommitH) < 0 */) {
    hasError = true;
  }

  tsdbInfo("vgId:%d commit over, commit status: %s", REPO_ID(pRepo), hasError ? "FAILED" : "SUCCEED");

  SListNode *pNode = NULL;

  while ((pNode = tdListPopHead(pCommitH->pModLog)) != NULL) {
    STsdbFileChange *pChange = (STsdbFileChange *)pNode->data;

    tsdbApplyFileChange(pChange, !hasError);
    free(pNode);
  }

  close(pCommitH->fd);
  pCommitH->fd = -1;
  remove(pCommitH->fname);
  tdListFree(pCommitH->pModLog);

  // notify uplayer to delete WAL
  if (!hasError && pRepo->appH.notifyStatus) {
    pRepo->appH.notifyStatus(pRepo->appH.appH, TSDB_STATUS_COMMIT_OVER);
  }
  return;
}

static int tsdbCommitTimeSeriesData(SCommitHandle *pCommitH) {
  STsdbRepo * pRepo = pCommitH->pRepo;
  SMemTable * pMem = pRepo->imem;
  STsdbCfg *  pCfg = &(pRepo->config);
  STsdbMeta * pMeta = pRepo->tsdbMeta;
  STsdbFileH *pFileH = pRepo->tsdbFileH;

  int mfid = tsdbGetCurrMinFid(pCfg->precision, pCfg->keep, pCfg->daysPerFile);
  if (tsdbLogRetentionChange(pCommitH, mfid) < 0) return -1;

  if (pMem->numOfRows <= 0) return 0;

  // Initialize resources
  STSCommitHandle tsCommitH = {0};
  if (tsdbInitTSCommitHandle(&tsCommitH, pRepo) < 0) return -1;

  // Commit Time-Series data file by file
  int sfid = (int)(TSDB_KEY_FILEID(pMem->keyFirst, pCfg->daysPerFile, pCfg->precision));
  int efid = (int)(TSDB_KEY_FILEID(pMem->keyLast, pCfg->daysPerFile, pCfg->precision));

  for (int fid = sfid; fid <= efid; fid++) {
    TSKEY minKey = 0, maxKey = 0;
    tsdbGetFidKeyRange(pCfg->daysPerFile, pCfg->precision, fid, &minKey, &maxKey);

    if (fid < mfid) {
      // Skip data in files beyond retention
      tsdbSeekTSCommitHandle(&tsCommitH, maxKey);
      continue;
    }

    if (!tsdbHasDataToCommit(tsCommitH.pIters, pMem->maxTables, minKey, maxKey)) continue;

    if (tsdbLogTSFileChange(pCommitH, fid) < 0) {
      tsdbDestroyTSCommitHandle(&tsCommitH);
      return -1;
    }

    if (tsdbCommitToFileGroup(pRepo, NULL, NULL, &tsCommitH) < 0) {
      tsdbDestroyTSCommitHandle(&tsCommitH);
      return -1;
    }
  }

  tsdbDestroyTSCommitHandle(&tsCommitH);
  return 0;
}

// Function to commit meta data
static int tsdbCommitMetaData(SCommitHandle *pCommitH) {
  STsdbRepo *pRepo = pCommitH->pRepo;
  SKVStore * pStore = pRepo->tsdbMeta->pStore;
  SMemTable *pMem = pRepo->imem;
  SActObj *  pAct = NULL;
  SActCont * pCont = NULL;

  if (listNEles(pMem->actList) <= 0) return 0;

  // Log meta file change
  if (tsdbLogMetaFileChange(pCommitH) < 0) return -1;
  
  // Commit data
  if (tdKVStoreStartCommit(pStore) < 0) {
    tsdbError("vgId:%d failed to commit data while start commit meta since %s", REPO_ID(pRepo), tstrerror(terrno));
    return -1;
  }

  SListNode *pNode = NULL;

  while ((pNode = tdListPopHead(pMem->actList)) != NULL) {
    pAct = (SActObj *)pNode->data;
    if (pAct->act == TSDB_UPDATE_META) {
      pCont = (SActCont *)POINTER_SHIFT(pAct, sizeof(SActObj));
      if (tdUpdateKVStoreRecord(pStore, pAct->uid, (void *)(pCont->cont), pCont->len) < 0) {
        tsdbError("vgId:%d failed to update meta with uid %" PRIu64 " since %s", REPO_ID(pRepo), pAct->uid,
                  tstrerror(terrno));
        tdKVStoreEndCommit(pStore, true /*hasErro*/);
        return -1;
      }
    } else if (pAct->act == TSDB_DROP_META) {
      if (tdDropKVStoreRecord(pStore, pAct->uid) < 0) {
        tsdbError("vgId:%d failed to drop meta with uid %" PRIu64 " since %s", REPO_ID(pRepo), pAct->uid,
                  tstrerror(terrno));
        tdKVStoreEndCommit(pStore, true /*hasErro*/);
        return -1;
      }
    } else {
      ASSERT(false);
    }
  }

  if (tdKVStoreEndCommit(pMeta->pStore, false /*hasError = false*/) < 0) {
    tsdbError("vgId:%d failed to commit data while end commit meta since %s", REPO_ID(pRepo), tstrerror(terrno));
    return -1;
  }

  return 0;
}

static SCommitIter *tsdbCreateCommitIters(STsdbRepo *pRepo) {
  SMemTable *pMem = pRepo->imem;
  STsdbMeta *pMeta = pRepo->tsdbMeta;

  SCommitIter *iters = (SCommitIter *)calloc(pMem->maxTables, sizeof(SCommitIter));
  if (iters == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return NULL;
  }

  if (tsdbRLockRepoMeta(pRepo) < 0) goto _err;

  // reference all tables
  for (int i = 0; i < pMem->maxTables; i++) {
    if (pMeta->tables[i] != NULL) {
      tsdbRefTable(pMeta->tables[i]);
      iters[i].pTable = pMeta->tables[i];
    }
  }

  if (tsdbUnlockRepoMeta(pRepo) < 0) goto _err;

  for (int i = 0; i < pMem->maxTables; i++) {
    if ((iters[i].pTable != NULL) && (pMem->tData[i] != NULL) && (TABLE_UID(iters[i].pTable) == pMem->tData[i]->uid)) {
      if ((iters[i].pIter = tSkipListCreateIter(pMem->tData[i]->pData)) == NULL) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        goto _err;
      }

      tSkipListIterNext(iters[i].pIter);
    }
  }

  return iters;

_err:
  tsdbDestroyCommitIters(iters, pMem->maxTables);
  return NULL;
}

static void tsdbDestroyCommitIters(SCommitIter *iters, int maxTables) {
  if (iters == NULL) return;

  for (int i = 1; i < maxTables; i++) {
    if (iters[i].pTable != NULL) {
      tsdbUnRefTable(iters[i].pTable);
      tSkipListDestroyIter(iters[i].pIter);
    }
  }

  free(iters);
}

static int tsdbCommitToFileGroup(STsdbRepo *pRepo, SFileGroup *pOldGroup, SFileGroup *pNewGroup, STSCommitHandle *pTSCh) {
  SCommitIter *iters = pTSCh->pIters;

  if (tsdbSetAndOpenCommitFGroup(pTSCh, pOldGroup, pNewGroup) < 0) return -1;

  if (tsdbLoadBlockIdx(pTSCh->pReadH) < 0) {
    tsdbCloseAndUnsetCommitFGroup(pTSCh, true /* hasError = true */);
    return -1;
  }

  for (int tid = 1; tid < pTSCh->maxIters; tid++) {
    if (tsdbCommitTableData(pTSCh, tid) < 0) {
      tsdbCloseAndUnsetCommitFGroup(pTSCh, true /* hasError = true */);
      return -1;
    }

    if (tsdbTryMoveLastBlock(pTSCh) < 0) {
      tsdbCloseAndUnsetCommitFGroup(pTSCh, true /* hasError = true */);
      return -1;
    }

    if (tsdbWriteBlockInfo(pWHelper) < 0) {
      tsdbCloseAndUnsetCommitFGroup(pTSCh, true /* hasError = true */);
      return -1;
    }
  }

  if (tsdbWriteBlockIdx(pWHelper) < 0) {
    tsdbCloseAndUnsetCommitFGroup(pTSCh, true /* hasError = true */);
    return -1;
  }

  tsdbCloseAndUnsetCommitFGroup(pTSCh, false /* hasError = true */);

  return 0;
}

static int tsdbHasDataToCommit(SCommitIter *iters, int nIters, TSKEY minKey, TSKEY maxKey) {
  for (int i = 0; i < nIters; i++) {
    SCommitIter *pIter = iters + i;
    if (pIter->pTable == NULL) continue;

    TSKEY nextKey = tsdbNextIterKey(pIter->pIter);
    if (nextKey > 0 && (nextKey >= minKey && nextKey <= maxKey)) return 1;
  }
  return 0;
}

static int tsdbInitTSCommitHandle(STSCommitHandle *pTSCh, STsdbRepo *pRepo) {
  STsdbCfg * pCfg = &(pRepo->config);
  STsdbMeta *pMeta = pRepo->tsdbMeta;
  SMemTable *pMem = pRepo->imem;

  pTSCh->pIters = tsdbCreateCommitIters(pRepo);
  if (pTSCh->pIters == NULL) {
    tsdbError("vgId:%d failed to create commit iterator since %s", REPO_ID(pRepo), tstrerror(terrno));
    tsdbDestroyTSCommitHandle(pTSCh);
    return -1;
  }
  pTSCh->maxIters = pMem->maxTables;

  pTSCh->pReadH = tsdbNewReadHandle(pRepo);
  if (pTSCh->pReadH == NULL) {
    tsdbError("vgId:%d failed to create new read handle since %s", REPO_ID(pRepo), tstrerror(terrno));
    tsdbDestroyTSCommitHandle(pTSCh);
    return -1;
  }

  pTSCh->pDataCols = tdNewDataCols(pMeta->maxRowBytes, pMeta->maxCols, pCfg->maxRowsPerFileBlock);
  if (pTSCh->pDataCols == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbError("vgId:%d failed to init data cols with maxRowBytes %d maxCols %d maxRowsPerFileBlock %d since %s",
              REPO_ID(pRepo), pMeta->maxCols, pMeta->maxRowBytes, pCfg->maxRowsPerFileBlock, tstrerror(terrno));
    tsdbDestroyTSCommitHandle(pTSCh);
    return -1;
  }

  return 0;
}

static void tsdbDestroyTSCommitHandle(STSCommitHandle *pTSCh) {
  if (pTSCh) {
    tdFreeDataCols(pTSCh->pDataCols);
    tsdbFreeReadHandle(pTSCh->pReadH);
    tsdbDestroyCommitIters(pTSCh->pIters, pTSCh->maxIters);
  }
}

static int tsdbLogFileChange(SCommitHandle *pCommitH, STsdbFileChange *pChange) {
  STsdbRepo *pRepo = pCommitH->pRepo;

  pChange->len = tsdbEncodeFileChange(NULL, pChange) + sizeof(TSCKSUM);

  if ((pCommitH->pBuffer = taosTRealloc(pCommitH->pBuffer, pChange->len)) == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  if (taosTWrite(pCommitH->fd, (void *)pChange, sizeof(*pChange)) < sizeof(*pChange)) {
    tsdbError("vgId:%d failed to write file change to file %s since %s", REPO_ID(pRepo), pCommitH->fname,
              strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  int tsize = tsdbEncodeFileChange(pCommitH->pBuffer, pChange);
  ASSERT(tsize + sizeof(TSCKSUM) == pChange->len);

  taosCalcChecksumAppend(0, pCommitH->pBuffer, pChange->len);

  if (taosTWrite(pCommitH->fd, pCommitH->pBuffer, pChange->len) < pChange->len) {
    tsdbError("vgId:%d failed to write file change encode to file %s, bytes %d since %s", REPO_ID(pRepo),
              pCommitH->fname, pChange->len, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (fsync(pCommitH->fd) < 0) {
    tsdbError("vgId:%d failed to fsync file %s since %s", REPO_ID(pRepo), pCommitH->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return 0;
}

static int tsdbEncodeFileChange(void **buf, STsdbFileChange *pChange) {
  int tsize = 0;
  if (pChange->type == TSDB_META_FILE_CHANGE) {
    SMetaFileChange *pMetaChange = (SMetaFileChange *)pChange->change;

    tsize += taosEncodeString(buf, pMetaChange->oname);
    tsize += taosEncodeString(buf, pMetaChange->nname);
    tsize += tdEncodeStoreInfo(buf, pMetaChange->info);
  } else if (pChange->type == TSDB_DATA_FILE_CHANGE) {
    SDataFileChange *pDataChange = (SDataFileChange *)pChange->change;

    tsize += tsdbEncodeSFileGroup(buf, &(pDataChange->ofgroup));
    tsize += tsdbEncodeSFileGroup(buf, &(pDataChange->nfgroup));
  } else {
    ASSERT(false);
  }

  return tsize;
}

static void *tsdbDecodeFileChange(void *buf, STsdbFileChange *pChange) {
  // TODO
  return buf;
}

static int tsdbLogTSFileChange(SCommitHandle *pCommitH, int fid) {
  STsdbRepo * pRepo = pCommitH->pRepo;
  STsdbFileH *pFileH = pRepo->tsdbFileH;

  SListNode *pNode = (SListNode *)calloc(1, sizeof(SListNode) + sizeof(STsdbFileChange) + sizeof(SDataFileChange));
  if (pNode == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  STsdbFileChange *pChange = (STsdbFileChange *)pNode->data;
  pChange->type = TSDB_DATA_FILE_CHANGE;

  SDataFileChange *pDataFileChange = (SDataFileChange *)pChange->change;

  SFileGroup *pFGroup = tsdbSearchFGroup(pFileH, fid, TD_EQ);
  if (pFGroup == NULL) {
    pDataFileChange->ofgroup.fileId = fid;
  } else {
    pDataFileChange->ofgroup = *pFGroup;
  }

  tsdbGetNextCommitFileGroup(&(pDataFileChange->ofgroup), &(pDataFileChange->nfgroup));

  if (tsdbLogFileChange(pCommitH, pChange) < 0) {
    free(pNode);
    return -1;
  }

  tdListAppendNode(pCommitH->pModLog, pNode);

  return 0;
}

static int tsdbLogMetaFileChange(SCommitHandle *pCommitH) {
  STsdbRepo *pRepo = pCommitH->pRepo;
  SKVStore * pStore = pRepo->tsdbMeta->pStore;

  SListNode *pNode = (SListNode *)calloc(1, sizeof(SListNode) + sizeof(STsdbFileChange) + sizeof(SMetaFileChange));
  if (pNode == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  STsdbFileChange *pChange = pNode->data;
  pChange->type = TSDB_META_FILE_CHANGE;

  SMetaFileChange *pMetaChange = (SMetaFileChange *)(pChange->change);
  strncpy(pMetaChange->oname, pStore->fname, TSDB_FILENAME_LEN);
  strncpy(pMetaChange->nname, pStore->fname, TSDB_FILENAME_LEN);
  pMetaChange->info = pStore->info;

  if (tsdbLogFileChange(pCommitH, pChange) < 0) {
    free(pNode);
    return -1;
  }
  tdListAppendNode(pCommitH->pModLog, pNode);

  return 0;
}

static int tsdbLogRetentionChange(SCommitHandle *pCommitH, int mfid) {
  STsdbRepo * pRepo = pCommitH->pRepo;
  STsdbFileH *pFileH = pRepo->tsdbFileH;

  for (int i = 0; i < pFileH->nFGroups; i++) {
    SFileGroup *pFGroup = pFileH->pFGroup[i];
    if (pFGroup->fileId < mfid) {
      SListNode *pNode = (SListNode *)calloc(1, sizeof(SListNode) + sizeof(STsdbFileChange) + sizeof(SDataFileChange));
      if (pNode == NULL) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        return -1;
      }

      STsdbFileChange *pChange = (STsdbFileChange *)pNode->data;
      pChange->type = TSDB_DATA_FILE_CHANGE;

      SDataFileChange *pDataFileChange = (SDataFileChange *)pChange->change;
      pDataFileChange->ofgroup = pFGroup;

      if (tsdbLogFileChange(pCommitH, pChange) < 0) {
        free(pNode);
        return -1;
      }
      tdListAppendNode(pCommitH->pModLog, &pChange);
    } else {
      break;
    }
  }

  return 0;
}

static int tsdbApplyFileChange(STsdbFileChange *pChange, bool isCommitEnd) {
  if (pChange->type == TSDB_META_FILE_CHANGE) {
    SMetaFileChange *pMetaChange = (SMetaFileChange *)pChange->change;

    if (isCommitEnd) {
      if (strncmp(pMetaChange->oname, pMetaChange->nname) != 0) {
        (void)remove(pMetaChange->oname);
      }
    } else { // roll back
      // TODO
    }
  } else if (pChange->len == TSDB_DATA_FILE_CHANGE) {

  } else {
    ASSERT(0);
  }

  return 0;
}

static void tsdbSeekTSCommitHandle(STSCommitHandle *pTSCh, TSKEY key) {
  for (int tid = 1; tid < pTSCh->maxIters; tid++) {
    SCommitIter *pIter = pTSCh->pIters + tid;
    if (pIter->pTable == NULL) continue;

    while (tsdbLoadDataFromCache(pIter->pTable, pIter->pIter, key, INT32_MAX, NULL, NULL, 0) != 0) {
    }
  }
}

static int tsdbEncodeSFileGroup(void **buf, SFileGroup *pFGroup) {
  int tsize = 0;

  tsize += taosEncodeVariantI32(buf, pFGroup->fileId);
  for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
    SFile *pFile = &(pFGroup->files[type]);

    tsize += taosEncodeString(buf, pFile->fname);
    tsize += tsdbEncodeSFileInfo(buf, &pFile->info);
  }

  return tsize;
}

static void *tsdbDecodeSFileGroup(void *buf, SFileGroup *pFGroup) {
  buf = taosDecodeVariantI32(buf, &(pFGroup->fileId));

  for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
    SFile *pFile = &(pFGroup->files[type]);

    buf = taosDecodeString(buf, &(pFile->fname));
    buf = tsdbDecodeSFileInfo(buf, &(pFile->info));
  }

  return buf;
}

static void tsdbGetNextCommitFileGroup(SFileGroup *pOldGroup, SFileGroup *pNewGroup) {
  pNewGroup->fileId = pOldGroup->fileId;

  for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
    SFile *pOldFile = &(pOldGroup->files[type]);
    SFile *pNewFile = &(pNewGroup->files[type]);

    size_t len =strlen(pOldFile->fname);
    if (len == 0 || pOldFile->fname[len - 1] == '1') {
      tsdbGetFileName(pRepo->rootDir, type, vid, pOldGroup->fileId, 0, pNewFile->fname);
    } else {
      tsdbGetFileName(pRepo->rootDir, type, vid, pOldGroup->fileId, 1, pNewFile->fname);
    }
  }
}

static int tsdbCommitTableData(STSCommitHandle *pTSCh, int tid) {
  SCommitIter *pIter = pTSCh->pIters + tid;
  if (pIter->pTable == NULL) return 0;

  taosRLockLatch(&(pIter->pTable->latch));

  if (pIter->pIter == NULL) {
    // TODO
  }

  if (tdInitDataCols(pTSCh->pDataCols, tsdbGetTableSchemaImpl(pIter->pTable, false, false, -1)) < 0) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  if (tsdbReadBlockInfo() < 0) {
    goto _err;
  }

  while (true) {
    TSKEY keyNext = tsdbNextIterKey(pIter->pIter);
    if (keyNext < 0 || keyNext > maxKey) break;

    if (/* no block info exists*/ || keyNext > pIdx->maxKey) {
      if (tsdbProcessAppendCommit() < 0) goto _err;
    } else {
      if (tsdbProcessMergeCommit() < 0) goto _err;
    }

  }

  taosRUnLockLatch(&(pIter->pTable->latch));
  return 0;

_err:
  taosRUnLockLatch(&(pIter->pTable->latch));
  return -1;
}

static int tsdbProcessAppendCommit(SRWHelper *pHelper, SCommitIter *pCommitIter, SDataCols *pDataCols, TSKEY maxKey) {
  STsdbCfg * pCfg = &(pHelper->pRepo->config);
  STable *   pTable = pCommitIter->pTable;
  SBlockIdx * pIdx = &(pHelper->curCompIdx);
  TSKEY      keyFirst = tsdbNextIterKey(pCommitIter->pIter);
  int        defaultRowsInBlock = pCfg->maxRowsPerFileBlock * 4 / 5;
  SBlock compBlock = {0};

  ASSERT(pIdx->len <= 0 || keyFirst > pIdx->maxKey);
  if (pIdx->hasLast) {  // append to with last block
    ASSERT(pIdx->len > 0);
    SBlock *pCompBlock = blockAtIdx(pHelper, pIdx->numOfBlocks - 1);
    ASSERT(pCompBlock->last && pCompBlock->numOfRows < pCfg->minRowsPerFileBlock);
    tdResetDataCols(pDataCols);
    int rowsRead = tsdbLoadDataFromCache(pTable, pCommitIter->pIter, maxKey, defaultRowsInBlock - pCompBlock->numOfRows,
                                         pDataCols, NULL, 0);
    ASSERT(rowsRead > 0 && rowsRead == pDataCols->numOfRows);
    if (rowsRead + pCompBlock->numOfRows < pCfg->minRowsPerFileBlock &&
        pCompBlock->numOfSubBlocks < TSDB_MAX_SUBBLOCKS && !TSDB_NLAST_FILE_OPENED(pHelper)) {
      if (tsdbWriteBlockToFile(pHelper, helperLastF(pHelper), pDataCols, &compBlock, true, false) < 0) return -1;
      if (tsdbAddSubBlock(pHelper, &compBlock, pIdx->numOfBlocks - 1, rowsRead) < 0) return -1;
    } else {
      if (tsdbLoadBlockData(pHelper, pCompBlock, NULL) < 0) return -1;
      ASSERT(pHelper->pDataCols[0]->numOfRows == pCompBlock->numOfRows);

      if (tdMergeDataCols(pHelper->pDataCols[0], pDataCols, pDataCols->numOfRows) < 0) return -1;
      ASSERT(pHelper->pDataCols[0]->numOfRows == pCompBlock->numOfRows + pDataCols->numOfRows);

      if (tsdbWriteBlockToProperFile(pHelper, pHelper->pDataCols[0], &compBlock) < 0) return -1;
      if (tsdbUpdateSuperBlock(pHelper, &compBlock, pIdx->numOfBlocks - 1) < 0) return -1;
    }

    if (pHelper->hasOldLastBlock) pHelper->hasOldLastBlock = false;
  } else {
    ASSERT(!pHelper->hasOldLastBlock);
    tdResetDataCols(pDataCols);
    int rowsRead = tsdbLoadDataFromCache(pTable, pCommitIter->pIter, maxKey, defaultRowsInBlock, pDataCols, NULL, 0);
    ASSERT(rowsRead > 0 && rowsRead == pDataCols->numOfRows);

    if (tsdbWriteBlockToProperFile(pHelper, pDataCols, &compBlock) < 0) return -1;
    if (tsdbInsertSuperBlock(pHelper, &compBlock, pIdx->numOfBlocks) < 0) return -1;
  }

#ifndef NDEBUG
  TSKEY keyNext = tsdbNextIterKey(pCommitIter->pIter);
  ASSERT(keyNext < 0 || keyNext > pIdx->maxKey);
#endif

  return 0;
}

static int tsdbProcessMergeCommit(SRWHelper *pHelper, SCommitIter *pCommitIter, SDataCols *pDataCols, TSKEY maxKey,
                                  int *blkIdx) {
  STsdbCfg * pCfg = &(pHelper->pRepo->config);
  STable *   pTable = pCommitIter->pTable;
  SBlockIdx * pIdx = &(pHelper->curCompIdx);
  SBlock compBlock = {0};
  TSKEY      keyFirst = tsdbNextIterKey(pCommitIter->pIter);
  int        defaultRowsInBlock = pCfg->maxRowsPerFileBlock * 4 / 5;
  SDataCols *pDataCols0 = pHelper->pDataCols[0];

  SSkipListIterator slIter = {0};

  ASSERT(keyFirst <= pIdx->maxKey);

  SBlock *pCompBlock = taosbsearch((void *)(&keyFirst), (void *)blockAtIdx(pHelper, *blkIdx),
                                       pIdx->numOfBlocks - *blkIdx, sizeof(SBlock), compareKeyBlock, TD_GE);
  ASSERT(pCompBlock != NULL);
  int tblkIdx = (int32_t)(TSDB_GET_COMPBLOCK_IDX(pHelper, pCompBlock));

  if (pCompBlock->last) {
    ASSERT(pCompBlock->numOfRows < pCfg->minRowsPerFileBlock && tblkIdx == pIdx->numOfBlocks - 1);
    int16_t colId = 0;
    slIter = *(pCommitIter->pIter);
    if (tsdbLoadBlockDataCols(pHelper, pCompBlock, NULL, &colId, 1) < 0) return -1;
    ASSERT(pDataCols0->numOfRows == pCompBlock->numOfRows);

    int rows1 = defaultRowsInBlock - pCompBlock->numOfRows;
    int rows2 =
        tsdbLoadDataFromCache(pTable, &slIter, maxKey, rows1, NULL, pDataCols0->cols[0].pData, pDataCols0->numOfRows);
    if (rows2 == 0) {  // all data filtered out
      *(pCommitIter->pIter) = slIter;
    } else {
      if (pCompBlock->numOfRows + rows2 < pCfg->minRowsPerFileBlock &&
          pCompBlock->numOfSubBlocks < TSDB_MAX_SUBBLOCKS && !TSDB_NLAST_FILE_OPENED(pHelper)) {
        tdResetDataCols(pDataCols);
        int rowsRead = tsdbLoadDataFromCache(pTable, pCommitIter->pIter, maxKey, rows1, pDataCols,
                                             pDataCols0->cols[0].pData, pDataCols0->numOfRows);
        ASSERT(rowsRead == rows2 && rowsRead == pDataCols->numOfRows);
        if (tsdbWriteBlockToFile(pHelper, helperLastF(pHelper), pDataCols, &compBlock, true, false) < 0) return -1;
        if (tsdbAddSubBlock(pHelper, &compBlock, tblkIdx, rowsRead) < 0) return -1;
        tblkIdx++;
      } else {
        if (tsdbLoadBlockData(pHelper, pCompBlock, NULL) < 0) return -1;
        int round = 0;
        int dIter = 0;
        while (true) {
          tdResetDataCols(pDataCols);
          int rowsRead =
              tsdbLoadAndMergeFromCache(pDataCols0, &dIter, pCommitIter, pDataCols, maxKey, defaultRowsInBlock);
          if (rowsRead == 0) break;

          if (tsdbWriteBlockToProperFile(pHelper, pDataCols, &compBlock) < 0) return -1;
          if (round == 0) {
            if (tsdbUpdateSuperBlock(pHelper, &compBlock, tblkIdx) < 0) return -1;
          } else {
            if (tsdbInsertSuperBlock(pHelper, &compBlock, tblkIdx) < 0) return -1;
          }

          tblkIdx++;
          round++;
        }
      }
      if (pHelper->hasOldLastBlock) pHelper->hasOldLastBlock = false;
    }
  } else {
    TSKEY keyLimit = (tblkIdx == pIdx->numOfBlocks - 1) ? maxKey : (pCompBlock[1].keyFirst - 1);
    TSKEY blkKeyFirst = pCompBlock->keyFirst;
    TSKEY blkKeyLast = pCompBlock->keyLast;

    if (keyFirst < blkKeyFirst) {
      while (true) {
        tdResetDataCols(pDataCols);
        int rowsRead =
            tsdbLoadDataFromCache(pTable, pCommitIter->pIter, blkKeyFirst - 1, defaultRowsInBlock, pDataCols, NULL, 0);
        if (rowsRead == 0) break;

        ASSERT(rowsRead == pDataCols->numOfRows);
        if (tsdbWriteBlockToFile(pHelper, helperDataF(pHelper), pDataCols, &compBlock, false, true) < 0) return -1;
        if (tsdbInsertSuperBlock(pHelper, &compBlock, tblkIdx) < 0) return -1;
        tblkIdx++;
      }
      ASSERT(tblkIdx == 0 || (tsdbNextIterKey(pCommitIter->pIter) < 0 ||
                              tsdbNextIterKey(pCommitIter->pIter) > blockAtIdx(pHelper, tblkIdx - 1)->keyLast));
    } else {
      ASSERT(keyFirst <= blkKeyLast);
      int16_t colId = 0;
      if (tsdbLoadBlockDataCols(pHelper, pCompBlock, NULL, &colId, 1) < 0) return -1;

      slIter = *(pCommitIter->pIter);
      int rows1 = (pCfg->maxRowsPerFileBlock - pCompBlock->numOfRows);
      int rows2 = tsdbLoadDataFromCache(pTable, &slIter, blkKeyLast, INT_MAX, NULL, pDataCols0->cols[0].pData,
                                        pDataCols0->numOfRows);

      if (rows2 == 0) {  // all filtered out
        *(pCommitIter->pIter) = slIter;
        ASSERT(tblkIdx == 0 || (tsdbNextIterKey(pCommitIter->pIter) < 0 ||
                                tsdbNextIterKey(pCommitIter->pIter) > blockAtIdx(pHelper, tblkIdx - 1)->keyLast));
      } else {
        int rows3 = tsdbLoadDataFromCache(pTable, &slIter, keyLimit, INT_MAX, NULL, NULL, 0) + rows2;

        if (pCompBlock->numOfSubBlocks < TSDB_MAX_SUBBLOCKS && rows1 >= rows2) {
          int rows = (rows1 >= rows3) ? rows3 : rows2;
          tdResetDataCols(pDataCols);
          int rowsRead = tsdbLoadDataFromCache(pTable, pCommitIter->pIter, keyLimit, rows, pDataCols,
                                               pDataCols0->cols[0].pData, pDataCols0->numOfRows);
          ASSERT(rowsRead == rows && rowsRead == pDataCols->numOfRows);
          if (tsdbWriteBlockToFile(pHelper, helperDataF(pHelper), pDataCols, &compBlock, false, false) < 0)
            return -1;
          if (tsdbAddSubBlock(pHelper, &compBlock, tblkIdx, rowsRead) < 0) return -1;
          tblkIdx++;
          ASSERT(tblkIdx == 0 || (tsdbNextIterKey(pCommitIter->pIter) < 0 ||
                                  tsdbNextIterKey(pCommitIter->pIter) > blockAtIdx(pHelper, tblkIdx - 1)->keyLast));
        } else {
          if (tsdbLoadBlockData(pHelper, pCompBlock, NULL) < 0) return -1;
          int round = 0;
          int dIter = 0;
          while (true) {
            int rowsRead =
                tsdbLoadAndMergeFromCache(pDataCols0, &dIter, pCommitIter, pDataCols, keyLimit, defaultRowsInBlock);
            if (rowsRead == 0) break;

            if (tsdbWriteBlockToFile(pHelper, helperDataF(pHelper), pDataCols, &compBlock, false, true) < 0)
              return -1;
            if (round == 0) {
              if (tsdbUpdateSuperBlock(pHelper, &compBlock, tblkIdx) < 0) return -1;
            } else {
              if (tsdbInsertSuperBlock(pHelper, &compBlock, tblkIdx) < 0) return -1;
            }

            round++;
            tblkIdx++;
          }
          ASSERT(tblkIdx == 0 || (tsdbNextIterKey(pCommitIter->pIter) < 0 ||
                                  tsdbNextIterKey(pCommitIter->pIter) > blockAtIdx(pHelper, tblkIdx - 1)->keyLast));
        }
      }
    }
  }

  *blkIdx = tblkIdx;
  return 0;
}

static int tsdbLoadAndMergeFromCache(SDataCols *pDataCols, int *iter, SCommitIter *pCommitIter, SDataCols *pTarget,
                                     TSKEY maxKey, int maxRows) {
  int       numOfRows = 0;
  TSKEY     key1 = INT64_MAX;
  TSKEY     key2 = INT64_MAX;
  STSchema *pSchema = NULL;

  ASSERT(maxRows > 0 && dataColsKeyLast(pDataCols) <= maxKey);
  tdResetDataCols(pTarget);

  while (true) {
    key1 = (*iter >= pDataCols->numOfRows) ? INT64_MAX : dataColsKeyAt(pDataCols, *iter);
    SDataRow row = tsdbNextIterRow(pCommitIter->pIter);
    key2 = (row == NULL || dataRowKey(row) > maxKey) ? INT64_MAX : dataRowKey(row);

    if (key1 == INT64_MAX && key2 == INT64_MAX) break;

    if (key1 <= key2) {
      for (int i = 0; i < pDataCols->numOfCols; i++) {
        dataColAppendVal(pTarget->cols + i, tdGetColDataOfRow(pDataCols->cols + i, *iter), pTarget->numOfRows,
                         pTarget->maxPoints);
      }
      pTarget->numOfRows++;
      (*iter)++;
      if (key1 == key2) tSkipListIterNext(pCommitIter->pIter);
    } else {
      if (pSchema == NULL || schemaVersion(pSchema) != dataRowVersion(row)) {
        pSchema = tsdbGetTableSchemaImpl(pCommitIter->pTable, false, false, dataRowVersion(row));
        ASSERT(pSchema != NULL);
      }

      tdAppendDataRowToDataCol(row, pSchema, pTarget);
      tSkipListIterNext(pCommitIter->pIter);
    }

    numOfRows++;
    if (numOfRows >= maxRows) break;
    ASSERT(numOfRows == pTarget->numOfRows && numOfRows <= pTarget->maxPoints);
  }

  return numOfRows;
}

static int tsdbWriteBlockToProperFile(SRWHelper *pHelper, SDataCols *pDataCols, SBlock *pCompBlock) {
  STsdbCfg *pCfg = &(pHelper->pRepo->config);
  SFile *   pFile = NULL;
  bool      isLast = false;

  ASSERT(pDataCols->numOfRows > 0);

  if (pDataCols->numOfRows >= pCfg->minRowsPerFileBlock) {
    pFile = helperDataF(pHelper);
  } else {
    isLast = true;
    pFile = TSDB_NLAST_FILE_OPENED(pHelper) ? helperNewLastF(pHelper) : helperLastF(pHelper);
  }

  ASSERT(pFile->fd > 0);

  if (tsdbWriteBlockToFile(pHelper, pFile, pDataCols, pCompBlock, isLast, true) < 0) return -1;

  return 0;
}

static int tsdbInsertSuperBlock(SRWHelper *pHelper, SBlock *pCompBlock, int blkIdx) {
  SBlockIdx *pIdx = &(pHelper->curCompIdx);

  ASSERT(blkIdx >= 0 && blkIdx <= (int)pIdx->numOfBlocks);
  ASSERT(pCompBlock->numOfSubBlocks == 1);

  // Adjust memory if no more room
  if (pIdx->len == 0) pIdx->len = sizeof(SBlockInfo) + sizeof(TSCKSUM);
  if (tsdbAdjustInfoSizeIfNeeded(pHelper, pIdx->len + sizeof(SBlockInfo)) < 0) goto _err;

  // Change the offset
  for (uint32_t i = 0; i < pIdx->numOfBlocks; i++) {
    SBlock *pTCompBlock = &pHelper->pCompInfo->blocks[i];
    if (pTCompBlock->numOfSubBlocks > 1) pTCompBlock->offset += sizeof(SBlock);
  }

  // Memmove if needed
  int tsize = pIdx->len - (sizeof(SBlockInfo) + sizeof(SBlock) * blkIdx);
  if (tsize > 0) {
    ASSERT(sizeof(SBlockInfo) + sizeof(SBlock) * (blkIdx + 1) < taosTSizeof(pHelper->pCompInfo));
    ASSERT(sizeof(SBlockInfo) + sizeof(SBlock) * (blkIdx + 1) + tsize <= taosTSizeof(pHelper->pCompInfo));
    memmove(POINTER_SHIFT(pHelper->pCompInfo, sizeof(SBlockInfo) + sizeof(SBlock) * (blkIdx + 1)),
            POINTER_SHIFT(pHelper->pCompInfo, sizeof(SBlockInfo) + sizeof(SBlock) * blkIdx), tsize);
  }
  pHelper->pCompInfo->blocks[blkIdx] = *pCompBlock;

  pIdx->numOfBlocks++;
  pIdx->len += sizeof(SBlock);
  ASSERT(pIdx->len <= taosTSizeof(pHelper->pCompInfo));
  pIdx->maxKey = blockAtIdx(pHelper, pIdx->numOfBlocks - 1)->keyLast;
  pIdx->hasLast = (uint32_t)blockAtIdx(pHelper, pIdx->numOfBlocks - 1)->last;

  if (pIdx->numOfBlocks > 1) {
    ASSERT(pHelper->pCompInfo->blocks[0].keyLast < pHelper->pCompInfo->blocks[1].keyFirst);
  }

  tsdbDebug("vgId:%d tid:%d a super block is inserted at index %d", REPO_ID(pHelper->pRepo), pHelper->tableInfo.tid,
            blkIdx);

  return 0;

_err:
  return -1;
}

static int tsdbAddSubBlock(SRWHelper *pHelper, SBlock *pCompBlock, int blkIdx, int rowsAdded) {
  ASSERT(pCompBlock->numOfSubBlocks == 0);

  SBlockIdx *pIdx = &(pHelper->curCompIdx);
  ASSERT(blkIdx >= 0 && blkIdx < (int)pIdx->numOfBlocks);

  SBlock *pSCompBlock = pHelper->pCompInfo->blocks + blkIdx;
  ASSERT(pSCompBlock->numOfSubBlocks >= 1 && pSCompBlock->numOfSubBlocks < TSDB_MAX_SUBBLOCKS);

  size_t spaceNeeded =
      (pSCompBlock->numOfSubBlocks == 1) ? pIdx->len + sizeof(SBlock) * 2 : pIdx->len + sizeof(SBlock);
  if (tsdbAdjustInfoSizeIfNeeded(pHelper, spaceNeeded) < 0) goto _err;

  pSCompBlock = pHelper->pCompInfo->blocks + blkIdx;

  // Add the sub-block
  if (pSCompBlock->numOfSubBlocks > 1) {
    size_t tsize = (size_t)(pIdx->len - (pSCompBlock->offset + pSCompBlock->len));
    if (tsize > 0) {
      memmove((void *)((char *)(pHelper->pCompInfo) + pSCompBlock->offset + pSCompBlock->len + sizeof(SBlock)),
              (void *)((char *)(pHelper->pCompInfo) + pSCompBlock->offset + pSCompBlock->len), tsize);

      for (uint32_t i = blkIdx + 1; i < pIdx->numOfBlocks; i++) {
        SBlock *pTCompBlock = &pHelper->pCompInfo->blocks[i];
        if (pTCompBlock->numOfSubBlocks > 1) pTCompBlock->offset += sizeof(SBlock);
      }
    }

    *(SBlock *)((char *)(pHelper->pCompInfo) + pSCompBlock->offset + pSCompBlock->len) = *pCompBlock;

    pSCompBlock->numOfSubBlocks++;
    ASSERT(pSCompBlock->numOfSubBlocks <= TSDB_MAX_SUBBLOCKS);
    pSCompBlock->len += sizeof(SBlock);
    pSCompBlock->numOfRows += rowsAdded;
    pSCompBlock->keyFirst = MIN(pSCompBlock->keyFirst, pCompBlock->keyFirst);
    pSCompBlock->keyLast = MAX(pSCompBlock->keyLast, pCompBlock->keyLast);
    pIdx->len += sizeof(SBlock);
  } else {  // Need to create two sub-blocks
    void *ptr = NULL;
    for (uint32_t i = blkIdx + 1; i < pIdx->numOfBlocks; i++) {
      SBlock *pTCompBlock = pHelper->pCompInfo->blocks + i;
      if (pTCompBlock->numOfSubBlocks > 1) {
        ptr = POINTER_SHIFT(pHelper->pCompInfo, pTCompBlock->offset);
        break;
      }
    }

    if (ptr == NULL) ptr = POINTER_SHIFT(pHelper->pCompInfo, pIdx->len - sizeof(TSCKSUM));

    size_t tsize = pIdx->len - ((char *)ptr - (char *)(pHelper->pCompInfo));
    if (tsize > 0) {
      memmove(POINTER_SHIFT(ptr, sizeof(SBlock) * 2), ptr, tsize);
      for (uint32_t i = blkIdx + 1; i < pIdx->numOfBlocks; i++) {
        SBlock *pTCompBlock = pHelper->pCompInfo->blocks + i;
        if (pTCompBlock->numOfSubBlocks > 1) pTCompBlock->offset += (sizeof(SBlock) * 2);
      }
    }

    ((SBlock *)ptr)[0] = *pSCompBlock;
    ((SBlock *)ptr)[0].numOfSubBlocks = 0;

    ((SBlock *)ptr)[1] = *pCompBlock;

    pSCompBlock->numOfSubBlocks = 2;
    pSCompBlock->numOfRows += rowsAdded;
    pSCompBlock->offset = ((char *)ptr) - ((char *)pHelper->pCompInfo);
    pSCompBlock->len = sizeof(SBlock) * 2;
    pSCompBlock->keyFirst = MIN(((SBlock *)ptr)[0].keyFirst, ((SBlock *)ptr)[1].keyFirst);
    pSCompBlock->keyLast = MAX(((SBlock *)ptr)[0].keyLast, ((SBlock *)ptr)[1].keyLast);

    pIdx->len += (sizeof(SBlock) * 2);
  }

  pIdx->maxKey = pHelper->pCompInfo->blocks[pIdx->numOfBlocks - 1].keyLast;
  pIdx->hasLast = (uint32_t)pHelper->pCompInfo->blocks[pIdx->numOfBlocks - 1].last;

  tsdbDebug("vgId:%d tid:%d a subblock is added at index %d", REPO_ID(pHelper->pRepo), pHelper->tableInfo.tid, blkIdx);

  return 0;

_err:
  return -1;
}

static int tsdbUpdateSuperBlock(SRWHelper *pHelper, SBlock *pCompBlock, int blkIdx) {
  ASSERT(pCompBlock->numOfSubBlocks == 1);

  SBlockIdx *pIdx = &(pHelper->curCompIdx);

  ASSERT(blkIdx >= 0 && blkIdx < (int)pIdx->numOfBlocks);

  SBlock *pSCompBlock = pHelper->pCompInfo->blocks + blkIdx;

  ASSERT(pSCompBlock->numOfSubBlocks >= 1);

  // Delete the sub blocks it has
  if (pSCompBlock->numOfSubBlocks > 1) {
    size_t tsize = (size_t)(pIdx->len - (pSCompBlock->offset + pSCompBlock->len));
    if (tsize > 0) {
      memmove(POINTER_SHIFT(pHelper->pCompInfo, pSCompBlock->offset),
              POINTER_SHIFT(pHelper->pCompInfo, pSCompBlock->offset + pSCompBlock->len), tsize);
    }

    for (uint32_t i = blkIdx + 1; i < pIdx->numOfBlocks; i++) {
      SBlock *pTCompBlock = &pHelper->pCompInfo->blocks[i];
      if (pTCompBlock->numOfSubBlocks > 1) pTCompBlock->offset -= (sizeof(SBlock) * pSCompBlock->numOfSubBlocks);
    }

    pIdx->len -= (sizeof(SBlock) * pSCompBlock->numOfSubBlocks);
  }

  *pSCompBlock = *pCompBlock;

  pIdx->maxKey = blockAtIdx(pHelper, pIdx->numOfBlocks - 1)->keyLast;
  pIdx->hasLast = (uint32_t)blockAtIdx(pHelper, pIdx->numOfBlocks - 1)->last;

  tsdbDebug("vgId:%d tid:%d a super block is updated at index %d", REPO_ID(pHelper->pRepo), pHelper->tableInfo.tid,
            blkIdx);

  return 0;
}

static int tsdbSetAndOpenCommitFGroup(STSCommitHandle *pTSCh, STsdbRepo *pOldGroup, STsdbRepo *pNewGroup) {
  STsdbRepo *pRepo = pTSCh->pReadH->pRepo;

  if (tsdbSetAndOpenReadFGroup(pTSCh->pReadH, pOldGroup) < 0) {
    tsdbError("vgId:%d failed to set and open commit file group since %s", REPO_ID(pRepo), tstrerror(terrno));
    return -1;
  }

  tsdbResetFGroupFd(pNewGroup);

  for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
    SFile *pOldFile = TSDB_FILE_IN_FGROUP(pOldGroup, type);
    SFile *pNewFile = TSDB_FILE_IN_FGROUP(pNewGroup, type);

    pNewFile->fd = open(pNewFile->fname, O_CREAT | O_WRONLY, 0755);
    if (pNewFile->fd < 0) {
      tsdbError("vgId:%d failed to open file %s while commit since %s", REPO_ID(pRepo), pNewFile->fname,
                strerror(errno));
      terrno = TAOS_SYSTEM_ERROR(errno);
      tsdbCloseAndUnsetCommitFGroup(pTSCh, true /*hasError = true*/);
      return -1;
    }

    if (pOldFile->fname[0] == '\0' ||
        strncmp(pOldFile->fname, pNewFile->fname, TSDB_FILENAME_LEN) != 0) {  // new file is created
        if (tsdbUpdateFileHeader(pNewFile) < 0) {
          tsdbError("vgId:%d failed to update file %s header since %s", REPO_ID(pRepo), pNewFile->fname, strerror(errno));
          terrno = TAOS_SYSTEM_ERROR(errno);
          tsdbCloseAndUnsetCommitFGroup(pTSCh, true /*hasError = true*/);
          return -1;
        }
    }
  }

  pTSCh->pFGroup = pNewGroup;
  return 0;

_err:
}

static void tsdbCloseAndUnsetCommitFGroup(STSCommitHandle *pTSCh, bool hasError) {
  tsdbCloseAndUnsetReadFile(pTSCh->pReadH);

  for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
    SFile *pOldFile = TSDB_FILE_IN_FGROUP(pOldGroup, type);
    SFile *pNewFile = TSDB_FILE_IN_FGROUP(pNewGroup, type);

    if (pNewFile->fd >= 0) {
      if (!hasError) {
        (void)fsync(pNewFile->fd);
      }
      (void)close(pNewFile->fd);
      pNewFile->fd = -1;
    }
  }
}
