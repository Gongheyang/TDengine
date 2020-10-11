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
  SRWHelper    whelper;
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

    { // TODO: Log file change
      SFileGroup *pFGroup = tsdbSearchFGroup(pFileH, fid, TD_EQ);
      if (pFGroup == NULL) {

      } else {

      }
    }

    if (tsdbCommitToFile(pRepo, fid, &tsCommitH) < 0) {
      tsdbError("vgId:%d failed to commit to file %d since %s", REPO_ID(pRepo), fid, tstrerror(terrno));
      goto _err;
    }
  }

  tsdbDestroyTSCommitHandle(&tsCommitH);
  return 0;

_err:
  tsdbDestroyTSCommitHandle(&tsCommitH);
  return -1;
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

static int tsdbCommitToFile(STsdbRepo *pRepo, int fid, STSCommitHandle *pTSCh) {
  char *       dataDir = NULL;
  STsdbCfg *   pCfg = &pRepo->config;
  STsdbFileH * pFileH = pRepo->tsdbFileH;
  SFileGroup * pGroup = NULL;
  SMemTable *  pMem = pRepo->imem;
  bool         newLast = false;
  SCommitIter *iters = pTSCh->pIters;
  SRWHelper *  pHelper = &(pTSCh->whelper);
  SDataCols *  pDataCols = pTSCh->pDataCols;

  // Create and open files for commit
  dataDir = tsdbGetDataDirName(pRepo->rootDir);
  if (dataDir == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  if ((pGroup = tsdbCreateFGroupIfNeed(pRepo, dataDir, fid)) == NULL) {
    tsdbError("vgId:%d failed to create file group %d since %s", REPO_ID(pRepo), fid, tstrerror(terrno));
    goto _err;
  }

  // Open files for write/read
  if (tsdbSetAndOpenHelperFile(pHelper, pGroup) < 0) {
    tsdbError("vgId:%d failed to set helper file since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _err;
  }

  newLast = TSDB_NLAST_FILE_OPENED(pHelper);

  if (tsdbLoadCompIdx(pHelper, NULL) < 0) {
    tsdbError("vgId:%d failed to load SCompIdx part since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _err;
  }

  // Loop to commit data in each table
  for (int tid = 1; tid < pMem->maxTables; tid++) {
    SCommitIter *pIter = iters + tid;
    if (pIter->pTable == NULL) continue;

    taosRLockLatch(&(pIter->pTable->latch));

    if (tsdbSetHelperTable(pHelper, pIter->pTable, pRepo) < 0) goto _err;

    if (pIter->pIter != NULL) {
      if (tdInitDataCols(pDataCols, tsdbGetTableSchemaImpl(pIter->pTable, false, false, -1)) < 0) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        goto _err;
      }

      if (tsdbCommitTableData(pHelper, pIter, pDataCols, maxKey) < 0) {
        taosRUnLockLatch(&(pIter->pTable->latch));
        tsdbError("vgId:%d failed to write data of table %s tid %d uid %" PRIu64 " since %s", REPO_ID(pRepo),
                  TABLE_CHAR_NAME(pIter->pTable), TABLE_TID(pIter->pTable), TABLE_UID(pIter->pTable),
                  tstrerror(terrno));
        goto _err;
      }
    }

    taosRUnLockLatch(&(pIter->pTable->latch));

    // Move the last block to the new .l file if neccessary
    if (tsdbMoveLastBlockIfNeccessary(pHelper) < 0) {
      tsdbError("vgId:%d, failed to move last block, since %s", REPO_ID(pRepo), tstrerror(terrno));
      goto _err;
    }

    // Write the SCompBlock part
    if (tsdbWriteCompInfo(pHelper) < 0) {
      tsdbError("vgId:%d, failed to write compInfo part since %s", REPO_ID(pRepo), tstrerror(terrno));
      goto _err;
    }
  }

  if (tsdbWriteCompIdx(pHelper) < 0) {
    tsdbError("vgId:%d failed to write compIdx part to file %d since %s", REPO_ID(pRepo), fid, tstrerror(terrno));
    goto _err;
  }

  taosTFree(dataDir);
  tsdbCloseHelperFile(pHelper, 0, pGroup);

  pthread_rwlock_wrlock(&(pFileH->fhlock));

  (void)rename(helperNewHeadF(pHelper)->fname, helperHeadF(pHelper)->fname);
  pGroup->files[TSDB_FILE_TYPE_HEAD].info = helperNewHeadF(pHelper)->info;

  if (newLast) {
    (void)rename(helperNewLastF(pHelper)->fname, helperLastF(pHelper)->fname);
    pGroup->files[TSDB_FILE_TYPE_LAST].info = helperNewLastF(pHelper)->info;
  } else {
    pGroup->files[TSDB_FILE_TYPE_LAST].info = helperLastF(pHelper)->info;
  }

  pGroup->files[TSDB_FILE_TYPE_DATA].info = helperDataF(pHelper)->info;

  pthread_rwlock_unlock(&(pFileH->fhlock));

  return 0;

_err:
  taosTFree(dataDir);
  tsdbCloseHelperFile(pHelper, 1, NULL);
  return -1;
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

  if (tsdbInitWriteHelper(&(pTSCh->whelper), pRepo) < 0) {
    tsdbError("vgId:%d failed to init write helper since %s", REPO_ID(pRepo), tstrerror(terrno));
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
    tsdbDestroyHelper(&(pTSCh->whelper));
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
    // TODO
  } else {
    ASSERT(false);
  }

  return tsize;
}

static void *tsdbDecodeFileChange(void *buf, STsdbFileChange *pChange) {
  // TODO
  return buf;
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
  tdListPrependNode(pCommitH->pModLog, pNode);

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
      tdListPrependNode(pCommitH->pModLog, &pChange);
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