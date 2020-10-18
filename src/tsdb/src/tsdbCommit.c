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

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "tchecksum.h"
#include "tscompression.h"
#include "tsdbMain.h"

#define TSDB_DATA_FILE_CHANGE 0
#define TSDB_META_FILE_CHANGE 1
#define TSDB_COMMIT_OVER 2

#define TSDB_MAX_SUBBLOCKS 8
#define TSDB_DEFAULT_ROWS_TO_COMMIT(maxRows) ((maxRows) * 4 / 5)

typedef struct {
  int          maxIters;
  SCommitIter *pIters;
  SReadHandle *pReadH;
  SFileGroup * pFGroup;
  TSKEY        minKey;
  TSKEY        maxKey;
  SBlockIdx *  pBlockIdx;
  int          nBlockIdx;
  SBlockIdx    newBlockIdx;
  SBlockInfo * pBlockInfo;
  int          nBlocks;
  SBlock *     pSubBlock;
  int          nSubBlocks;
  SDataCols *  pDataCols;
  int          miter;
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

static int              tsdbStartCommit(SCommitHandle *pCommitH);
static void             tsdbEndCommit(SCommitHandle *pCommitH, bool hasError);
static int              tsdbCommitTimeSeriesData(SCommitHandle *pCommitH);
static int              tsdbCommitMetaData(SCommitHandle *pCommitH);
static SCommitIter *    tsdbCreateCommitIters(STsdbRepo *pRepo);
static void             tsdbDestroyCommitIters(SCommitIter *iters, int maxTables);
static int              tsdbCommitToFileGroup(STSCommitHandle *pTSCh, SFileGroup *pOldGroup, SFileGroup *pNewGroup);
static int              tsdbHasDataToCommit(STSCommitHandle *pTSCh, TSKEY minKey, TSKEY maxKey);
static STSCommitHandle *tsdbNewTSCommitHandle(STsdbRepo *pRepo);
static void             tsdbFreeTSCommitHandle(STSCommitHandle *pTSCh);
static int              tsdbLogFileChange(SCommitHandle *pCommitH, STsdbFileChange *pChange);
static int              tsdbEncodeFileChange(void **buf, STsdbFileChange *pChange);
static void *           tsdbDecodeFileChange(void *buf, STsdbFileChange *pChange);
static int              tsdbLogTSFileChange(SCommitHandle *pCommitH, int fid);
static int              tsdbLogMetaFileChange(SCommitHandle *pCommitH);
static int              tsdbLogRetentionChange(SCommitHandle *pCommitH, int mfid);
static int              tsdbApplyFileChange(STsdbFileChange *pChange, bool isCommitEnd);
static void             tsdbSeekTSCommitHandle(STSCommitHandle *pTSCh, TSKEY key);
static int              tsdbEncodeSFileGroup(void **buf, SFileGroup *pFGroup);
static void *           tsdbDecodeSFileGroup(void *buf, SFileGroup *pFGroup);
static void tsdbGetNextCommitFileGroup(STsdbRepo *pRepo, SFileGroup *pOldGroup, SFileGroup *pNewGroup);
static int  tsdbCommitTableData(STSCommitHandle *pTSCh, int tid);
static int  tsdbWriteBlockToRightFile(STSCommitHandle *pTSCh, SDataCols *pDataCols, SBlock *pBlock);
static int  tsdbSetAndOpenCommitFGroup(STSCommitHandle *pTSCh, SFileGroup *pOldGroup, SFileGroup *pNewGroup);
static void tsdbCloseAndUnsetCommitFGroup(STSCommitHandle *pTSCh, bool hasError);
static int  tsdbWriteBlockInfo(STSCommitHandle *pTSCh);
static int  tsdbWriteBlockIdx(STSCommitHandle *pTSCh);
static int  tsdbSetCommitTable(STSCommitHandle *pTSCh, STable *pTable);
static int  tsdbCommitTableDataImpl(STSCommitHandle *pTSCh, int tid);
static int  tsdbCopyBlocks(STSCommitHandle *pTSCh, int sidx, int eidx);
static int  tsdbAppendCommit(STSCommitHandle *pTSCh);
static int  tsdbMergeCommit(STSCommitHandle *pTSCh, SBlock *pBlock);
static int  tsdbWriteBlockToFile(STSCommitHandle *pTSCh, int ftype, SDataCols *pDataCols, SBlock *pBlock,
                                 bool isSuperBlock);
static int  tsdbEncodeBlockIdxArray(STSCommitHandle *pTSCh);
static int  tsdbUpdateFileGroupInfo(SFileGroup *pFileGroup);
static int  tsdbAppendBlockIdx(STSCommitHandle *pTSCh);
static int  tsdbCopyBlock(STSCommitHandle *pTSCh, SBlock *pBlock);
static int  compareKeyBlock(const void *arg1, const void *arg2);
static int  tsdbAddSuperBlock(STSCommitHandle *pTSCh, SBlock *pBlock);
static int  tsdbAddSubBlocks(STSCommitHandle *pTSCh, SBlock *pBlocks, int nBlocks);
static int  tsdbMergeLastBlock(STSCommitHandle *pTSCh, SBlock *pBlock);
static int  tsdbMergeDataBlock(STSCommitHandle *pTSCh, SBlock *pBlock);
static void tsdbLoadMergeFromCache(STSCommitHandle *pTSCh, TSKEY maxKey);
static int  tsdbInsertSubBlock(STSCommitHandle *pTSCh, SBlock *pBlock);

void *tsdbCommitData(void *arg) {
  STsdbRepo *pRepo = (STsdbRepo *)arg;
  ASSERT(pRepo->commit == 1 && pRepo->imem != NULL);

  SCommitHandle  commitHandle = {0};
  SCommitHandle *pCommitH = &commitHandle;

  pCommitH->pRepo = pRepo;

  if (tsdbStartCommit(pCommitH) < 0) return NULL;

  if (tsdbCommitTimeSeriesData(pCommitH) < 0) {
    tsdbEndCommit(pCommitH, true);
    return NULL;
  }

  if (tsdbCommitMetaData(pCommitH) < 0) {
    tsdbEndCommit(pCommitH, true);
    return NULL;
  }

  tsdbEndCommit(pCommitH, false);
  return NULL;
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
  STsdbRepo *pRepo = pCommitH->pRepo;
  SMemTable *pMem = pRepo->imem;
  STsdbCfg * pCfg = &(pRepo->config);
  TSKEY      minKey = 0;
  TSKEY      maxKey = 0;

  int mfid = tsdbGetCurrMinFid(pCfg->precision, pCfg->keep, pCfg->daysPerFile);
  if (tsdbLogRetentionChange(pCommitH, mfid) < 0) return -1;

  if (pMem->numOfRows <= 0) return 0;

  // Initialize resources
  STSCommitHandle *pTSCh = tsdbNewTSCommitHandle(pRepo);
  if (pTSCh == NULL) return -1;

  // Seek skip over data beyond retention
  tsdbGetFidKeyRange(pCfg->daysPerFile, pCfg->precision, mfid, &minKey, &maxKey);
  tsdbSeekTSCommitHandle(pTSCh, minKey);

  // Commit Time-Series data file by file
  int sfid = (int)(TSDB_KEY_FILEID(pMem->keyFirst, pCfg->daysPerFile, pCfg->precision));
  int efid = (int)(TSDB_KEY_FILEID(pMem->keyLast, pCfg->daysPerFile, pCfg->precision));

  for (int fid = sfid; fid <= efid; fid++) {
    // Skip files beyond retention
    if (fid < mfid) continue;

    if (!tsdbHasDataToCommit(pTSCh, minKey, maxKey)) continue;

    // TODO : set pOldGroup and pNewGroup
    SFileGroup *pOldGroup = NULL;
    SFileGroup *pNewGroup = NULL;
    if (tsdbLogTSFileChange(pCommitH, fid) < 0) {
      tsdbFreeTSCommitHandle(pTSCh);
      return -1;
    }

    if (tsdbCommitToFileGroup(pTSCh, pOldGroup, pNewGroup) < 0) {
      tsdbFreeTSCommitHandle(pTSCh);
      return -1;
    }
  }

  tsdbFreeTSCommitHandle(pTSCh);
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

  if (tdKVStoreEndCommit(pStore, false /*hasError = false*/) < 0) {
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

static int tsdbCommitToFileGroup(STSCommitHandle *pTSCh, SFileGroup *pOldGroup, SFileGroup *pNewGroup) {
  SCommitIter *pIters = pTSCh->pIters;

  if (tsdbSetAndOpenCommitFGroup(pTSCh, pOldGroup, pNewGroup) < 0) return -1;

  if (tsdbLoadBlockIdx(pTSCh->pReadH) < 0) {
    tsdbCloseAndUnsetCommitFGroup(pTSCh, true /* hasError = true */);
    return -1;
  }

  for (int tid = 1; tid < pTSCh->maxIters; tid++) {
    SCommitIter *pIter = pIters + tid;
    if (pIter->pTable == NULL) continue;

    if (tsdbCommitTableData(pTSCh, tid) < 0) {
      tsdbCloseAndUnsetCommitFGroup(pTSCh, true /* hasError = true */);
      return -1;
    }
  }

  if (tsdbWriteBlockIdx(pTSCh) < 0) {
    tsdbCloseAndUnsetCommitFGroup(pTSCh, true /* hasError = true */);
    return -1;
  }

  if (tsdbUpdateFileGroupInfo(pNewGroup) < 0) {
    tsdbCloseAndUnsetCommitFGroup(pTSCh, true /* hasError = true */);
    return -1;
  }

  tsdbCloseAndUnsetCommitFGroup(pTSCh, false /* hasError = false */);

  return 0;
}

static int tsdbHasDataToCommit(STSCommitHandle *pTSCh, TSKEY minKey, TSKEY maxKey) {
  int          nIters = pTSCh->maxIters;
  SCommitIter *iters = pTSCh->pIters;

  for (int i = 0; i < nIters; i++) {
    SCommitIter *pIter = iters + i;
    if (pIter->pTable == NULL) continue;

    TSKEY nextKey = tsdbNextIterKey(pIter->pIter);
    if (nextKey > 0 && (nextKey >= minKey && nextKey <= maxKey)) return 1;
  }
  return 0;
}

static STSCommitHandle *tsdbNewTSCommitHandle(STsdbRepo *pRepo) {
  STsdbCfg * pCfg = &(pRepo->config);
  STsdbMeta *pMeta = pRepo->tsdbMeta;
  SMemTable *pMem = pRepo->imem;

  STSCommitHandle *pTSCh = (STSCommitHandle *)calloc(1, sizeof(*pTSCh));
  if (pTSCh == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return NULL;
  }

  pTSCh->maxIters = pMem->maxTables;
  pTSCh->pIters = tsdbCreateCommitIters(pRepo);
  if (pTSCh->pIters == NULL) {
    tsdbError("vgId:%d failed to create commit iterator since %s", REPO_ID(pRepo), tstrerror(terrno));
    tsdbFreeTSCommitHandle(pTSCh);
    return NULL;
  }

  pTSCh->pReadH = tsdbNewReadHandle(pRepo);
  if (pTSCh->pReadH == NULL) {
    tsdbError("vgId:%d failed to create new read handle since %s", REPO_ID(pRepo), tstrerror(terrno));
    tsdbFreeTSCommitHandle(pTSCh);
    return NULL;
  }

  pTSCh->pDataCols = tdNewDataCols(pMeta->maxRowBytes, pMeta->maxCols, pCfg->maxRowsPerFileBlock);
  if (pTSCh->pDataCols == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbError("vgId:%d failed to init data cols with maxRowBytes %d maxCols %d maxRowsPerFileBlock %d since %s",
              REPO_ID(pRepo), pMeta->maxCols, pMeta->maxRowBytes, pCfg->maxRowsPerFileBlock, tstrerror(terrno));
    tsdbFreeTSCommitHandle(pTSCh);
    return NULL;
  }

  return pTSCh;
}

static void tsdbFreeTSCommitHandle(STSCommitHandle *pTSCh) {
  if (pTSCh) {
    tdFreeDataCols(pTSCh->pDataCols);
    tsdbFreeReadHandle(pTSCh->pReadH);
    tsdbDestroyCommitIters(pTSCh->pIters, pTSCh->maxIters);
    taosTZfree(pTSCh->pSubBlock);
    taosTZfree(pTSCh->pBlockInfo);
    taosTZfree(pTSCh->pBlockIdx);
    free(pTSCh);
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
    tsize += tdEncodeStoreInfo(buf, &(pMetaChange->info));
  } else if (pChange->type == TSDB_DATA_FILE_CHANGE) {
    SDataFileChange *pDataChange = (SDataFileChange *)pChange->change;

    tsize += tsdbEncodeSFileGroup(buf, &(pDataChange->ofgroup));
    tsize += tsdbEncodeSFileGroup(buf, &(pDataChange->nfgroup));
  } else {
    ASSERT(false);
  }

  return tsize;
}

static UNUSED_FUNC void *tsdbDecodeFileChange(void *buf, STsdbFileChange *pChange) {
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

  tsdbGetNextCommitFileGroup(pRepo, &(pDataFileChange->ofgroup), &(pDataFileChange->nfgroup));

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

  STsdbFileChange *pChange = (STsdbFileChange *)pNode->data;
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
    SFileGroup *pFGroup = pFileH->pFGroup + i;
    if (pFGroup->fileId < mfid) {
      SListNode *pNode = (SListNode *)calloc(1, sizeof(SListNode) + sizeof(STsdbFileChange) + sizeof(SDataFileChange));
      if (pNode == NULL) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        return -1;
      }

      STsdbFileChange *pChange = (STsdbFileChange *)pNode->data;
      pChange->type = TSDB_DATA_FILE_CHANGE;

      SDataFileChange *pDataFileChange = (SDataFileChange *)pChange->change;
      pDataFileChange->ofgroup = *pFGroup;

      if (tsdbLogFileChange(pCommitH, pChange) < 0) {
        free(pNode);
        return -1;
      }
      tdListAppendNode(pCommitH->pModLog, pNode);
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
      if (strncmp(pMetaChange->oname, pMetaChange->nname, TSDB_FILENAME_LEN) != 0) {
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

static UNUSED_FUNC void *tsdbDecodeSFileGroup(void *buf, SFileGroup *pFGroup) {
  buf = taosDecodeVariantI32(buf, &(pFGroup->fileId));

  for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
    SFile *pFile = &(pFGroup->files[type]);
    char *fname = pFile->fname;

    buf = taosDecodeString(buf, &fname);
    buf = tsdbDecodeSFileInfo(buf, &(pFile->info));
  }

  return buf;
}

static void tsdbGetNextCommitFileGroup(STsdbRepo *pRepo, SFileGroup *pOldGroup, SFileGroup *pNewGroup) {
  pNewGroup->fileId = pOldGroup->fileId;

  for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
    SFile *pOldFile = &(pOldGroup->files[type]);
    SFile *pNewFile = &(pNewGroup->files[type]);

    size_t len =strlen(pOldFile->fname);
    if (len == 0 || pOldFile->fname[len - 1] == '1') {
      tsdbGetFileName(pRepo->rootDir, type, REPO_ID(pRepo), pOldGroup->fileId, 0, pNewFile->fname);
    } else {
      tsdbGetFileName(pRepo->rootDir, type, REPO_ID(pRepo), pOldGroup->fileId, 1, pNewFile->fname);
    }
  }
}

static int tsdbCommitTableData(STSCommitHandle *pTSCh, int tid) {
  SCommitIter *pIter = pTSCh->pIters + tid;
  SReadHandle *pReadH = pTSCh->pReadH;
  TSKEY        keyNext = tsdbNextIterKey(pIter->pIter);

  taosRLockLatch(&(pIter->pTable->latch));

  if (tsdbSetCommitTable(pTSCh, pIter->pTable) < 0) {
    taosRUnLockLatch(&(pIter->pTable->latch));
    return -1;
  }

  if (pReadH->pCurBlockIdx == NULL && TSDB_KEY_BEYOND_RANGE(keyNext, pTSCh->maxKey)) {
    // no data in memory and no old data in file, just skip the table
    taosRUnLockLatch(&(pIter->pTable->latch));
    return 0;
  }

  if (tsdbLoadBlockInfo(pReadH, NULL) < 0) {
    taosRUnLockLatch(&(pIter->pTable->latch));
    return -1;
  }

  if (tsdbCommitTableDataImpl(pTSCh, tid) < 0) {
    taosRUnLockLatch(&(pIter->pTable->latch));
    return -1;
  }

  if (tsdbWriteBlockInfo(pTSCh) < 0) {
    taosRUnLockLatch(&(pIter->pTable->latch));
    return -1;
  }

  // Append a new blockIdx
  if (tsdbAppendBlockIdx(pTSCh) < 0) {
    taosRUnLockLatch(&(pIter->pTable->latch));
    return -1;
  }

  taosRUnLockLatch(&(pIter->pTable->latch));
  return 0;
}

static int tsdbWriteBlockToRightFile(STSCommitHandle *pTSCh, SDataCols *pDataCols, SBlock *pBlock) {
  STsdbCfg *pCfg = &(pTSCh->pReadH->pRepo->config);
  int       ftype = 0;

  if (pDataCols->numOfRows >= pCfg->minRowsPerFileBlock) {
    ftype = TSDB_FILE_TYPE_DATA;
  } else {
    ftype = TSDB_FILE_TYPE_LAST;
  }

  if (tsdbWriteBlockToFile(pTSCh, ftype, pDataCols, pBlock, true) < 0) return -1;

  return 0;
}

static int tsdbSetAndOpenCommitFGroup(STSCommitHandle *pTSCh, SFileGroup *pOldGroup, SFileGroup *pNewGroup) {
  ASSERT(pOldGroup->fileId == pNewGroup->fileId);

  STsdbRepo *pRepo = pTSCh->pReadH->pRepo;
  STsdbCfg * pCfg = &(pRepo->config);

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
  pTSCh->nBlockIdx = 0;
  tsdbGetFidKeyRange(pCfg->daysPerFile, pCfg->precision, pOldGroup->fileId, &(pTSCh->minKey), &(pTSCh->maxKey));

  return 0;
}

static void tsdbCloseAndUnsetCommitFGroup(STSCommitHandle *pTSCh, bool hasError) {
  tsdbCloseAndUnsetReadFile(pTSCh->pReadH);

  for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
    // SFile *pOldFile = TSDB_FILE_IN_FGROUP(pOldGroup, type);
    SFile *pNewFile = TSDB_FILE_IN_FGROUP(pTSCh->pFGroup, type);

    if (pNewFile->fd >= 0) {
      if (!hasError) {
        (void)fsync(pNewFile->fd);
      }
      (void)close(pNewFile->fd);
      pNewFile->fd = -1;
    }
  }
}

static int tsdbWriteBlockInfo(STSCommitHandle *pTSCh) {
  ASSERT(pTSCh->nBlocks > 0);
  SReadHandle *pReadH = pTSCh->pReadH;
  STsdbRepo *  pRepo = pReadH->pRepo;
  SBlockInfo * pBlockInfo = pTSCh->pBlockInfo;
  SFile *      pFile = TSDB_FILE_IN_FGROUP(pTSCh->pFGroup, TSDB_FILE_TYPE_HEAD);
  int          tlen = TSDB_BLOCK_INFO_LEN(pTSCh->nBlocks+pTSCh->nSubBlocks);

  pBlockInfo->delimiter = TSDB_FILE_DELIMITER;
  pBlockInfo->uid = TABLE_UID(pReadH->pTable);
  pBlockInfo->tid = TABLE_TID(pReadH->pTable);

  if (pTSCh->nSubBlocks > 0) {
    if (tsdbAllocBuf((void **)(&(pTSCh->pBlockInfo)), tlen) < 0) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      return -1;
    }

    memcpy(POINTER_SHIFT(pTSCh->pBlockInfo, sizeof(SBlockInfo) + sizeof(SBlock) * pTSCh->nBlocks),
           (void *)pTSCh->pSubBlock, sizeof(SBlock) * pTSCh->nSubBlocks);
    
    int64_t oShift = sizeof(SBlockInfo) + sizeof(SBlock) * pTSCh->nBlocks;
    for (int i = 0; i < pTSCh->nBlocks; i++) {
      SBlock *pBlock = pTSCh->pBlockInfo->blocks + i;
      ASSERT(pBlock->numOfSubBlocks >= 1 && pBlock->numOfSubBlocks <= TSDB_MAX_SUBBLOCKS);

      if (pBlock->numOfSubBlocks > 1) pBlock->offset += oShift;
    }
  }

  taosCalcChecksumAppend(0, (uint8_t *)(pTSCh->pBlockInfo), tlen);

  if (taosTWrite(pFile->fd, (void *)pTSCh->pBlockInfo, tlen) < tlen) {
    tsdbError("vgId:%d failed to write %d bytes to file %s since %s", REPO_ID(pRepo), tlen, pFile->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  pTSCh->newBlockIdx.tid = TABLE_TID(pReadH->pTable);
  pTSCh->newBlockIdx.uid = TABLE_UID(pReadH->pTable);
  pTSCh->newBlockIdx.offset = (uint32_t)(pFile->info.size);
  pTSCh->newBlockIdx.numOfBlocks = pTSCh->nBlocks;
  pTSCh->newBlockIdx.len = tlen;
  pTSCh->newBlockIdx.hasLast = pTSCh->pBlockInfo->blocks[pTSCh->nBlocks - 1].last;
  pTSCh->newBlockIdx.maxKey = pTSCh->pBlockInfo->blocks[pTSCh->nBlocks - 1].keyLast;

  pFile->info.size += tlen;
  pFile->info.magic = taosCalcChecksum(
      pFile->info.magic, (uint8_t *)POINTER_SHIFT(pTSCh->pBlockInfo, tlen - sizeof(TSCKSUM)), sizeof(TSCKSUM));

  return 0;
}

static int tsdbWriteBlockIdx(STSCommitHandle *pTSCh) {
  ASSERT(pTSCh->nBlockIdx > 0);

  SReadHandle *pReadH = pTSCh->pReadH;
  STsdbRepo *  pRepo = pReadH->pRepo;
  SFile *      pFile = TSDB_FILE_IN_FGROUP(pTSCh->pFGroup, TSDB_FILE_TYPE_HEAD);

  int len = tsdbEncodeBlockIdxArray(pTSCh);
  if (len < 0) return -1;

  // label checksum
  len += sizeof(TSCKSUM);
  if (tsdbAllocBuf((void **)(&(pReadH->pBuf)), len) < 0) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  taosCalcChecksumAppend(0, (uint8_t *)(pReadH->pBuf), len);

  off_t offset = lseek(pFile->fd, 0, SEEK_END);
  if (offset < 0) {
    tsdbError("vgId:%d failed to lseek to end of file %s since %s", REPO_ID(pRepo), pFile->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (taosTWrite(pFile->fd, pReadH->pBuf, len) < len) {
    tsdbError("vgId:%d failed to write %d bytes to file %s since %s", REPO_ID(pRepo), len, pFile->fname,
              strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  // Update pFile->info
  pFile->info.size += len;
  pFile->info.offset = (uint32_t)offset;
  pFile->info.len = len;
  pFile->info.magic = taosCalcChecksum(pFile->info.magic, (uint8_t *)POINTER_SHIFT(pReadH->pBuf, len - sizeof(TSCKSUM)),
                                       sizeof(TSCKSUM));

  ASSERT(pFile->info.size == pFile->info.offset + pFile->info.len);

  return 0;
}

static int tsdbSetCommitTable(STSCommitHandle *pTSCh, STable *pTable) {
  if (tsdbSetReadTable(pTSCh->pReadH, pTable) < 0) return -1;
  if (tdInitDataCols(pTSCh->pDataCols, tsdbGetTableSchemaImpl(pTable, false, false, -1)) < 0) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

static int tsdbCommitTableDataImpl(STSCommitHandle *pTSCh, int tid) {
  SCommitIter *pIter = pTSCh->pIters + tid;
  SReadHandle *pReadH = pTSCh->pReadH;
  SBlockIdx *  pOldIdx = pReadH->pCurBlockIdx;
  TSKEY        keyNext = tsdbNextIterKey(pIter->pIter);

  ASSERT((pOldIdx == NULL && (!TSDB_KEY_BEYOND_RANGE(keyNext, pTSCh->maxKey))) || pOldIdx->numOfBlocks > 0);

  // Initialize
  memset((void *)(&(pTSCh->newBlockIdx)), 0, sizeof(pTSCh->newBlockIdx));
  pTSCh->nBlocks = 0;
  pTSCh->nSubBlocks = 0;

  int sidx = 0;
  int eidx = (pOldIdx == NULL) ? 0 : pOldIdx->numOfBlocks;

  while (true) {
    if (TSDB_KEY_BEYOND_RANGE(keyNext, pTSCh->maxKey)) break;
    ASSERT(pTSCh->nBlocks == 0 || keyNext > pTSCh->pBlockInfo->blocks[pTSCh->nBlocks-1].keyLast);

    void *ptr = NULL;
    if (eidx > sidx) {
      ptr = taosbsearch((void *)keyNext, (void *)(pReadH->pBlockInfo->blocks + sidx), eidx - sidx, sizeof(SBlock),
                        compareKeyBlock, TD_GE);
    }

    if (ptr == NULL && sidx < eidx && pOldIdx->hasLast) {
      ptr = pReadH->pBlockInfo->blocks + eidx - 1;
    }

    int bidx = 0;
    if (ptr == NULL) {
      bidx = eidx;
    } else {
      bidx = POINTER_DISTANCE(ptr, (void *)(pReadH->pBlockInfo->blocks)) / sizeof(SBlock);
    }

    if (tsdbCopyBlocks(pTSCh, sidx, bidx) < 0) return -1;
    sidx = bidx;

    if (ptr == NULL) {
      if (tsdbAppendCommit(pTSCh) < 0) return -1;
    } else {
      if (tsdbMergeCommit(pTSCh, (SBlock *)ptr) < 0) return -1;
      sidx++;
    }

    // Update keyNext
    keyNext = tsdbNextIterKey(pIter->pIter);
  }

  // Move remaining blocks
  if (tsdbCopyBlocks(pTSCh, sidx, eidx) < 0) return -1;

  return 0;
}

static int tsdbCopyBlocks(STSCommitHandle *pTSCh, int sidx, int eidx) {
  ASSERT(sidx <= eidx);

  for (int idx = sidx; idx < eidx; idx++) {
    if (tsdbCopyBlock(pTSCh, pTSCh->pReadH->pBlockInfo->blocks + idx) < 0) return -1;
  }

  return 0;
}

static int tsdbAppendCommit(STSCommitHandle *pTSCh) {
  SDataCols *  pDataCols = pTSCh->pDataCols;
  SReadHandle *pReadH = pTSCh->pReadH;
  STable *     pTable = pReadH->pTable;
  SBlock       block = {0};
  SBlock *     pBlock = &block;
  STsdbCfg *   pCfg = &(pReadH->pRepo->config);
  SCommitIter *pIter = pTSCh->pIters + TABLE_TID(pTable);
  int          dbrows = TSDB_DEFAULT_ROWS_TO_COMMIT(pCfg->maxRowsPerFileBlock);  // default block rows

  tdResetDataCols(pDataCols);
  int rows = tsdbLoadDataFromCache(pTable, pIter->pIter, pTSCh->maxKey, dbrows, pDataCols, NULL, 0);
  ASSERT(rows > 0 && pDataCols->numOfRows == rows);

  if (tsdbWriteBlockToRightFile(pTSCh, pDataCols, pBlock) < 0) return -1;
  if (tsdbAddSuperBlock(pTSCh, pBlock) < 0) return -1;

  return -1;
}

static int tsdbMergeCommit(STSCommitHandle *pTSCh, SBlock *pBlock) {
  SReadHandle *pReadH = pTSCh->pReadH;
  if (pBlock->last) {
    ASSERT(POINTER_DISTANCE(pBlock, pReadH->pBlockInfo->blocks) / sizeof(SBlock) == (pReadH->nBlockIdx - 1));
    ASSERT(pReadH->pCurBlockIdx->hasLast);
    if (tsdbMergeLastBlock(pTSCh, pBlock) < 0) return -1;
  } else {
    if (tsdbMergeDataBlock(pTSCh, pBlock) < 0) return -1;
  }

  return 0;
}

static int tsdbWriteBlockToFile(STSCommitHandle *pTSCh, int ftype, SDataCols *pDataCols, SBlock *pBlock, bool isSuperBlock) {
  SFile *      pFile = TSDB_FILE_IN_FGROUP(pTSCh->pFGroup, ftype);
  SReadHandle *pReadH = pTSCh->pReadH;
  STsdbRepo *  pRepo = pReadH->pRepo;
  STsdbCfg *   pCfg = &(pRepo->config);
  int64_t      offset = pFile->info.size;
  int          nColsNotAllNull = 0;
  int          csize = TSDB_BLOCK_DATA_LEN(nColsNotAllNull);  // column size
  int32_t      keyLen = 0;
  bool         isLast = (ftype == TSDB_FILE_TYPE_LAST);

  ASSERT(offset == lseek(pFile->fd, 0, SEEK_END));
  ASSERT(pDataCols->numOfRows > 0 && pDataCols->numOfRows <= pCfg->maxRowsPerFileBlock);
  ASSERT(isLast ? pDataCols->numOfRows < pCfg->minRowsPerFileBlock : true);

  if (tsdbAllocBuf((void **)(&(pReadH->pBlockData)), csize) < 0) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  int32_t coffset = 0; // column data offset
  for (int ncol = 0; ncol < pDataCols->numOfCols; ncol++) {  // ncol from 1, we skip the timestamp column
    SDataCol * pDataCol = pDataCols->cols + ncol;
    SBlockCol *pBlockCol = NULL;

    if (ncol != 0) {
      if (isNEleNull(pDataCol, pDataCols->numOfRows)) {  // all data to commit are NULL, just ignore it
        continue;
      }

      nColsNotAllNull++;
      csize = TSDB_BLOCK_DATA_LEN(nColsNotAllNull);
      if (tsdbAllocBuf((void **)(&(pReadH->pBlockData)), csize) < 0) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        return -1;
      }

      pBlockCol = pReadH->pBlockData->cols + nColsNotAllNull - 1;
      memset(pBlockCol, 0, sizeof(*pBlockCol));

      pBlockCol->colId = pDataCol->colId;
      pBlockCol->type = pDataCol->type;
      if (tDataTypeDesc[pDataCol->type].getStatisFunc) {
        (*tDataTypeDesc[pDataCol->type].getStatisFunc)((TSKEY *)(pDataCols->cols[0].pData), pDataCol->pData,
                                                       pDataCols->numOfRows, &(pBlockCol->min), &(pBlockCol->max),
                                                       &(pBlockCol->sum), &(pBlockCol->minIndex),
                                                       &(pBlockCol->maxIndex), &(pBlockCol->numOfNull));
      }
    }

    // compress data if needed
    int32_t olen = dataColGetNEleLen(pDataCol, pDataCols->numOfRows);
    int32_t blen = olen + COMP_OVERFLOW_BYTES; // allocated buffer length
    int32_t clen = 0;

    if (tsdbAllocBuf((void **)(&(pReadH->pBuf)), coffset + blen + sizeof(TSCKSUM)) < 0) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      return -1;
    }

    void *pData = POINTER_SHIFT(pReadH->pBuf, coffset);

    if (pCfg->compression) {
      if (pCfg->compression == TWO_STAGE_COMP) {
        if (tsdbAllocBuf((void **)(&(pReadH->pCBuf)), blen) < 0) {
          terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
          return -1;
        }
      }

      clen = (*(tDataTypeDesc[pDataCol->type].compFunc))((char *)pDataCol->pData, olen, pDataCols->numOfRows, pData,
                                                         blen, pCfg->compression, pReadH->pCBuf, blen);
    } else {
      clen = olen;
      memcpy(pData, pDataCol->pData, olen);
    }

    ASSERT(clen > 0 && clen <= blen);

    clen += sizeof(TSCKSUM);
    taosCalcChecksumAppend(0, (uint8_t *)pData, clen);
    pFile->info.magic =
        taosCalcChecksum(pFile->info.magic, (uint8_t *)POINTER_SHIFT(pData, clen - sizeof(TSCKSUM)), sizeof(TSCKSUM));

    if (ncol != 0) {
      pReadH->pBlockData->cols[ncol].offset = coffset;
      pReadH->pBlockData->cols[ncol].len = clen;
    } else {
      keyLen = clen;
    }

    coffset += clen;
  }
  ASSERT(nColsNotAllNull >= 0 && nColsNotAllNull <= pDataCols->numOfCols);

  pReadH->pBlockData->delimiter = TSDB_FILE_DELIMITER;
  pReadH->pBlockData->uid = TABLE_UID(pReadH->pTable);
  pReadH->pBlockData->numOfCols = nColsNotAllNull;

  taosCalcChecksumAppend(0, (uint8_t *)pReadH->pBlockData, csize);
  pFile->info.magic = taosCalcChecksum(
      pFile->info.magic, (uint8_t *)POINTER_SHIFT(pReadH->pBlockData, csize - sizeof(TSCKSUM)), sizeof(TSCKSUM));

  if (taosTWrite(pFile->fd, (void *)pReadH->pBlockData, csize) < csize) {
    tsdbError("vgId:%d failed to write %d bytes to file %s since %s", REPO_ID(pRepo), csize, pFile->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (taosTWrite(pFile->fd, pReadH->pBuf, coffset) < coffset) {
    tsdbError("vgId:%d failed to write %d bytes to file %s since %s", REPO_ID(pRepo), coffset, pFile->fname,
              strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  // Update pBlock membership vairables
  pBlock->last = isLast;
  pBlock->offset = offset;
  pBlock->algorithm = pCfg->compression;
  pBlock->numOfRows = pDataCols->numOfRows;
  pBlock->len = coffset+csize;
  pBlock->keyLen = keyLen;
  pBlock->numOfSubBlocks = isSuperBlock ? 1 : 0;
  pBlock->numOfCols = nColsNotAllNull;
  pBlock->keyFirst = dataColsKeyFirst(pDataCols);
  pBlock->keyLast = dataColsKeyLast(pDataCols);

  pFile->info.size += pBlock->len;

  tsdbDebug("vgId:%d tid:%d a block of data is written to file %s, offset %" PRId64
            " numOfRows %d len %d numOfCols %" PRId16 " keyFirst %" PRId64 " keyLast %" PRId64,
            REPO_ID(pRepo), TABLE_TID(pReadH->pTable), pFile->fname, (int64_t)(pBlock->offset),
            (int)(pBlock->numOfRows), pBlock->len, pBlock->numOfCols, pBlock->keyFirst, pBlock->keyLast);

  return 0;
}

static int tsdbEncodeBlockIdxArray(STSCommitHandle *pTSCh) {
  SReadHandle *pReadH = pTSCh->pReadH;
  int          len = 0;

  for (int i = 0; i < pTSCh->nBlockIdx; i++) {
    int tlen = tsdbEncodeBlockIdx(NULL, pTSCh->pBlockIdx + i);

    if (tsdbAllocBuf((void **)(&(pReadH->pBuf)), tlen + len) < 0) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      return -1;
    }

    void *ptr = POINTER_SHIFT(pReadH->pBuf, len);
    tsdbEncodeBlockIdx(&ptr, pTSCh->pBlockIdx + i);

    len += tlen;
  }

  return len;
}

static int tsdbUpdateFileGroupInfo(SFileGroup *pFileGroup) {
  for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
    SFile *pFile = TSDB_FILE_IN_FGROUP(pFileGroup, type);
    if (tsdbUpdateFileHeader(pFile) < 0) return -1;
  }

  return 0;
}

static int tsdbAppendBlockIdx(STSCommitHandle *pTSCh) {
  if (tsdbAllocBuf((void **)(&(pTSCh->pBlockIdx)), sizeof(SBlockIdx) * (pTSCh->nBlockIdx + 1)) < 0) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  pTSCh->pBlockIdx[pTSCh->nBlockIdx++] = pTSCh->newBlockIdx;
  return 0;
}

static int tsdbCopyBlock(STSCommitHandle *pTSCh, SBlock *pBlock) {
  ASSERT(pBlock->numOfSubBlocks >= 1);

  SReadHandle *pReadH = pTSCh->pReadH;
  SFile *      pWFile = NULL;
  SFile *      pRFile = NULL;
  SBlock       newBlock = {0};
  int          ftype = 0;

  if (pBlock->last) {
    pWFile = TSDB_FILE_IN_FGROUP(pTSCh->pFGroup, TSDB_FILE_TYPE_LAST);
    pRFile = TSDB_FILE_IN_FGROUP(&(pReadH->fGroup), TSDB_FILE_TYPE_LAST);
    ftype = TSDB_FILE_TYPE_LAST;
  } else {
    pWFile = TSDB_FILE_IN_FGROUP(pTSCh->pFGroup, TSDB_FILE_TYPE_DATA);
    pRFile = TSDB_FILE_IN_FGROUP(&(pReadH->fGroup), TSDB_FILE_TYPE_DATA);
    ftype = TSDB_FILE_TYPE_DATA;
  }

  // TODO: use flag to omit this string compare. this may cause a lot of time
  if (strncmp(pWFile->fname, pRFile->fname, TSDB_FILENAME_LEN) == 0) {
    if (pBlock->numOfSubBlocks == 1) {
      if (tsdbAddSuperBlock(pTSCh, pBlock) < 0) return -1;
    } else {  // need to copy both super block and sub-blocks
      newBlock = *pBlock;
      newBlock.offset = sizeof(SBlock) * pTSCh->nSubBlocks;

      if (tsdbAddSuperBlock(pTSCh, &newBlock) < 0) return -1;
      if (tsdbAddSubBlocks(pTSCh, POINTER_SHIFT(pReadH->pBlockInfo, pBlock->offset), pBlock->numOfSubBlocks) < 0)
        return -1;
    }
  } else {
    if (tsdbLoadBlockData(pReadH, pBlock, NULL) < 0) return -1;
    if (tsdbWriteBlockToFile(pTSCh, ftype, pReadH->pDataCols[0], &newBlock, true) < 0) return -1;
    if (tsdbAddSuperBlock(pTSCh, &newBlock) < 0) return -1;
  }

  return 0;
}

static int compareKeyBlock(const void *arg1, const void *arg2) {
  TSKEY   key = *(TSKEY *)arg1;
  SBlock *pBlock = (SBlock *)arg2;

  if (key < pBlock->keyFirst) {
    return -1;
  } else if (key > pBlock->keyLast) {
    return 1;
  }

  return 0;
}

static int tsdbAddSuperBlock(STSCommitHandle *pTSCh, SBlock *pBlock) {
  ASSERT(pBlock->numOfSubBlocks > 0);

  int tsize = TSDB_BLOCK_INFO_LEN(pTSCh->nBlocks + 1);
  if (tsdbAllocBuf((void **)(&(pTSCh->pBlockInfo)), tsize) < 0) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  ASSERT(pTSCh->nBlocks == 0 || pBlock->keyFirst > pTSCh->pBlockInfo->blocks[pTSCh->nBlocks - 1].keyLast);
  pTSCh->pBlockInfo->blocks[pTSCh->nBlocks++] = *pBlock;

  return 0;
}

static int tsdbAddSubBlocks(STSCommitHandle *pTSCh, SBlock *pBlocks, int nBlocks) {
  int tBlocks = pTSCh->nSubBlocks + nBlocks;
  int tsize = sizeof(SBlock) * tBlocks;

  if (tsdbAllocBuf((void **)(&(pTSCh->pSubBlock)), tsize) < 0) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  memcpy((void *)(&pTSCh->pSubBlock[pTSCh->nSubBlocks]), (void *)pBlocks, sizeof(SBlock) * nBlocks);
  pTSCh->nSubBlocks += nBlocks;

  return 0;
}

static int tsdbMergeLastBlock(STSCommitHandle *pTSCh, SBlock *pBlock) {
  SReadHandle *pReadH = pTSCh->pReadH;
  STsdbRepo *  pRepo = pReadH->pRepo;
  STsdbCfg *   pCfg = &(pRepo->config);
  SDataCols *  pDataCols = pTSCh->pDataCols;
  STable *     pTable = pReadH->pTable;
  SCommitIter *pIter = pTSCh->pIters + TABLE_TID(pTable);
  int          dbrows = TSDB_DEFAULT_ROWS_TO_COMMIT(pCfg->maxRowsPerFileBlock);
  SBlock       newBlock = {0};

  TSKEY keyNext = tsdbNextIterKey(pIter->pIter);
  if (keyNext > pBlock->keyLast) { // append merge last block
    tdResetDataCols(pDataCols);
    int rows = tsdbLoadDataFromCache(pTable, pIter->pIter, pTSCh->maxKey, dbrows-pBlock->numOfRows, pDataCols, NULL, 0);
    ASSERT(rows > 0);

    if (pBlock->numOfRows + pDataCols->numOfRows < pCfg->minRowsPerFileBlock &&
        pBlock->numOfSubBlocks < TSDB_MAX_SUBBLOCKS && true /*TODO: check if same file*/) {
      if (tsdbWriteBlockToFile(pTSCh, TSDB_FILE_TYPE_LAST, pDataCols, &newBlock, false) < 0) return -1;
      if (tsdbCopyBlock(pTSCh, pBlock) < 0) return -1;
      if (tsdbInsertSubBlock(pTSCh, &newBlock) < 0) return -1; 
    } else {
      if (tsdbLoadBlockData(pReadH, pBlock, NULL) < 0) return -1;
      if (tdMergeDataCols(pReadH->pDataCols[0], pDataCols, pDataCols->numOfRows) < 0) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        return -1;
      }
      if (tsdbWriteBlockToRightFile(pTSCh, pReadH->pDataCols[0], &newBlock) < 0) return -1;
      if (tsdbAddSuperBlock(pTSCh, &newBlock) < 0) return -1;
    }
  } else { // sort merge last block
    SSkipListIterator titer = *(pIter->pIter);

    if (tsdbLoadKeyCol(pReadH, NULL, pBlock) < 0) return -1;
    int rows = tsdbLoadDataFromCache(pTable, &titer, pTSCh->maxKey, INT32_MAX, NULL,
                                     pReadH->pDataCols[0]->cols[0].pData, pBlock->numOfRows);
    if (rows == 0) { // all data duplicate
      *pIter->pIter = titer;
      if (tsdbCopyBlock(pTSCh, pBlock) < 0) return -1;
    } else if (pBlock->numOfRows + rows < pCfg->minRowsPerFileBlock && pBlock->numOfSubBlocks < TSDB_MAX_SUBBLOCKS && true/*TODO: if have same file*/){
      tdResetDataCols(pDataCols);
      tsdbLoadDataFromCache(pTable, pIter->pIter, pTSCh->maxKey, INT32_MAX, pDataCols,
                            pReadH->pDataCols[0]->cols[0].pData, pBlock->numOfRows);
      ASSERT(pDataCols->numOfRows == rows);
      if (tsdbWriteBlockToFile(pTSCh, TSDB_FILE_TYPE_LAST, pDataCols, &newBlock, false) < 0) return -1;
      if (tsdbCopyBlock(pTSCh, pBlock) < 0) return -1;
      if (tsdbInsertSubBlock(pTSCh, &newBlock) < 0) return -1;
    } else {
      if (tsdbLoadBlockData(pReadH, pBlock, NULL) < 0) return -1;
      while (true) {
        pTSCh->miter = 0;
        tsdbLoadMergeFromCache(pTSCh, pTSCh->maxKey);
        if (pDataCols->numOfRows == 0) break;
        if (tsdbWriteBlockToRightFile(pTSCh, pDataCols, &newBlock) < 0) return -1;
        if (tsdbAddSuperBlock(pTSCh, &newBlock) < 0) return -1;
      }
    }
  }

  return 0;
}

static int tsdbMergeDataBlock(STSCommitHandle *pTSCh, SBlock *pBlock) {
  SReadHandle *pReadH = pTSCh->pReadH;
  STsdbRepo *  pRepo = pReadH->pRepo;
  STsdbCfg *   pCfg = &(pRepo->config);
  SCommitIter *pIter = pTSCh->pIters + TABLE_TID(pReadH->pTable);
  SDataCols *  pDataCols = pTSCh->pDataCols;
  SBlock       newBlock = {0};
  int          dbrows = TSDB_DEFAULT_ROWS_TO_COMMIT(pCfg->maxRowsPerFileBlock);
  int          bidx = POINTER_DISTANCE(pBlock, pReadH->pBlockInfo->blocks) / sizeof(SBlock);
  int          rows = 0;
  TSKEY        keyLimit = (bidx == pReadH->pCurBlockIdx->numOfBlocks - 1) ? pTSCh->maxKey : (pBlock[1].keyFirst - 1);
  TSKEY        keyNext = tsdbNextIterKey(pIter->pIter);

  SSkipListIterator titer = *(pIter->pIter);

  ASSERT(bidx < pReadH->pCurBlockIdx->numOfBlocks && pBlock->numOfSubBlocks >= 1);

  // Commit data to pBlock->keyFirst - 1 included
  if (keyNext < pBlock->keyFirst) {
    while (true) {
      if (TSDB_KEY_BEYOND_RANGE(keyNext, pBlock->keyFirst - 1)) break;

      tdResetDataCols(pDataCols);
      rows = tsdbLoadDataFromCache(pIter->pTable, pIter->pIter, pBlock->keyFirst - 1, dbrows, pDataCols, NULL, 0);
      ASSERT(rows > 0);
      if (tsdbWriteBlockToFile(pTSCh, TSDB_FILE_TYPE_DATA, pDataCols, &newBlock, true) < 0)
        return -1;
      if (tsdbAddSuperBlock(pTSCh, &newBlock) < 0) return -1;

      keyNext = tsdbNextIterKey(pIter->pIter);
    }
  }

  if (keyNext > pBlock->keyLast) {
    if (tsdbCopyBlock(pTSCh, pBlock) < 0) return -1;
    return 0;
  }

  // Commit data to keyLimit included
  if (tsdbLoadKeyCol(pReadH, NULL, pBlock) < 0) return -1;
  rows = tsdbLoadDataFromCache(pIter->pTable, &titer, pBlock->keyLast, INT32_MAX, NULL,
                               pReadH->pDataCols[0]->cols[0].pData, pBlock->numOfRows);

  if (rows == 0) {
    *(pIter->pIter) = titer;
    if (tsdbCopyBlock(pTSCh, pBlock) < 0) return -1;
  } else if (pBlock->numOfSubBlocks < TSDB_MAX_SUBBLOCKS && rows + pBlock->numOfRows <= pCfg->maxRowsPerFileBlock &&
             true /*TODO: the same block*/) {
    int trow = tsdbLoadDataFromCache(pIter->pTable, &titer, keyLimit, INT_MAX, NULL, NULL, 0) + rows;
    if (trow + pBlock->numOfRows <= pCfg->maxRowsPerFileBlock) rows = trow;
    tdResetDataCols(pDataCols);
    tsdbLoadDataFromCache(pIter->pTable, pIter->pIter, keyLimit, rows, pDataCols, pReadH->pDataCols[0]->cols[0].pData,
                          pBlock->numOfRows);
    ASSERT(pDataCols->numOfRows == rows);
    if (tsdbWriteBlockToFile(pTSCh, TSDB_FILE_TYPE_DATA, pDataCols, &newBlock, false) < 0) return -1;
    if (tsdbCopyBlock(pTSCh, pBlock) < 0) return -1;
    if (tsdbInsertSubBlock(pTSCh, &newBlock) < 0) return -1;
  } else {
    if (tsdbLoadBlockData(pReadH, pBlock, NULL) < 0) return -1;
    while (true) {
      pTSCh->miter = 0;
      tsdbLoadMergeFromCache(pTSCh, keyLimit);
      if (pDataCols->numOfRows == 0) break;
      if (tsdbWriteBlockToFile(pTSCh, TSDB_FILE_TYPE_DATA, pDataCols, &newBlock, true) < 0) return -1;
      if (tsdbAddSuperBlock(pTSCh, &newBlock) < 0) return -1;
    }
  }

  return 0;
}

static void tsdbLoadMergeFromCache(STSCommitHandle *pTSCh, TSKEY maxKey) {
  SReadHandle *pReadH = pTSCh->pReadH;
  STsdbRepo *  pRepo = pReadH->pRepo;
  SDataCols *  pMCols = pReadH->pDataCols[0];
  SDataCols *  pDataCols = pTSCh->pDataCols;
  int          dbrows = TSDB_DEFAULT_ROWS_TO_COMMIT(pRepo->config.maxRowsPerFileBlock);
  SCommitIter *pIter = pTSCh->pIters + TABLE_TID(pReadH->pTable);
  TSKEY        key1 = 0;
  TSKEY        key2 = 0;
  SDataRow     row = NULL;
  TSKEY        keyNext = 0;
  STSchema *   pSchema = NULL;

  tdResetDataCols(pDataCols);

  if (pTSCh->miter >= pMCols->numOfRows) {
    key1 = INT64_MAX;
  } else {
    key1 = dataColsKeyAt(pMCols, pTSCh->miter);
  }

  keyNext = tsdbNextIterKey(pIter->pIter);
  if (TSDB_KEY_BEYOND_RANGE(keyNext, maxKey)) {
    key2 = INT64_MAX;
  } else {
    row = tsdbNextIterRow(pIter->pIter);
    key2 = keyNext;
  }

  while (true) {
    if ((key1 == INT64_MAX && key2 == INT64_MAX) || pDataCols->numOfRows >= dbrows) break;

    if (key1 <= key2) {
      for (int i = 0; i < pMCols->numOfCols; i++) {
        dataColAppendVal(pDataCols->cols + i, tdGetColDataOfRow(pMCols->cols + i, pTSCh->miter), pDataCols->numOfRows,
                         pDataCols->maxPoints);
      }
      pDataCols->numOfRows++;
      pTSCh->miter++;
      if (key1 == key2) {
        tSkipListIterNext(pIter->pIter);
        keyNext = tsdbNextIterKey(pIter->pIter);
        if (TSDB_KEY_BEYOND_RANGE(keyNext, maxKey)) {
          key2 = INT64_MAX;
        } else {
          row = tsdbNextIterRow(pIter->pIter);
          key2 = keyNext;
        }
      }
    } else {
      if (pSchema == NULL || schemaVersion(pSchema) != dataRowVersion(row)) {
        pSchema = tsdbGetTableSchemaImpl(pIter->pTable, false, false, dataRowVersion(row));
        ASSERT(pSchema != NULL);
      }

      tdAppendDataRowToDataCol(row, pSchema, pDataCols);
      tSkipListIterNext(pIter->pIter);

      // update row and key2
      keyNext = tsdbNextIterKey(pIter->pIter);
      if (TSDB_KEY_BEYOND_RANGE(keyNext, maxKey)) {
        key2 = INT64_MAX;
      } else {
        row = tsdbNextIterRow(pIter->pIter);
        key2 = keyNext;
      }
    }
  }
}

static int tsdbInsertSubBlock(STSCommitHandle *pTSCh, SBlock *pBlock) {
  ASSERT(pBlock->numOfSubBlocks == 0 && pTSCh->nBlocks > 0);

  SBlock *pSuperBlock = pTSCh->pBlockInfo->blocks + pTSCh->nBlocks - 1;

  ASSERT(pSuperBlock->numOfSubBlocks > 0 && pSuperBlock->numOfSubBlocks < TSDB_MAX_SUBBLOCKS);
  if (pSuperBlock->numOfSubBlocks == 1) {
    SBlock oBlock = *pSuperBlock;
    oBlock.numOfSubBlocks = 0;
    pSuperBlock->offset = sizeof(SBlock) * pTSCh->nSubBlocks;
    if (tsdbAddSubBlocks(pTSCh, &oBlock, 1) < 0) return -1;
  }
  pSuperBlock->numOfSubBlocks++;
  pSuperBlock->numOfRows += pBlock->numOfRows;
  pSuperBlock->keyFirst = MIN(pSuperBlock->keyFirst, pBlock->keyFirst);
  pSuperBlock->keyLast = MAX(pSuperBlock->keyLast, pBlock->keyLast);
  if (tsdbAddSubBlocks(pTSCh, pBlock, 1) < 0) return -1;

  return 0;
}