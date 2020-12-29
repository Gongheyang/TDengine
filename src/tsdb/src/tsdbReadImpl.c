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

#include "tsdbMain.h"

static int tsdbLoadBlockDataImpl(SReadHandle *pReadH, SBlock *pBlock, SDataCols *pDataCols);
static int tsdbCheckAndDecodeColumnData(SDataCol *pDataCol, char *content, int32_t len, int8_t comp, int numOfRows,
                                        int maxPoints, char *buffer, int bufferSize);
static int tsdbLoadBlockDataColsImpl(SReadHandle *pReadH, SBlock *pBlock, SDataCols *pDataCols, int16_t *colIds,
                                     int numOfColIds);
static int tsdbLoadColData(SReadHandle *pReadH, SDFile *pFile, SBlock *pBlock, SBlockCol *pBlockCol,
                           SDataCol *pDataCol);

int tsdbInitReadHandle(SReadHandle *pReadH, STsdbRepo *pRepo) {
  STsdbMeta *pMeta = pRepo->tsdbMeta;
  STsdbCfg * pCfg = &pRepo->config;

  memset((void *)pReadH, 0, sizeof(*pReadH));
  pReadH->pRepo = pRepo;
  FILE_SET_INIT(&(pReadH->fset));

  pReadH->blockIdx = taosArrayInit(pMeta->nTables, sizeof(SBlockIdx));
  if (pReadH->blockIdx == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbDestroyReadHandle(pReadH);
    return -1;
  }

  pReadH->pDataCols[0] = tdNewDataCols();
  if ((pReadH->pDataCols[0] == NULL) || (pReadH->pDataCols[1] == NULL)) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbDestroyReadHandle(pReadH);
    return -1;
  }

  return 0;
}

void tsdbDestroyReadHandle(SReadHandle *pReadH) {
  if (pReadH) {
    taosTZfree(pReadH->pCBuffer);
    taosTZfree(pReadH->pBuffer);
    taosTZfree(pReadH->pBlockData);
    taosTZfree(pReadH->pBlockInfo);
    tdFreeDataCols(pReadH->pDataCols[0]);
    tdFreeDataCols(pReadH->pDataCols[1]);
    taosArrayDestroy(pReadH->blockIdx);
    tsdbCloseFSet(&(pReadH->fset), false);
    memset((void *)pReadH, 0, sizeof(*pReadH));
    FILE_SET_INIT(&(pReadH->fset));
  }
}

int tsdbSetAndOpenFSet(SReadHandle *pReadH, SDFileSet *pFSet) {
  // Clear existing state
  tdResetDataCols(pReadH->pDataCols[0]);
  tdResetDataCols(pReadH->pDataCols[1]);
  pReadH->pCurIdx = NULL;
  pReadH->cid = 0;
  pReadH->pTable = NULL;
  taosArrayClear(pReadH->blockIdx);
  tsdbCloseFSet(&(pReadH->pSet), false);

  // Set new state
  pReadH->fset = *pFSet;
  FILE_SET_INIT(&(pReadH->fset));

  return tsdbOpenFSet(&(pReadH->fset), O_RDONLY);
}

int tsdbLoadBlockIdx(SReadHandle *pReadH) {
  char *    errMsg = NULL;
  SDFile *  pFile = HEAD_FILE_IN_SET(&(pReadH->fset));
  int       iter;
  SBlockIdx bidx;
  int64_t   len;
  void *    ptr;

  // Assume pReadH->blockIdx is clear and empty
  if (pFile->info.len <= 0) return 0;

  ASSERT(taosArrayGetSize(pReadH->blockIdx) == 0);

  if (tsdbBufMakeRoom((void **)(&pReadH->pBuffer), pFile->info.len) < 0) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    errMsg = "out of memory";
    goto _err;
  }

  // Load raw data from file
  if (taosLSeek(pFile->fd, pFile->info.offset, SEEK_SET) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    errMsg = "seek error";
    goto _err;
  }

  len = taosRead(pFile->fd, pReadH->pBuffer, pFile->info.len);
  if (len < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    errMsg = "read error";
    goto _err;
  }

  if ((len < pFile->info.len) || (!taosCheckChecksumWhole((uint8_t *)pReadH->pBuffer, pFile->info.len))) {
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    errMsg = "wrong checksum";
    goto _err;
  }

  // Decode the SBlockIdx array
  ptr = pReadH->pBuffer;
  iter = 0;
  while (POINTER_DISTANCE(ptr, pReadH->pBuffer) < (int)(len - sizeof(TSCKSUM))) {
    ptr = tsdbDecodeSBlockIdx(ptr, &bidx);
    if (ptr == NULL) {
      terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
      errMsg = "decode error";
      goto _err;
    }

    ASSERT(POINTER_DISTANCE(ptr, pReadH->pBuffer) <= (int)(pFile->info.len - sizeof(TSCKSUM)));

    if (taosArrayPush(pReadH->blockIdx, &bidx) == NULL) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      errMsg = "out of memory";
      goto _err;
    }

#ifndef NDEBUG
    if (iter > 0) {
      SBlockIdx *pIdx1 = taosArrayGet(pReadH->blockIdx, iter - 1);
      SBlockIdx *pIdx2 = taosArrayGet(pReadH->blockIdx, iter);
      assert(pIdx1->tid < pIdx2->tid);
    }
#endif

    iter++;
  }

  return 0;

_err:
  tsdbError("vgId:%d failed to load SBlockIdx part since %s, file:%s offset:%u len:%u size:%u reason:%s",
            REPO_ID(pRepo), errMsg, TSDB_FILENAME(pFile), pFile->info.offset, pFile->info.len, pFile->info.size,
            tstrerror(terrno));
  return -1;
}

int tsdbSetReadTable(SReadHandle *pReadH, STable *pTable) {
  STSchema *pSchema = NULL;
  size_t    size = 0;

  pReadH->pTable = pTable;
  pSchema = tsdbGetTableSchemaImpl(pTable, false, false, -1);

  if (tdInitDataCols(pReadH->pDataCols[0], pSchema) < 0) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  if (tdInitDataCols(pReadH->pDataCols[1], pSchema) < 0) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  size = taosArrayGetSize(pReadH->blockIdx);
  if (size == 0) {
    pReadH->pCurIdx = NULL;
  } else {
    while (true) {
      if (pReadH->cid >= size) {
        pReadH->pCurIdx = NULL;
        break;
      }

      SBlockIdx *pIdx = (SBlockIdx *)taosArrayGet(pReadH->blockIdx, pReadH->cid);
      if (pIdx->tid == TABLE_TID(pTable)) {
        if (pIdx->uid == TABLE_UID(pTable)) {
          pReadH->pCurIdx = pIdx;
        } else {
          pReadH->pCurIdx = NULL;
        }
        pReadH->cid++;
        break;
      } else if (pIdx->tid > TABLE_TID(pTable)) {
        pReadH->pCurIdx = NULL;
        break;
      } else {
        pReadH->cid++;
      }
    }
  }

  return 0;
}

int tsdbLoadBlockInfo(SReadHandle *pReadH, void *target) {
  ASSERT(pReadH->pCurIdx != NULL && pReadH->pCurIdx->uid == TABLE_UID(pReadH->pTable));

  char *     errMsg = NULL;
  SDFile *   pFile = HEAD_FILE_IN_SET(&(pReadH->fset));
  SBlockIdx *pIdx = pReadH->pCurIdx;
  int64_t    size = 0;

  if (tsdbBufMakeRoom((void **)(&pReadH->pBlockInfo), pIdx->len) < 0) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    errMsg = "out of memory";
    goto _err;
  }

  if (taosLSeek(pFile->fd, pIdx->offset, SEEK_SET) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    errMsg = "seek error";
    goto _err;
  }

  size = taosRead(pFile->fd, (void *)pReadH->pBlockInfo, pIdx->len);
  if (size < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    errMsg = "read error";
    goto _err;
  }

  if ((size < pIdx->len) || (!taosCheckChecksumWhole((uint8_t *)pReadH->pBlockInfo, pIdx->len))) {
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    errMsg = "file corrupted";
    goto _err;
  }

  if (target) memcpy(target, pReadH->pBlockInfo, pIdx->len);

  return 0;

_err:
  tsdbError("vgId:%d failed to load SBlockInfo part since %s, table:%s uid:%" PRIu64 " offset:%u len:%u reason:%s",
            REPO_ID(pRepo), errMsg, TABLE_CHAR_NAME(pReadH->pTable), TABLE_UID(pReadH->pTable), pIdx->offset, pIdx->len,
            tstrerror(terrno));
  return -1;
}

int tsdbLoadBlockCols(SReadHandle *pReadH, SBlock *pBlock) {
  ASSERT(pBlock->numOfSubBlocks <= 1);

  SDFile *pFile;
  char *  errMsg = NULL;
  int64_t size;
  size_t  tsize;

  if (pBlock->last) {
    pFile = LAST_FILE_IN_SET(&(pReadH->fset));
  } else {
    pFile = DATA_FILE_IN_SET(&(pReadH->fset));
  }

  if (taosLSeek(pFile->fd, pBlock->offset, SEEK_SET) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    errMsg = "seek error";
    goto _err;
  }

  tsize = TSDB_GET_COMPCOL_LEN(pBlock->numOfCols);

  if (tsdbBufMakeRoom((void **)(&pReadH->pBlockData), tszie) < 0) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    errMsg = "out of memory";
    goto _err;
  }

  size = taosRead(pFile->fd, (void *)pReadH->pBlockData, tsize);
  if (size < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    errMsg = "read error";
    goto _err;
  }

  if (size < tsize) {
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    errMsg = "read inadequate bytes";
    goto _err;
  }

  if (!taosCheckChecksumWhole((uint8_t *)pReadH->pBlockData, (uint32_t)tsize)) {
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    errMsg = "wrong checksum";
    goto _err;
  }

  ASSERT(pBlock->numOfCols == pReadH->pCompData->numOfCols);

  return 0;

_err:
  tsdbError("vgId:%d failed to load SBlockCol part since %s, file:%s offset:%" PRId64 " len:%d", REPO_ID(pReadH->pRepo),
            errMsg, TSDB_FILENAME(pFile), pBlock->offset, pBlock->len);
  return -1;
}

int tsdbLoadBlockData(SReadHandle *pReadH, SBlock *pBlock, SBlockInfo *pBlockInfo) {
  SBlock *pIBlock = pBlock;
  int     nSub = pBlock->numOfSubBlocks;

  ASSERT(nSub > 0);
  if (nSub > 1) {
    if (pBlockInfo == NULL) {
      pIBlock = (SBlock *)POINTER_SHIFT(pReadH->pBlockInfo, pBlock->offset);
    } else {
      pIBlock = (SBlock *)POINTER_SHIFT(pBlockInfo, pBlock->offset);
    }
  }

  // Load the first block/sub-block
  tdResetDataCols(pReadH->pDataCols[0]);
  if (tsdbLoadBlockDataImpl(pReadH, pIBlock, pReadH->pDataCols[0]) < 0) return -1;

  // Load and merge the following sub-blocks
  for (int i = 1; i < nSub; i++) {
    tdResetDataCols(pReadH->pDataCols[1]);
    pIBlock++;

    ASSERT(pIBlock->numOfSubBlocks == 0);

    if (tsdbLoadBlockDataImpl(pReadH, pIBlock, pReadH->pDataCols[1]) < 0) return -1;
    if (tdMergeDataCols(pReadH->pDataCols[0], pReadH->pDataCols[1], pReadH->pDataCols[1]->numOfRows) < 0) return -1;
  }

  ASSERT(pReadH->pDataCols[0]->numOfRows == pBlock->numOfRows);
  ASSERT(dataColsKeyFirst(pReadH->pDataCols[0]) == pBlock->keyFirst);
  ASSERT(dataColsKeyLast(pReadH->pDataCols[0]) == pBlock->keyLast);

  return 0;
}

int tsdbLoadBlockDataCols(SReadHandle *pReadH, SBlock *pBlock, SBlockInfo *pBlockInfo, int16_t *colIds, int numOfColIds) {
  ASSERT(pBlock->numOfSubBlocks >= 1);  // Must be super block

  SBlock *pIBlock = pBlock;
  int     nSub = pBlock->numOfSubBlocks;

  ASSERT(nSub > 0);
  if (nSub > 1) {
    if (pBlockInfo == NULL) {
      pIBlock = (SBlock *)POINTER_SHIFT(pReadH->pBlockInfo, pBlock->offset);
    } else {
      pIBlock = (SBlock *)POINTER_SHIFT(pBlockInfo, pBlock->offset);
    }
  }

  // Load the first block/sub-block
  tdResetDataCols(pReadH->pDataCols[0]);
  if (tsdbLoadBlockDataColsImpl(pReadH, pIBlock, pReadH->pDataCols[0], colIds, numOfColIds) < 0) return -1;;

  // Load and merge the following sub-blocks
  for (int i = 1; i < nSub; i++) {
    tdResetDataCols(pReadH->pDataCols[1]);
    pIBlock++;

    ASSERT(pIBlock->numOfSubBlocks == 0);

    if (tsdbLoadBlockDataColsImpl(pReadH, pIBlock, pReadH->pDataCols[1], colIds, numOfColIds) < 0) return -1;
    if (tdMergeDataCols(pReadH->pDataCols[0], pReadH->pDataCols[1], pReadH->pDataCols[1]->numOfRows) < 0) return -1;
  }

  ASSERT(pReadH->pDataCols[0]->numOfRows == pBlock->numOfRows);
  ASSERT(dataColsKeyFirst(pReadH->pDataCols[0]) == pBlock->keyFirst);
  ASSERT(dataColsKeyLast(pReadH->pDataCols[0]) == pBlock->keyLast);

  return 0;
}

static int tsdbLoadBlockDataImpl(SReadHandle *pReadH, SBlock *pBlock, SDataCols *pDataCols) {
  ASSERT(pBlock->numOfSubBlocks <= 1);

  char *      errMsg = "";
  SDFile *    pFile;
  SBlockData *pBlockData;
  int64_t     size;
  int32_t     tsize;

  if (pBlock->last) {
    pFile = LAST_FILE_IN_SET(&(pReadH->fset));
  } else {
    pFile = DATA_FILE_IN_SET(&(pReadH->fset));
  }

  if (tsdbBufMakeRoom((void **)(&pReadH->pBuffer), pBlock->len) < 0) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    errMsg = "out of memory";
    goto _err;
  }

  // Load data from pFile
  pBlockData = (SBlockData *)pReadH->pBuffer;

  if (taosLSeek(pFile->fd, pBlock->offset, SEEK_SET) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    errMsg = "seek error";
    goto _err;
  }

  size = taosRead(pFile->fd, (void *)pBlockData, pBlock->len);
  if (size < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    errMsg = "read error";
    goto _err;
  }

  if (size < pBlock->len) {
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    errMsg = "file corrupted with no enough data";
    goto _err;
  }

  tsize = TSDB_GET_COMPCOL_LEN(pBlock->numOfCols);
  if (!taosCheckChecksumWhole((uint8_t *)pBlockData, tsize)) {
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    errMsg = "file corrupted with SBlockCol wrong checksum";
    goto _err;
  }

  ASSERT(pBlockData->numOfCols == pBlock->numOfCols);

  pDataCols->numOfRows = pBlock->numOfRows;
  int bcol = 0;  // loop iter for SBlockCol object
  int dcol = 0;  // loop iter for SDataCols object
  while (dcol < pDataCols->numOfCols) {
    SDataCol *pDataCol = &(pDataCols->cols[dcol]);
    if (dcol != 0 && bcol >= pBlockData->numOfCols) {
      // Set current column as NULL and forward
      dataColSetNEleNull(pDataCol, pBlock->numOfRows, pDataCols->maxPoints);
      dcol++;
      continue;
    }

    int16_t tcolId = 0;
    int32_t toffset = TSDB_KEY_COL_OFFSET;
    int32_t tlen = pBlock->keyLen;

    if (dcol != 0) {
      SBlockCol *pCompCol = &(pBlockData->cols[bcol]);
      tcolId = pCompCol->colId;
      toffset = pCompCol->offset;
      tlen = pCompCol->len;
    } else {
      ASSERT(pDataCol->colId == tcolId);
    }

    if (tcolId == pDataCol->colId) {
      if (pBlock->algorithm == TWO_STAGE_COMP) {
        int zsize = pDataCol->bytes * pBlock->numOfRows + COMP_OVERFLOW_BYTES;
        if (pDataCol->type == TSDB_DATA_TYPE_BINARY || pDataCol->type == TSDB_DATA_TYPE_NCHAR) {
          zsize += (sizeof(VarDataLenT) * pBlock->numOfRows);
        }
        if (tsdbBufMakeRoom((void **)(&pReadH->pCBuffer), zsize) < 0) {
          terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
          errMsg = "out of memory";
          goto _err;
        }
      }

      if (tsdbCheckAndDecodeColumnData(pDataCol, POINTER_SHIFT(pBlockData, tsize + toffset), tlen, pBlock->algorithm,
                                       pBlock->numOfRows, pDataCols->maxPoints, pReadH->pCBuffer,
                                       (int32_t)taosTSizeof(pReadH->pCBuffer)) < 0) {
        tsdbError("vgId:%d failed to load block data since %s, file:%s offset:%" PRId64 " len:%d bcol:%d",
                  REPO_ID(pRepo), errMsg, TSDB_FILENAME(pFile), pBlock->offset, pBlock->len, bcol);
        return -1;
      }

      if (dcol != 0) bcol++;
      dcol++;
    } else if (tcolId < pDataCol->colId) {
      bcol++;
    } else {
      dataColSetNEleNull(pDataCol, pBlock->numOfRows, pDataCols->maxPoints);
      dcol++;
    }
  }

  return 0;

_err:
  tsdbError("vgId:%d failed to load block data since %s, file:%s offset:%" PRId64 " len:%d", REPO_ID(pRepo), errMsg,
            TSDB_FILENAME(pFile), pBlock->offset, pBlock->len);
  return -1;
}

static int tsdbCheckAndDecodeColumnData(SDataCol *pDataCol, char *content, int32_t len, int8_t comp, int numOfRows,
                                        int maxPoints, char *buffer, int bufferSize) {
  // Verify by checksum
  if (!taosCheckChecksumWhole((uint8_t *)content, len)) {
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    return -1;
  }

  // Decode the data
  if (comp) {
    // // Need to decompress
    int tlen = (*(tDataTypeDesc[pDataCol->type].decompFunc))(content, len - sizeof(TSCKSUM), numOfRows, pDataCol->pData,
                                                             pDataCol->spaceSize, comp, buffer, bufferSize);
    if (tlen <= 0) {
      tsdbError("decompress error");
      terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
      return -1;
    }
    pDataCol->len = tlen;
    if (pDataCol->type == TSDB_DATA_TYPE_BINARY || pDataCol->type == TSDB_DATA_TYPE_NCHAR) {
      dataColSetOffset(pDataCol, numOfRows);
    }
  } else {
    // No need to decompress, just memcpy it
    pDataCol->len = len - sizeof(TSCKSUM);
    memcpy(pDataCol->pData, content, pDataCol->len);
    if (pDataCol->type == TSDB_DATA_TYPE_BINARY || pDataCol->type == TSDB_DATA_TYPE_NCHAR) {
      dataColSetOffset(pDataCol, numOfRows);
    }
  }
  return 0;
}

static int tsdbLoadBlockDataColsImpl(SReadHandle *pReadH, SBlock *pBlock, SDataCols *pDataCols, int16_t *colIds, int numOfColIds) {
  ASSERT(pBlock->numOfSubBlocks <= 1 && colIds[0] == 0);

  SDFile *  pFile;
  SBlockCol blockCol = {0};

  if (pBlock->last) {
    pFile = LAST_FILE_IN_SET(&(pReadH->fset));
  } else {
    pFile = DATA_FILE_IN_SET(&(pReadH->fset));
  }

  // If only load timestamp column, no need to load SBlockData part
  if (numOfColIds > 1 && tsdbLoadBlockCols(pReadH, pBlock) < 0) return -1;

  pDataCols->numOfRows = pBlock->numOfRows;

  int dcol = 0;
  int ccol = 0;
  for (int i = 0; i < numOfColIds; i++) {
    int16_t    colId = colIds[i];
    SDataCol * pDataCol = NULL;
    SBlockCol *pBlockCol = NULL;

    while (true) {
      if (dcol >= pDataCols->numOfCols) {
        pDataCol = NULL;
        break;
      }
      pDataCol = &pDataCols->cols[dcol];
      if (pDataCol->colId > colId) {
        pDataCol = NULL;
        break;
      } else {
        dcol++;
        if (pDataCol->colId == colId) break;
      }
    }

    if (pDataCol == NULL) continue;
    ASSERT(pDataCol->colId == colId);

    if (colId == 0) {  // load the key row
      blockCol.colId = colId;
      blockCol.len = pBlock->keyLen;
      blockCol.type = pDataCol->type;
      blockCol.offset = TSDB_KEY_COL_OFFSET;
      pBlockCol = &blockCol;
    } else {  // load non-key rows
      while (true) {
        if (ccol >= pBlock->numOfCols) {
          pBlockCol = NULL;
          break;
        }

        pBlockCol = &(pReadH->pCompData->cols[ccol]);
        if (pBlockCol->colId > colId) {
          pBlockCol = NULL;
          break;
        } else {
          ccol++;
          if (pBlockCol->colId == colId) break;
        }
      }

      if (pBlockCol == NULL) {
        dataColSetNEleNull(pDataCol, pBlock->numOfRows, pDataCols->maxPoints);
        continue;
      }

      ASSERT(pBlockCol->colId == pDataCol->colId);
    }

    if (tsdbLoadColData(pReadH, pFile, pBlock, pBlockCol, pDataCol) < 0) goto _err;
  }

  return 0;

_err:
  return -1;
}

static int tsdbLoadColData(SReadHandle *pReadH, SDFile *pFile, SBlock *pBlock, SBlockCol *pBlockCol,
                           SDataCol *pDataCol) {
  ASSERT(pDataCol->colId == pBlockCol->colId);
  int tsize = pDataCol->bytes * pBlock->numOfRows + COMP_OVERFLOW_BYTES;
  if (tsdbBufMakeRoom((void **)(&pReadH->pBuffer), tsize) < 0) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  if (tsdbBufMakeRoom((void **)(&pReadH->pCBuffer), tsize) < 0) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  int64_t offset = pBlock->offset + TSDB_GET_COMPCOL_LEN(pBlock->numOfCols) + pBlockCol->offset;
  if (lseek(pFile->fd, (off_t)offset, SEEK_SET) < 0) {
    tsdbError("vgId:%d failed to lseek file %s since %s", REPO_ID(pReadH->pRepo), TSDB_FILE_NAME(pFile),
              strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (taosRead(pFile->fd, pReadH->pBuffer, pBlockCol->len) < pBlockCol->len) {
    tsdbError("vgId:%d failed to read %d bytes from file %s since %s", REPO_ID(pReadH->pRepo), pBlockCol->len,
              TSDB_FILE_NAME(pFile), strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (tsdbCheckAndDecodeColumnData(pDataCol, pReadH->pBuffer, pBlockCol->len, pBlock->algorithm, pBlock->numOfRows,
                                   pReadH->pRepo->config.maxRowsPerFileBlock, pReadH->pCBuffer,
                                   (int32_t)taosTSizeof(pReadH->pCBuffer)) < 0) {
    tsdbError("vgId:%d file %s is broken at column %d offset %" PRId64, REPO_ID(pReadH->pRepo), TSDB_FILE_NAME(pFile),
              pBlockCol->colId, offset);
    return -1;
  }

  return 0;
}