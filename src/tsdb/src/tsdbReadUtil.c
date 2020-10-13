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

#include "tchecksum.h"
#include "tdataformat.h"
#include "tscompression.h"
#include "tsdbMain.h"

typedef struct {
  STsdbRepo * pRepo;
  SFileGroup  fGroup;
  TSKEY       minKey;
  TSKEY       maxKey;
  SBlockIdx * pBlockIdx;
  int         nBlockIdx;
  SBlockIdx * pCurBlockIdx;
  STable *    pTable;
  SBlockInfo *pBlockInfo;
  SDataCols * pDataCols[2];
  void *      pBuf;
  void *      pCBuf;
} SReadHandle;

#define TSDB_READ_FILE(pReadH, type) (&((pReadH)->fGroup.files[(type)]))
#define TSDB_BLOCK_DATA_LEN(nCols) (sizeof(SBlockData) + sizeof(SBlockCol) * (nCols) + sizeof(TSCKSUM))

int tsdbInitReadHandle(SReadHandle *pReadH, STsdbRepo *pRepo) {
  pReadH->pRepo = pRepo;
  return 0;
}

void tsdbDestroyReadHandle(SReadHandle *pReadH) {
  // TODO
}

int tsdbSetAndOpenFGroup(SReadHandle *pReadH, SFileGroup *pFGroup) {
  STsdbRepo *pRepo = pReadH->pRepo;
  pReadH->fGroup = *pFGroup;

  for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
    pReadH->fGroup.files[type].fd = -1;
  }

  for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
    SFile *pFile = &(pReadH->fGroup.files[type]);

    if (pFile->fname[0] != '\0') {
      pFile->fd = open(pFile->fname, O_RDONLY);
      if (pFile->fd < 0) {
        tsdbError("vgId:%d failed to open file %s since %s", REPO_ID(pRepo), pFile->fname, strerror(errno));
        terrno = TAOS_SYSTEM_ERROR(errno);
        tsdbCloseAndUnsetFile(pReadH);
        return -1;
      }
    }
  }

  return 0;
}

void tsdbCloseAndUnsetFile(SReadHandle *pReadH) {
  for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
    SFile *pFile = &(pReadH->fGroup.files[type]);

    if (pFile->fd > 0) {
      (void)close(pFile->fd);
      pFile->fd = -1;
    }
  }
}

int tsdbLoadBlockIdx(SReadHandle *pReadH) {
  STsdbRepo *pRepo = pReadH->pRepo;
  SFile *    pFile = &(pReadH->fGroup.files[TSDB_FILE_TYPE_HEAD]);

  if (pFile->fd < 0 || pFile->info.len == 0) {
    pReadH->nBlockIdx = 0;
    return 0;
  }

  if (tsdbAllocBuf(&(pReadH->pBuf), pFile->info.len) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (lseek(pFile->fd, pFile->info.offset, SEEK_SET) < 0) {
    tsdbError("vgId:%d failed to lseek file %s since %s", REPO_ID(pRepo), pFile->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  ssize_t ret = taosTRead(pFile->fd, pReadH->pBuf, pFile->info.len);
  if (ret < 0) {
    tsdbError("vgId:%d failed to read block idx part from file %s since %s", REPO_ID(pRepo), pFile->fname,
              strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (ret < pFile->info.len || !taosCheckChecksumWhole((uint8_t *)pReadH->pBuf, pFile->info.len)) {
    tsdbError("vgId:%d block idx part is corrupted in file %s, offset %u len %u", REPO_ID(pRepo), pFile->fname,
              pFile->info.offset, pFile->info.len);
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    return -1;
  }

  if (tsdbDecodeBlockIdxArray(pReadH) < 0) {
    tsdbError("vgId:%d error occurs while decoding block idx part from file %s", REPO_ID(pRepo), pFile->fname);
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    return -1;
  }

  return 0;
}

int tsdbSetReadTable(SReadHandle *pReadH, STable *pTable) {
  pReadH->pTable = pTable;
  // TODO
  // pReadH->pCurBlockIdx = NULL;
  return 0;
}

int tsdbLoadBlockInfo(SReadHandle *pReadH) {
  if (pReadH->pCurBlockIdx == NULL) return 0;

  SFile *    pFile = TSDB_READ_FILE(pReadH, TSDB_FILE_TYPE_HEAD);
  SBlockIdx *pBlockIdx = pReadH->pCurBlockIdx;
  STsdbRepo *pRepo = pReadH->pRepo;

  ASSERT(pFile->fd > 0);

  if (tsdbAllocBuf(&((void *)pReadH->pBlockInfo), pBlockIdx->len) < 0) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  if (lseek(pFile->fd, pBlockIdx->offset, SEEK_SET) < 0) {
    tsdbError("vgId:%d failed to lseek file %s since %s", REPO_ID(pRepo), pFile->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  ssize_t ret = taosTRead(pFile->fd, (void *)pReadH->pBlockInfo, pBlockIdx->len);
  if (ret < 0) {
    tsdbError("vgId:%d failed to read block info part of table %s from file %s since %s", REPO_ID(pRepo),
              TABLE_CHAR_NAME(pReadH->pTable), pFile->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (ret < pBlockIdx->len || tsdbVerifyBlockInfo(pReadH->pBlockInfo, pBlockIdx) < 0) {
    tsdbError("vgId:%d table %s block info part is corrupted from file %s since %s", REPO_ID(pRepo),
              TABLE_CHAR_NAME(pReadH->pTable), pFile->fname, strerror(errno));
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    return -1;
  }

  return 0;
}

int tsdbLoadBlockData(SReadHandle *pReadH, SBlock *pBlock, SBlockInfo *pBlockInfo) {
  ASSERT(pBlock->numOfSubBlocks >= 1);

  SBlock *pSubBlock = pBlock;
  int     nSubBlocks = pBlock->numOfSubBlocks;

  if (nSubBlocks > 1) {
    if (pBlockInfo == NULL) pBlockInfo = pReadH->pBlockInfo;
    pSubBlock = (SBlock *)POINTER_SHIFT((void *)pBlockInfo, pBlock->offset);
  }

  if (tsdbLoadBlockDataImpl(pReadH, pSubBlock, pReadH->pDataCols[0]) < 0) return -1;
  for (int i = 1; i < nSubBlocks; i++) {
    pSubBlock++;
    if (tsdbLoadBlockDataImpl(pReadH, pSubBlock, pReadH->pDataCols[1]) < 0) return -1;
    if (tdMergeDataCols(pReadH->pDataCols[0], pReadH->pDataCols[1], pReadH->pDataCols[1]->numOfRows) < 0) return -1;
  }

  ASSERT(pReadH->pDataCols[0]->numOfRows == pBlock->numOfRows);
  ASSERT(dataColsKeyFirst(pReadH->pDataCols[0]) == pBlock->keyFirst);
  ASSERT(dataColsKeyLast(pReadH->pDataCols[0]) == pBlock->keyLast);

  return 0;
}

int tsdbLoadBlockDataCols(SReadHandle *pReadH, SBlock *pBlock, SBlockInfo *pBlockInfo, int16_t *colIds, int numOfCols) {
  ASSERT(pBlock->numOfSubBlocks >= 1);

  SBlock *pSubBlock = pBlock;
  int     nSubBlock = pBlock->numOfSubBlocks;

  if (nSubBlock > 1) {
    if (pBlockInfo == NULL) pBlockInfo = pReadH->pBlockInfo;
    pSubBlock = (SBlock *)POINTER_SHIFT((void *)pBlockInfo, pBlock->offset);
  }

  if (tsdbLoadBlockDataColsImpl(pReadH, pSubBlock, pReadH->pDataCols[0], colIds, numOfCols) < 0) return -1;
  for (int i = 1; i < nSubBlock; i++) {
    pSubBlock++;
    if (tsdbLoadBlockDataColsImpl(pReadH, pSubBlock, pReadH->pDataCols[1], colIds, numOfCols) < 0) return -1;
    if (tdMergeDataCols(pReadH->pDataCols[0], pReadH->pDataCols[1], pReadH->pDataCols[1]->numOfRows) < 0) goto _err;
  }

  return 0;
}

static int tsdbLoadBlockDataImpl(SReadHandle *pReadH, SBlock *pBlock, SDataCols *pDataCols) {
  ASSERT(pBlock->numOfSubBlocks  <= 1);

  STsdbRepo *pRepo = pReadH->pRepo;
  SFile *pFile =
      (pBlock->last) ? TSDB_READ_FILE(pReadH, TSDB_FILE_TYPE_LAST) : TSDB_READ_FILE(pReadH, TSDB_FILE_TYPE_DATA);

  if (tsdbAllocBuf(&(pReadH->pBuf), pBlock->len) < 0) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  if (lseek(pFile->fd, pBlock->offset, SEEK_SET) < 0) {
    tsdbError("vgId:%d failed to lseek file %s since %s", REPO_ID(pRepo), pFile->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  int ret = taosTRead(pFile->fd, (void *)pReadH->pBuf, pBlock->len);
  if (ret < 0) {
    tsdbError("vgId:%d failed to read block data part from file %s since %s", REPO_ID(pRepo), pFile->fname,
              strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  int tsize = TSDB_BLOCK_DATA_LEN(pBlock->numOfCols);
  if (ret < pBlock->len || !taosCheckChecksumWhole((uint8_t *)pReadH->pBuf, tsize)) {
    tsdbError("vgId:%d block data part from file %s is corrupted", REPO_ID(pRepo), pFile->fname);
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    return -1;
  }

  SBlockData *pBlockData = (SBlockData *)pReadH->pBuf;

  ASSERT(pBlockData->delimiter == TSDB_FILE_DELIMITER);
  ASSERT(pBlockData->numOfCols = pBlock->numOfCols);

  tdResetDataCols(pDataCols);
  pDataCols->numOfRows = pBlock->numOfRows;

  int ccol = 0;
  int dcol = 0;
  while (dcol < pDataCols->numOfCols) {
    SDataCol *pDataCol = &(pDataCols->cols[dcol]);
    if (dcol != 0 && ccol >= pBlockData->numOfCols) {
      dataColSetNEleNull(pDataCol, pBlock->numOfRows, pDataCols->maxPoints);
      dcol++;
      continue;
    }

    int16_t tcolId = 0;
    int32_t toffset = TSDB_KEY_COL_OFFSET;
    int32_t tlen = pBlock->keyLen;

    if (dcol != 0) {
      SBlockCol *pBlockCol = &(pBlockData->cols[ccol]);
      tcolId = pBlockCol->colId;
      toffset = pBlockCol->offset;
      tlen = pBlockCol->len;
    } else {
      ASSERT(pDataCol->colId == tcolId);
    }

    if (tcolId == pDataCol->colId) {
      if (pBlock->algorithm == TWO_STAGE_COMP) {  // extend compression buffer
        int zsize = pDataCol->bytes * pBlock->numOfRows + COMP_OVERFLOW_BYTES;
        if (pDataCol->type == TSDB_DATA_TYPE_BINARY || pDataCol->type == TSDB_DATA_TYPE_NCHAR) {
          zsize += (sizeof(VarDataLenT) * pCompBlock->numOfRows);
        }

        if (tsdbAllocBuf(&(pReadH->pCBuf), zsize) < 0) {
          terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
          return -1;
        }
      }
      if (tsdbCheckAndDecodeColumnData(pDataCol, POINTER_SHIFT(pBlockData, tsize + toffset), tlen, pBlock->algorithm,
                                       pBlock->numOfRows, pDataCols->maxPoints, pReadH->pCBuf,
                                       (int32_t)taosTSizeof(pReadH->pCBuf)) < 0) {
        tsdbError("vgId:%d file %s is broken at column %d block offset %" PRId64 " column offset %d", REPO_ID(pRepo),
                  pFile->fname, tcolId, (int64_t)pBlock->offset, toffset);
        return -1;
      }

      if (dcol != 0) ccol++;
      dcol++;
    } else if (tcolId < pDataCol->colId) {
      ccol++;
    } else {
      dataColSetNEleNull(pDataCol, pBlock->numOfRows, pDataCols->maxPoints);
      dcol++;
    }
  }

  return 0;
}

static int tsdbLoadBlockDataColsImpl(SReadHandle *pReadH, SBlock *pBlock, SDataCols *pDataCols, int16_t *colIds,
                                     int numOfColIds) {
  ASSERT(pBlock->numOfSubBlocks <= 1);
  ASSERT(colIds[0] == 0);

  SFile *pFile =
      pBlock->last ? TSDB_READ_FILE(pReadH, TSDB_FILE_TYPE_LAST) : TSDB_READ_FILE(pReadH, TSDB_FILE_TYPE_DATA);
  SBlockCol compCol = {0};

  // If only load timestamp column, no need to load SBlockData part
  if (numOfColIds > 1 && tsdbLoadCompData(pHelper, pCompBlock, NULL) < 0) goto _err;

  pDataCols->numOfRows = pCompBlock->numOfRows;

  int dcol = 0;
  int ccol = 0;
  for (int i = 0; i < numOfColIds; i++) {
    int16_t   colId = colIds[i];
    SDataCol *pDataCol = NULL;
    SBlockCol *pCompCol = NULL;

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
      compCol.colId = colId;
      compCol.len = pCompBlock->keyLen;
      compCol.type = pDataCol->type;
      compCol.offset = TSDB_KEY_COL_OFFSET;
      pCompCol = &compCol;
    } else {  // load non-key rows
      while (true) {
        if (ccol >= pCompBlock->numOfCols) {
          pCompCol = NULL;
          break;
        }

        pCompCol = &(pHelper->pCompData->cols[ccol]);
        if (pCompCol->colId > colId) {
          pCompCol = NULL;
          break;
        } else {
          ccol++;
          if (pCompCol->colId == colId) break;
        }
      }

      if (pCompCol == NULL) {
        dataColSetNEleNull(pDataCol, pCompBlock->numOfRows, pDataCols->maxPoints);
        continue;
      }

      ASSERT(pCompCol->colId == pDataCol->colId);
    }

    if (tsdbLoadColData(pHelper, pFile, pCompBlock, pCompCol, pDataCol) < 0) goto _err;
  }

  return 0;

_err:
  return -1;
}

static int tsdbDecodeBlockIdxArray(SReadHandle *pReadH) {
  void *pBuf = pReadH->pBuf;
  SFile *pFile = &(pReadH->fGroup.files[TSDB_FILE_TYPE_HEAD]);

  pReadH->nBlockIdx = 0;
  while (POINTER_DISTANCE(pBuf, pReadH->pBuf) < (int)(pFile->info.len - sizeof(TSCKSUM))) {
    if (tsdbAllocBuf(&((void *)(pReadH->pBlockIdx), sizeof(SBlockIdx) * (pReadH->nBlockIdx + 1))) < 0) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      return -1;
    }

    pBuf = tsdbDecodeBlockIdx(pBuf, &(pReadH->pBlockIdx[pReadH->nBlockIdx]));
    if (pBuf == NULL) {
      tsdbError("vgId:%d failed to decode block idx part from file %s", REPO_ID(pRepo), pFile->fname);
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      return -1;
    }

    pReadH->nBlockIdx++;
    ASSERT(pReadH->nBlockIdx == 1 || (pReadH->pBlockIdx[pReadH->nBlockIdx-1].tid < (pReadH->pBlockIdx[pReadH->nBlockIdx-2].tid));
  }
  return 0;
}

static int tsdbAllocBuf(void **ppBuf, int size) {
  void *pBuf = *pBuf;

  int tsize = taosTSizeof(pBuf);
  if (tsize == 0) tsize = 1024;

  while (tsize < size) {
    tsize *= 2;
  }

  *ppBuf = taosTRealloc(pBuf, tsize);
  if (*ppBuf == NULL) return -1;
}

static int tsdbVerifyBlockInfo(SBlockInfo *pBlockInfo, SBlockIdx *pBlockIdx) {
  if (!taosCheckChecksumWhole((uint8_t *)pBlockInfo, pBlockIdx->len)) return -1;
  if (pBlockInfo->delimiter != TSDB_FILE_DELIMITER || pBlockInfo->uid != pBlockIdx->uid ||
      pBlockInfo->tid != pBlockIdx->tid)
    return -1;
  return 0;
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
      tsdbError("Failed to decompress column, file corrupted, len:%d comp:%d numOfRows:%d maxPoints:%d bufferSize:%d",
                len, comp, numOfRows, maxPoints, bufferSize);
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

static int tsdbLoadColData(SRWHelper *pHelper, SFile *pFile, SBlock *pCompBlock, SBlockCol *pCompCol,
                           SDataCol *pDataCol) {
  ASSERT(pDataCol->colId == pCompCol->colId);
  int tsize = pDataCol->bytes * pCompBlock->numOfRows + COMP_OVERFLOW_BYTES;
  pHelper->pBuffer = taosTRealloc(pHelper->pBuffer, pCompCol->len);
  if (pHelper->pBuffer == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  pHelper->compBuffer = taosTRealloc(pHelper->compBuffer, tsize);
  if (pHelper->compBuffer == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  int64_t offset = pCompBlock->offset + TSDB_GET_COMPCOL_LEN(pCompBlock->numOfCols) + pCompCol->offset;
  if (lseek(pFile->fd, (off_t)offset, SEEK_SET) < 0) {
    tsdbError("vgId:%d failed to lseek file %s since %s", REPO_ID(pHelper->pRepo), pFile->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (taosTRead(pFile->fd, pHelper->pBuffer, pCompCol->len) < pCompCol->len) {
    tsdbError("vgId:%d failed to read %d bytes from file %s since %s", REPO_ID(pHelper->pRepo), pCompCol->len, pFile->fname,
              strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (tsdbCheckAndDecodeColumnData(pDataCol, pHelper->pBuffer, pCompCol->len, pCompBlock->algorithm,
                                   pCompBlock->numOfRows, pHelper->pRepo->config.maxRowsPerFileBlock,
                                   pHelper->compBuffer, (int32_t)taosTSizeof(pHelper->compBuffer)) < 0) {
    tsdbError("vgId:%d file %s is broken at column %d offset %" PRId64, REPO_ID(pHelper->pRepo), pFile->fname,
              pCompCol->colId, offset);
    return -1;
  }

  return 0;
}