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

typedef struct {
  STsdbRepo * pRepo;
  SFileGroup  fGroup;
  TSKEY       minKey;
  TSKEY       maxKey;
  SBlockIdx * pBlockIdx;
  int         nBlockIdx;
  uint64_t    uid;
  int32_t     tid;
  SBlockInfo *pBlockInfo;
  SDataCols * pDataCols[2];
  void *      pBuf;
  void *      pCBuf;
} SReadHandle;

#define TSDB_READ_FILE(pReadH, type) (&((pReadH)->fGroup.files[(type)]))

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