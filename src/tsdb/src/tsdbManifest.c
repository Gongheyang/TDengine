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
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "tchecksum.h"
#include "tsdbMain.h"

#define TSDB_MANIFEST_FILE_VERSION 0
#define TSDB_MANIFEST_FILE_HEADER_SIZE 128
#define TSDB_MANIFEST_END "C0D09F476DEF4A32B694A6A9E7B7B240"
#define TSDB_MANIFEST_END_SIZE 32

#define TSDB_MANIFEST_END_RECORD 0
#define TSDB_MANIFEST_META_RECORD 1
#define TSDB_MANIFEST_DATA_RECORD 2

typedef struct {
  int type;
  int len;
} SManifestRecord;

int tsdbInitManifestHandle(STsdbRepo *pRepo, SManifestHandle *pManifest) {
  STsdbCfg *pCfg = &(pRepo->config);

  pManifest->pBuffer = NULL;
  pManifest->contSize = 0;

  tsdbGetFileName(pRepo->rootDir, TSDB_FILE_TYPE_MANIFEST, pCfg->tsdbId, 0, 0, &(pManifest->fname));
  pManifest->fd = open(pManifest->fname, O_CREAT | O_APPEND, 0755);
  if (pManifest->fd < 0) {
    tsdbError("vgId:%d failed to open file %s since %s", REPO_ID(pRepo), fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (tsdbWriteManifestHeader(pRepo, pManifest) < 0) {
    tsdbCloseManifestHandle(pRepo, pManifest);
    return -1;
  }

  return 0;
}

void tsdbCloseManifestHandle(SManifestHandle *pManifest) {
  if (pManifest != NULL && pManifest->fd > 0) {
    close(pManifest->fd);
    pManifest->fd = -1;
  }

  remove(pManifest->fname);
  taosTZfree(pManifest->pBuffer);
  pManifest->pBuffer = NULL;
  pManifest->contSize = 0;
  return 0;
}

int tsdbAppendManifestRecord(SManifestHandle *pManifest, STsdbRepo *pRepo, int type) {
  ASSERT(pManifest->pBuffer != NULL && taosTSizeof(pManifest->pBuffer) >= pManifest->contSize);

  if (pManifest->contSize > 0) {
    if (tsdbManifestMakeMoreRoom(pManifest, sizeof(TSCKSUM)) < 0) return -1;
    pManifest->contSize += sizeof(TSCKSUM);
    taosCalcChecksumAppend(0, (uint8_t *)pManifest->pBuffer, pManifest->contSize);
  }

  SManifestRecord mRecord = {.type = type, .len = pManifest->contSize};

  // Write mRecord part
  if (taosTWrite(pManifest->fd, (void *)(&mRecord), sizeof(mRecord)) < sizeof(mRecord)) {
    tsdbError("vgId:%d failed to write %d bytes to file %s since %s", REPO_ID(pRepo), sizeof(mRecord), pManifest->fname,
              strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  // Write buffer part
  if (pManifest->contSize > 0 && taosTWrite(pManifest->fd, pManifest->pBuffer, pManifest->contSize) < pManifest->contSize) {
    tsdbError("vgId:%d failed to write %d bytes to file %s since %s", REPO_ID(pRepo), pManifest->contSize,
              pManifest->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (fsync(pManifest->fd) < 0) {
    tsdbError("vgId:%d failed to fsync file %s since %s", REPO_ID(pRepo), pManifest->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return 0;
}

int tsdbAppendManifestEnd(SManifestHandle *pManifest, STsdbRepo *pRepo) {
  pManifest->contSize = 0;
  return tsdbAppendManifestRecord(pManifest, pRepo, TSDB_MANIFEST_END_RECORD);
}

int tsdbManifestMakeRoom(SManifestHandle *pManifest, int expectedSize) {
  pManifest->pBuffer = taosTRealloc(pManifest->pBuffer, expectedSize);
  if (pManifest->pBuffer == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

int tsdbManifestMakeMoreRoom(SManifestHandle *pManifest, int moreSize) {
  return tsdbManifestMakeRoom(pManifest, pManifest->contSize + moreSize);
}

// TODO
bool tsdbIsManifestEnd(SManifestHandle *pManifest) {
  SManifestRecord mRecord;

  if (lseek(pManifest->fd, sizeof(mRecord), SEEK_END) < 0) {
    tsdbError("vgId:%d failed to lseek file %s since %s", REPO_ID(pRepo), pManifest->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return false;
  }

  if (taosTRead(pManifest->fd, (void *)(&mRecord), sizeof(mRecord)) < 0) {
    tsdbError("vgId:%d failed to read manifest end from file %s since %s", REPO_ID(pRepo), pManifest->fname,
              strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return false;
  }

  return (mRecord.type == TSDB_MANII)
}

int tsdbManifestRollBackOrForward(SManifestHandle *pManifest, bool isManifestEnd, STsdbRepo *pRepo) {
  SManifestRecord mRecord;

  if (lseek(pManifest->fd, TSDB_MANIFEST_FILE_HEADER_SIZE, SEEK_SET) < 0) {
    tsdbError("vgId:%d failed to lseek file %s since %s", REPO_ID(pRepo), pManifest->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return;
  }

  while (true) {
    ssize_t size = 0;

    size = taosTRead(pManifest->fd, (void *)(&mRecord), sizeof(mRecord));
    if (size < 0) {
      tsdbError("vgId:%d failed to read SManifestRecord part from file %s since %s", REPO_ID(pRepo), pManifest->fname,
                strerror(errno));
      terrno = TAOS_SYSTEM_ERROR(errno);
      return -1;
    }

    if (size < sizeof(mRecord)) break;
    if ((mRecord.type != TSDB_MANIFEST_DATA_RECORD && mRecord.type != TSDB_MANIFEST_META_RECORD && mRecord.type != TSDB_MANIFEST_END_RECORD) || mRecord.len < 0) {
      tsdbError("vgId:%d manifest file %s is broken since invalid mRecord content", REPO_ID(pRepo), pManifest->fname);
      terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
      return -1;
    }

    if (mRecord.type == TSDB_MANIFEST_END_RECORD) {
      ASSERT(isManifestEnd && mRecord.len == 0);
      break;
    }

    if (tsdbManifestMakeRoom(pManifest, mRecord.len) < 0) return -1;

    size = taosTRead(pManifest->fd, pManifest->pBuffer, mRecord.len);
    if (size < 0) {
      tsdbError("vgId:%d failed to read SManifestRecord content from file %s since %s", REPO_ID(pRepo), pManifest->fname,
                strerror(errno));
      terrno = TAOS_SYSTEM_ERROR(errno);
      return -1;
    }

    if (size < mRecord.len) break;

    if (!taosCheckChecksumWhole((uint8_t *)pManifest->pBuffer, size)) {
      tsdbError("vgId:%d manifest file %s is broken since checksum error", REPO_ID(pRepo), pManifest->fname);
      terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
      return -1;
    }

    if (mRecord.type == TSDB_MANIFEST_DATA_RECORD) {
      // func1(pManifest->pBuffer, mRecord.len, isManifestEnd);
    } else if (mRecord.type == TSDB_MANIFEST_META_RECORD) {
      // func2(pManifest->pBuffer, mRecord.len, isManifestEnd);
    } else {
      ASSERT(0);
    }
  }

  return 0;
  
}

int tsdbEncodeManifestRecord(SManifestHandle *pManifest) {
  pManifest->contSize = 0;
  
}

static int tsdbEncodeManifestHeader(void **buffer) {
  int len = taosEncodeFixedU32(buf, TSDB_MANIFEST_FILE_VERSION);
  return len;
}

static void *tsdbDecodeManifestHeader(void *buffer, uint32_t version) {
  buffer = taosDecodeFixedU32(buffer, &version);
  return buffer;
}

static int tsdbWriteManifestHeader(STsdbRepo *pRepo, SManifestHandle *pManifest) {
  char buffer[TSDB_MANIFEST_FILE_HEADER_SIZE] = "\0";
  tsdbEncodeManifestHeader(&buffer);

  taosCalcChecksumAppend(0, (uint8_t)buffer, TSDB_MANIFEST_FILE_HEADER_SIZE);
  if (taosTWrite(pManifest->fd, buffer, TSDB_MANIFEST_FILE_HEADER_SIZE) < 0) {
    tsdbError("vgId:%d failed to write file %s since %s", REPO_ID(pRepo), pManifest->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return 0;
}