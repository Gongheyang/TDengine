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


const char *tsdbFileSuffix[] = {".head", ".data", ".last", ".manifest", "meta", "config"};

int tsdbGetFileName(char *rootDir, int type, int vid, int fid, int seq, char *fname) {
  switch (type) {
    case TSDB_FILE_TYPE_HEAD:
    case TSDB_FILE_TYPE_DATA:
    case TSDB_FILE_TYPE_LAST:
      if (seq == 0) {  // For backward compatibility
        snprintf(fname, TSDB_FILENAME_LEN, "%s/%s/v%df%d%s", rootDir, TSDB_DATA_DIR_NAME, vid, fid,
                 tsdbFileSuffix[type]);
      } else {
        snprintf(fname, TSDB_FILENAME_LEN, "%s/%s/v%df%d%s-%d", rootDir, TSDB_DATA_DIR_NAME, vid, fid,
                 tsdbFileSuffix[type], seq);
      }
      break;
    case TSDB_FILE_TYPE_MANIFEST:
      snprintf(fname, TSDB_FILENAME_LEN, "%s/v%d%s", rootDir, vid, tsdbFileSuffix[type]);
      break;
    case TSDB_FILE_TYPE_META:
    case TSDB_FILE_TYPE_CFG:
      snprintf(fname, TSDB_FILENAME_LEN, "%s/%s", rootDir, tsdbFileSuffix[type]);
      break;
    default:
      ASSERT(0);
      break;
  }

  return 0;
}

int tsdbParseFileName(char *fname, int *type, int *vid, int *fid, int *seq) {
  // TODO
  return 0;
}

int tsdbGetNextSeqNum(int currentNum) {
  if (currentNum == 0) {
    return 1;
  } else {
    return 0;
  }
}

int tsdbEncodeBlockIdx(void **buf, SBlockIdx *pBlockIdx) {
  int tlen = 0;

  tlen += taosEncodeVariantI32(buf, pBlockIdx->tid);
  tlen += taosEncodeVariantU32(buf, pBlockIdx->len);
  tlen += taosEncodeVariantU32(buf, pBlockIdx->offset);
  tlen += taosEncodeFixedU8(buf, pBlockIdx->hasLast);
  tlen += taosEncodeVariantU32(buf, pBlockIdx->numOfBlocks);
  tlen += taosEncodeFixedU64(buf, pBlockIdx->uid);
  tlen += taosEncodeFixedU64(buf, pBlockIdx->maxKey);

  return tlen;
}

void *tsdbDecodeBlockIdx(void *buf, SBlockIdx *pBlockIdx) {
  uint8_t  hasLast = 0;
  uint32_t numOfBlocks = 0;
  uint64_t uid = 0;
  uint64_t maxKey = 0;

  if ((buf = taosDecodeVariantI32(buf, &(pBlockIdx->tid))) == NULL) return NULL;
  if ((buf = taosDecodeVariantU32(buf, &(pBlockIdx->len))) == NULL) return NULL;
  if ((buf = taosDecodeVariantU32(buf, &(pBlockIdx->offset))) == NULL) return NULL;
  if ((buf = taosDecodeFixedU8(buf, &(hasLast))) == NULL) return NULL;
  if ((buf = taosDecodeVariantU32(buf, &(numOfBlocks))) == NULL) return NULL;
  if ((buf = taosDecodeFixedU64(buf, &uid)) == NULL) return NULL;
  if ((buf = taosDecodeFixedU64(buf, &maxKey)) == NULL) return NULL;

  pBlockIdx->hasLast = hasLast;
  pBlockIdx->numOfBlocks = numOfBlocks;
  pBlockIdx->uid = uid;
  pBlockIdx->maxKey = (TSKEY)maxKey;

  return buf;
}

// TODO: make it static FORCE_INLINE
void tsdbResetFGroupFd(SFileGroup *pFGroup) {
  for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
    TSDB_FILE_IN_FGROUP(pFGroup, type)->fd = -1;
  }
}