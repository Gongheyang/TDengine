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
#include "libgen.h"
#include "stdio.h"

#include "tsdbMain.h"

#define TSDB_DATA_DIR_NAME "data"

const char *tsdbFileSuffix[] = {".head", ".data", ".last", ".manifest", "meta", "config"};

int tsdbGetFileName(char *rootDir, int type, int vid, int fid, int seq, char **fname) {
  if (*fname == NULL) {
    *fname = (char *)malloc(TSDB_FILENAME_LEN);
    if (*fname == NULL) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      return -1;
    }
  }

  switch (type) {
    case TSDB_FILE_TYPE_HEAD:
    case TSDB_FILE_TYPE_DATA:
    case TSDB_FILE_TYPE_LAST:
      if (seq == 0) {  // For backward compatibility
        snprintf(*fname, TSDB_FILENAME_LEN, "%s/%s/v%df%d%s", rootDir, TSDB_DATA_DIR_NAME, vid, fid,
                 tsdbFileSuffix[type]);
      } else {
        snprintf(*fname, TSDB_FILENAME_LEN, "%s/%s/v%df%d%s-%d", rootDir, TSDB_DATA_DIR_NAME, vid, fid,
                 tsdbFileSuffix[type], seq);
      }
      break;
    case TSDB_FILE_TYPE_MANIFEST:
      snprintf(*fname, TSDB_FILENAME_LEN, "%s/v%d%s", rootDir, vid, tsdbFileSuffix[type]);
      break;
    case TSDB_FILE_TYPE_META:
    case TSDB_FILE_TYPE_CFG:
      snprintf(*fname, TSDB_FILENAME_LEN, "%s/%s", rootDir, tsdbFileSuffix[type]);
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