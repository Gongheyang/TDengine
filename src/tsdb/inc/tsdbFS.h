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
#ifndef _TD_TSDB_FS_H_
#define _TD_TSDB_FS_H_

#ifdef __cplusplus
extern "C" {
#endif

#define TSDB_FILE_HEAD_SIZE    512
#define TSDB_FILE_DELIMITER    0xF00AFA0F
#define TSDB_FILE_INIT_MAGIC   0xFFFFFFFF

enum { TSDB_FILE_HEAD = 0, TSDB_FILE_DATA, TSDB_FILE_LAST, TSDB_FILE_MAX };

// For meta file
typedef struct {
  int64_t  size;
  int64_t  tombSize;
  int64_t  nRecords;
  int64_t  nDels;
  uint32_t magic;
} SMFInfo;

typedef struct {
  SMFInfo info;
  TFILE   f;
} SMFile;

// For .head/.data/.last file
typedef struct {
  uint32_t magic;
  uint32_t len;
  uint32_t totalBlocks;
  uint32_t totalSubBlocks;
  uint32_t offset;
  uint64_t size;
  uint64_t tombSize;
} SDFInfo;

typedef struct {
  SDFInfo info;
  TFILE   f;
} SDFile;

typedef struct {
  int    id;
  int    state;
  SDFile files[TSDB_FILE_MAX];
} SDFileSet;

/* Statistic information of the TSDB file system.
 */
typedef struct {
  int64_t fsversion; // file system version, related to program
  int64_t version;
  int64_t totalPoints;
  int64_t totalStorage;
} STsdbFSMeta;

typedef struct {
  int64_t     version;
  STsdbFSMeta meta;
  SMFile      mf;  // meta file
  SArray *    df;  // data file array
} SFSSnapshot;

typedef struct {
  pthread_rwlock_t lock;

  SFSSnapshot *curr;
  SFSSnapshot *new;
} STsdbFS;

int  tsdbOpenFS(STsdbRepo *pRepo);
void tsdbCloseFS(STsdbRepo *pRepo);
int  tsdbFSNewTxn(STsdbRepo *pRepo);
int  tsdbFSEndTxn(STsdbRepo *pRepo, bool hasError);
int  tsdbUpdateMFile(STsdbRepo *pRepo, SMFile *pMFile);
int  tsdbUpdateDFileSet(STsdbRepo *pRepo, SDFileSet *pSet);

#ifdef __cplusplus
}
#endif

#endif