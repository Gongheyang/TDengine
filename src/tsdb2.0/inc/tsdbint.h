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

#ifndef _TD_TSDB_INT_H_
#define _TD_TSDB_INT_H_

#include "tsdb.h"
#include "tsdbLog.h"
#include "tsdbBufBlock.h"
#include "tsdbTableData.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct STsdbBufPool STsdbBufPool;
typedef struct SMemTable    SMemTable;

typedef struct {
  STsdbCfg        config;
  SMemTable *     mem;
  SMemTable *     imem;
  STsdbBufPool *  pPool;
  bool            repoLocked;
  pthread_mutex_t mutex;
} STsdbRepo;

#define REPO_CFG(r) (&((r)->config))
#define REPO_ID(r) ((r)->config.tsdbId)
#define REPO_MEM(r) ((r)->mem)
#define REPO_IMEM(r) ((r)->imem)
#define REPO_POOL(r) ((r)->pPool)
#define IS_REPO_LOCKED(r) ((r)->repoLocked)

#define REPO_SET_LOCKED(r)  \
  do {                      \
    (r)->repoLocked = true; \
  } while (0);

#define REPO_SET_UNLOCKED(r) \
  do {                       \
    (r)->repoLocked = false; \
  } while (0);

static FORCE_INLINE int tsdbLockRepo(STsdbRepo *pRepo) {
  int code = pthread_mutex_lock(&(pRepo->mutex));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  REPO_SET_LOCKED(pRepo);
  return 0;
}

static FORCE_INLINE int tsdbUnlockRepo(STsdbRepo *pRepo) {
  ASSERT(IS_REPO_LOCKED(pRepo));
  REPO_SET_UNLOCKED(pRepo);
  int code = pthread_mutex_unlock(&(pRepo->mutex));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

#ifdef __cplusplus
}
#endif

#include "tsdbBuffer.h"

#endif