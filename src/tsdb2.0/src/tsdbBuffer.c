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

#include "tsdbint.h"

struct STsdbBufPool {
  pthread_cond_t poolNotEmpty;
  int            bufBlockSize;
  int            tBufBlocks;
  int            nBufBlocks;
  int64_t        index;
  SList *        bufBlockList;
};

#define POOL_IS_EMPTY(p) ((listNeles(p)->bufBlockList) == 0)

static STsdbBufPool *tsdbNewBufPool(int bufBlockSize /*bytes*/, int tBufBlocks);
static STsdbBufPool *tsdbFreeBufPool(STsdbBufPool *pPool);
static int           tsdbAddBufBlockToPoolImpl(STsdbBufPool *pPool, STsdbBufBlock *pBufBlock);
static FORCE_INLINE SListNode *tsdbPopBufBlockNodeImpl(STsdbBufPool *pPool);
static FORCE_INLINE void       tsdbWaitPoolNotEmpty(STsdbRepo *pRepo);

int tsdbOpenBufPool(STsdbRepo *pRepo) {
  ASSERT(pRepo != NULL && REPO_POOL(pRepo) == NULL);

  STsdbCfg *pCfg = REPO_CFG(pRepo);

  pRepo->pPool = tsdbNewBufPool(pCfg->cacheBlockSize * 1024 * 1024, pCfg->totalBlocks);
  if (pPool == NULL) {
    tsdbError("vgId:%d failed to open TSDB buffer pool since %s", REPO_ID(pRepo), tstrerror(terrno));
    return -1;
  }

  return 0;
}

void tsdbCloseBufPool(STsdbRepo *pRepo) {
  if (pRepo == NULL) return;
  pRepo->pPool = tsdbFreeBufPool(pRepo->pPool);
}

SListNode* tsdbAllocBufBlockFromPool(STsdbRepo* pRepo) {
  ASSERT(pRepo != NULL && REPO_POOL(pRepo) != NULL);
  ASSERT(IS_REPO_LOCKED(pRepo));

  STsdbBufPool * pPool = REPO_POOL(pRepo);
  SListNode *    pNode;
  STsdbBufBlock *pBufBlock;

  while (POOL_IS_EMPTY(pPool)) {
    tsdbWaitPoolNotEmpty(pRepo);
  }

  pNode = tsdbPopBufBlockNodeImpl(pPool);
  ASSERT(pNode != NULL);
  tdListNodeGetData(pPool->bufBlockList, pNode, &pBufBlock);

  tsdbInitBufBlock(pBufBlock, pPool->index++, pPool->bufBlockSize);
  return pNode;
}

static STsdbBufPool *tsdbNewBufPool(int bufBlockSize /*bytes*/, int tBufBlocks) {
  STsdbBufPool *pPool = (STsdbBufPool *)calloc(1, sizeof(*pPool));
  if (pPool == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return NULL;
  }

  pPool->bufBlockSize = bufBlockSize;
  pPool->tBufBlocks = tBufBlocks;

  int code = pthread_cond_init(&(pPool->poolNotEmpty), NULL);
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    tsdbFreeBufPool(pPool);
    return NULL;
  }

  pPool->bufBlockList = tdListNew(sizeof(STsdbBufBlock *));
  if (pPool->bufBlockList == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbFreeBufPool(pPool);
    return NULL;
  }

  for (size_t i = 0; i < tBufBlocks; i++) {
    STsdbBufBlock *pBufBlock = tsdbNewBufBlock(bufBlockSize);
    if (pBufBlock == NULL) {
      tsdbFreeBufPool(pPool);
      return NULL;
    }

    if (tsdbAddBufBlockToPoolImpl(pPool, pBufBlock) < 0) {
      tsdbFreeBufBlock(pBufBlock);
      tsdbFreeBufPool(pPool);
      return NULL;
    }
  }

  return pPool;
}

static STsdbBufPool *tsdbFreeBufPool(STsdbBufPool *pPool) {
  if (pPool) {
    if (pPool->bufBlockList) {
      SListNode *    pNode;
      STsdbBufBlock *pBufBlock;
      while ((pNode = tsdbPopBufBlockNodeImpl(pPool)) != NULL) {
        tdListNodeGetData(pPool->bufBlockList, pNode, (void *)(&pBufBlock));
        tsdbFreeBufBlock(pBufBlock);
        free(pNode);
      }
      
      tdListFree(pPool->bufBlockList);
    }

    pthread_cond_destroy(&pPool->poolNotEmpty);

    free(pPool);
  }

  return NULL;
}

static int tsdbAddBufBlockToPoolImpl(STsdbBufPool *pPool, STsdbBufBlock *pBufBlock) {
  if (tdListAppend(pPool->bufBlockList, (void *)(&pBufBlock)) < 0) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  pPool->nBufBlocks++;
  return 0;
}

static FORCE_INLINE SListNode *tsdbPopBufBlockNodeImpl(STsdbBufPool *pPool) {
  return tdListPopHead(pPool->bufBlockList);
}

static FORCE_INLINE void tsdbWaitPoolNotEmpty(STsdbRepo *pRepo) {
  STsdbBufPool *pPool = REPO_POOL(pRepo);

  REPO_SET_UNLOCKED(pRepo);
  pthread_cond_wait(&(pPool->poolNotEmpty), &(pRepo->mutex));
  REPO_SET_LOCKED(pRepo);
}