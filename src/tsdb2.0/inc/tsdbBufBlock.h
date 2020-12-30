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

#ifndef _TD_TSDB_BUF_BLOCK_H_
#define _TD_TSDB_BUF_BLOCK_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  int64_t blockId;
  int     offset;
  int     remain;
  char    data[];
} STsdbBufBlock;

#define BUF_BLOCK_ID(b) ((b)->blockId)
#define BUF_BLOCK_OFFSET(b) ((b)->offset)
#define BUF_BLOCK_REMAIN(b) ((b)->remain)
#define BUF_BLOCK_DATA(b) ((b)->data)
#define BUF_BLOCK_ALLOC_START(b) POINTER_SHIFT(BUF_BLOCK_DATA(b), BUF_BLOCK_OFFSET(b))

STsdbBufBlock *tsdbNewBufBlock(int bufBlockSize);

static FORCE_INLINE void tsdbFreeBufBlock(STsdbBufBlock *pBufBlock) { tfree(pBufBlock); }

static FORCE_INLINE void tsdbInitBufBlock(STsdbBufBlock *pBufBlock, int64_t id, int remain) {
  BUF_BLOCK_ID(pBufBlock) = id;
  BUF_BLOCK_OFFSET(pBufBlock) = 0;
  BUF_BLOCK_REMAIN(pBufBlock) = remain;
}

#ifdef __cplusplus
}
#endif

#endif