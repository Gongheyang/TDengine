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

struct SMemTable {
  T_REF_DECLARE()
  SRWLatch     latch;
  TSKEY        keyFirst;
  TSKEY        keyLast;
  int64_t      numOfRows;
  int32_t      maxTables;
  STableData** tData;
  SList*       actList;
  SList*       extraBuffList;
  SList*       bufBlockList;
};
