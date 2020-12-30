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

#define TSDB_DATA_SKIPLIST_MAX_LEVEL 5

STableData *tsdbNewTableData(uint64_t uid, bool update) {
  STableData *pTableData = (STableData *)malloc(1, sizeof(*pTableData));
  if (pTableData == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return NULL;
  }

  pTableData->uid = uid;
  pTableData->keyFirst = INT64_MAX;
  pTableData->keyLast = INT64_MIN;
  pTableData->numOfRows = 0;

  pTableData->pData =
      tSkipListCreate(TSDB_DATA_SKIPLIST_MAX_LEVEL, TSDB_DATA_TYPE_TIMESTAMP, TYPE_BYTES[TSDB_DATA_TYPE_TIMESTAMP],
                      tkeyComparFn, update ? SL_UPDATE_DUP_KEY : SL_DISCARD_DUP_KEY, tsdbGetTsTupleKey);
  if (pTableData->pData == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbFreeTableData(pTableData);
    return NULL;
  }

  return pTableData;
}

STableData *tsdbFreeTableData(STableData *pTableData) {
  if (pTableData) {
    tSkipListDestroy(pTableData->pData);
    tfree(pTableData);
  }

  return NULL
}

static char *tsdbGetTsTupleKey(const void *data) { return dataRowTuple((SDataRow)data); }