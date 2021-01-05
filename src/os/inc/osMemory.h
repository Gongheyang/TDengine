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

#ifndef TDENGINE_OS_MEMORY_H
#define TDENGINE_OS_MEMORY_H

#include "osString.h"
#include <malloc.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  TAOS_ALLOC_MODE_DEFAULT = 0,
  TAOS_ALLOC_MODE_RANDOM_FAIL = 1,
  TAOS_ALLOC_MODE_DETECT_LEAK = 2
} ETaosMemoryAllocMode;

void   taosSetAllocMode(int mode, const char *path, bool autoDump);
void   taosDumpMemoryLeak();

// used in tsdb module
void * taosTMalloc(size_t size);
void * taosTCalloc(size_t nmemb, size_t size);
void * taosTRealloc(void *ptr, size_t size);
void   taosTZfree(void *ptr);
size_t taosTSizeof(void *ptr);
void   taosTMemset(void *ptr, int c);

// used in other module
#define tmalloc(size) malloc(size)
#define tcalloc(num, size) calloc(num, size)
#define trealloc(ptr, size) realloc(ptr, size)
#define tstrdup(str) taosStrdupImp(str)
#define tstrndup(str, size) taosStrndupImp(str, size)
#define tgetline(lineptr, n, stream) taosGetlineImp(lineptr, n, stream)
#define tfree(x)         \
  do {                   \
    if (x) {             \
      free((void *)(x)); \
      x = 0;             \
    }                    \
  } while (0)

#ifdef TAOS_MEM_CHECK
  #ifdef TAOS_MEM_CHECK_TEST
    void *  taosMallocMem(size_t size, const char *file, uint32_t line);
    void *  taosCallocMem(size_t num, size_t size, const char *file, uint32_t line);
    void *  taosReallocMem(void *ptr, size_t size, const char *file, uint32_t line);
    void    taosFreeMem(void *ptr, const char *file, uint32_t line);
    char *  taosStrdupMem(const char *str, const char *file, uint32_t line);
    char *  taosStrndupMem(const char *str, size_t size, const char *file, uint32_t line);
    ssize_t taosGetlineMem(char **lineptr, size_t *n, FILE *stream, const char *file, uint32_t line);
    #undef tmalloc
    #undef tcalloc
    #undef trealloc
    #undef tfree
    #define tmalloc(size) taosMallocMem(size, __FILE__, __LINE__)
    #define tcalloc(num, size) taosCallocMem(num, size, __FILE__, __LINE__)
    #define trealloc(ptr, size) taosReallocMem(ptr, size, __FILE__, __LINE__)
    #define tfree(ptr) taosFreeMem(ptr, __FILE__, __LINE__) 
    
    // #undef tstrdup
    // #undef tstrndup
    // #undef tgetline
    // #define taosStrdup(str) taos_strdup(str, __FILE__, __LINE__)
    // #define taosStrndup(str, size) taos_strndup(str, size, __FILE__, __LINE__)
    // #define tgetline(lineptr, n, stream) taos_getline(lineptr, n, stream, __FILE__, __LINE__)
  #endif  
#endif 


#define TDM_MAX_NUM 1000

extern int32_t tdm_num;
extern long tdm_ps;

void tdmfree(void *p);

void *tdmrealloc(void *p, int s);

void *tdmalloc(char *f, unsigned line, int s,int set);
void tdminit();

#define TDMALLOCED(p) ((*(long *)(((void*)p)-8)==0x1234567887654321L) && (*(long *)(((void *)p)-16)==0x0011223344556677L))
#define TDMSIZE(s) ((s+24)%tdm_ps == 0 ? (s+24)+tdm_ps:((s+24)/tdm_ps+2)*tdm_ps)

#define TDMALLOC(s) tdmalloc(__FILE__, __LINE__, s,0)
#define TDMCALLOC(s,n) tdmalloc(__FILE__, __LINE__, (s)*(n),1)
#define TDMREALLOC(p,s) tdmrealloc(p,s)
#define TDMFREE(p) do { if(p) \
			{tdmfree((void *)p); \
			p = 0; } \
		   } while (0)


#ifdef __cplusplus
}
#endif

#endif
