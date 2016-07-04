#ifndef CLS_COMPOUND_CLIENT_H
#define CLS_COMPOUND_CLIENT_H

#include "include/rados/librados.h"


#ifdef __cplusplus
extern "C" {
#endif

int cls_compound_append(rados_ioctx_t io, const char * origin_name, int origin_name_len, const char * oid, const char * buf, int buflen, librados::AioCompletion ** p);
int cls_compound_read(rados_ioctx_t io, const char * oid, int offset, int size, char ** buf, int *buflen);
int cls_compound_delete(rados_ioctx_t io, const char * oid, const char * origin_name, int origin_name_len, int offset, int size);

#ifdef __cplusplus 
}
#endif


#endif

