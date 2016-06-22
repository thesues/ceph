#ifndef CLS_STRIPESHA1_CLIENT_H
#define CLS_STRIPESHA1_CLIENT_H

#include "include/rados/librados.h"


#ifdef __cplusplus
extern "C" {
#endif

int cls_client_stripesha1_get(rados_ioctx_t io, const char * oid, unsigned int throttle, char ** buf, int * buflen, uint64_t *piece_length, uint64_t * length);

#ifdef __cplusplus 
}
#endif


#endif
