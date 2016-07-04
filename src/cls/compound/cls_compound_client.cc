#include <errno.h>
#include "include/rados/librados.hpp"
#include "include/rados/librados.h"
#include "librados/IoCtxImpl.h"
#include "librados/RadosClient.h"
#include "cls/lock/cls_lock_client.h"
#include <sstream>


/*
 *
 * parameters: origin_name is used for compact
 * RETURN VALUE: -ENOSPC, -EIO, -EAGAIN or others
 *
 */

extern "C" int cls_compound_append(rados_ioctx_t io, const char * origin_name, int origin_name_len, const char * oid, const char * buf, int buflen, librados::AioCompletion **c)
{
	int ret;
	uint32_t remote_offset;
	bufferlist in, out;

	bufferlist origin_name_bl;
	bufferlist buf_bl;

	origin_name_bl.append(origin_name, origin_name_len);
	buf_bl.append(buf, buflen);

	::encode(origin_name_bl, in);
	::encode(buf_bl, in);

	librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;

	*c = librados::Rados::aio_create_completion();

	if ((ret = ctx->aio_exec(oid, (*c)->pc, "compound", "append", in, &out)) < 0) {
		return ret;
	} else 
		return 0;
}

extern "C" int cls_compound_read(rados_ioctx_t io, const char * oid, int offset, int size, char ** buf, int *buflen)
{
	int ret;

	bufferlist in, out;
	bufferlist file_bl;

	librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;

	::encode(offset, in);
	::encode(size, in);

	if ((ret = ctx->exec(oid, "compound", "read", in, out)) < 0) {
		return ret;
	}


	bufferlist::iterator iter = out.begin();
	try {
		::decode(file_bl, iter);
	} catch(buffer::error &err) {
		return -EIO;
	}

	//*buf = (char*)malloc(file_bl.length());
	//file_bl.copy(0, file_bl.length(), *buf);
	
	/*make bufferlist flatten the ptr first*/
	*buf = out.c_str();
	if (buflen != NULL)
		*buflen = out.length();
}

extern "C" int cls_compound_delete(rados_ioctx_t io, const char * oid, const char * origin_name, int origin_name_len, int offset, int size)
{
	bufferlist in,out;
}
