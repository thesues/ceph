#include <iostream>
#include <string.h>

#include "objclass/objclass.h"

CLS_VER(1,0)
CLS_NAME(compound)
cls_handle_t h_class;
cls_method_handle_t h_compound_append;
cls_method_handle_t h_compound_read;
cls_method_handle_t h_compound_delete;



#define LOCK_COMPOUND "compound"
#define ALIGN_SIZE    4 << 10 //4k
#define MAX_SIZE      512 << 20  //512M

/*copied from linux kernel */
#define __round_mask(x, y) ((__typeof__(x))((y)-1))
#define round_up(x, y) ((((x)-1) | __round_mask(x, y))+1)
#define round_down(x, y) ((x) & ~__round_mask(x, y))
/*
 * Copied from cls/lock, 
 * if it has any lock, return positive value
 * No lock:return 0
 * other error: return nagative value
 * the block is no more than 4G
 */
static int read_lock(cls_method_context_t hctx, const string& name)
{
	bufferlist bl;
	string key = "lock.";
	key.append(name);

	int r = cls_cxx_getxattr(hctx, key.c_str(), &bl);
	if (r < 0) {
		if (r ==  -ENODATA || r == -ENOENT) {
			return 0;
		} else {
			CLS_ERR("error reading xattr %s: %d", key.c_str(), r);
			return r;
		}
	}
	return r ;
}

static int cls_compound_append(cls_method_context_t hctx, bufferlist *in, bufferlist *out){

	bufferlist origin_name;
	bufferlist buf;
	
	//local check lock
	bufferlist::iterator it = in->begin();
	try {
		::decode(origin_name, it);
		::decode(buf,it);
	} catch(buffer::error & err) {
               CLS_LOG(0, "ERROR: cls_compound_append, invalid parameters");
               return -EINVAL;
	}

	int lock_status = read_lock(hctx, LOCK_COMPOUND);
	if (lock_status > 0) {
		return -EAGAIN;
	} else if (lock_status < 0) {
		return lock_status;
	}
	//now we have lock_status == 0, this is good
	//
	
	//read current size
	uint32_t wr_offset = 0;
	int ret;
	if ((ret = cls_cxx_stat(hctx, (uint64_t*)&wr_offset, NULL)) == -ENOENT) {
		//TODO
		//pre allocate the space
		wr_offset = 0;
	} else if (ret < 0) {
                CLS_ERR("ERROR: cls_compound_append, cls_cxx_stat err");
		return ret;
	}

	//rounded up based on 4K
	//
	wr_offset = round_up(wr_offset, ALIGN_SIZE);

	if (wr_offset > MAX_SIZE) {
                CLS_ERR("ERROR: cls_compound_append, size is not big enough");
		return -ENOSPC;
	}
	//append data
	if ((ret = cls_cxx_write(hctx, wr_offset, buf.length(), &buf)) < 0) {
                CLS_ERR("ERROR: cls_compound_append:cls_cxx write failed");
		return ret;
	}

	//write omap files, store {origin_name=>{offset,size}}
	bufferlist alloclist_bl;
	::encode(wr_offset, alloclist_bl);
	::encode(buf.length(), alloclist_bl);

	if((ret = cls_cxx_map_set_val(hctx, std::string(origin_name.c_str(), origin_name.length()), &alloclist_bl)) < 0)
		return ret;

	CLS_LOG(20, "%s wrote to offset %" PRIu32 ", length %d", origin_name.c_str(), wr_offset, buf.length());
	return 0;
}

static int cls_compound_read(cls_method_context_t hctx, bufferlist *in, bufferlist *out){

	std::string origin_name;

	bufferlist::iterator it = in->begin();
	try {
		::decode(origin_name,it);
	} catch(buffer::error & err) {
               CLS_LOG(0, "ERROR: cls_compound_read, invalid parameters");
               return -EINVAL;
	}


	bufferlist metaout;
	int ret = cls_cxx_map_get_val(hctx, origin_name, &metaout);
	if (ret < 0) {
		return -ENOENT;
	}

	uint32_t offset;
	uint32_t size;

	it = metaout.begin();
	try {
		::decode(offset, it);
		::decode(size, it);
	} catch(buffer::error & err) {
               CLS_LOG(0, "ERROR: cls_compound_read, invalid parameters");
               return -ENODATA;
	}

	cls_cxx_read(hctx, offset, size, out);

	return 0;
} 

static int cls_compound_delete(cls_method_context_t hctx, bufferlist *in, bufferlist *out){
	return 0;

}

static int cls_compound_lock(cls_method_context_t hctx, bufferlist *in, bufferlist *out){
	return 0;
}

void __cls_init() 
{
	CLS_LOG(1, "Loaded compound class");
	cls_register("compound", &h_class);
	cls_register_cxx_method(h_class, "append", CLS_METHOD_RD|CLS_METHOD_WR, cls_compound_append, &h_compound_append);
	cls_register_cxx_method(h_class, "read", CLS_METHOD_RD, cls_compound_read, &h_compound_read);
	cls_register_cxx_method(h_class, "delete", CLS_METHOD_WR|CLS_METHOD_RD, cls_compound_delete, &h_compound_delete);
	//cls_register_cxx_method(h_class, "lock", CLS_METHOD_WR|CLS_METHOD_RD, cls_compound_lock);
}
