#include <errno.h>
#include "cls/stripesha1/cls_stripesha1_ops.h"
#include "include/rados/librados.hpp"
#include "include/rados/librados.h"
#include "librados/IoCtxImpl.h"
#include "librados/RadosClient.h"
#include <sstream>

//this is C API for gobinding
//
//
static std::string  getObjectId(const char * oid, long long unsigned objectno) {
	std::ostringstream s;
	s << oid << '.' << std::setfill('0') << std::setw(16) << std::hex << objectno;
	return s.str();
} 

//send struct stripe_parameters to cls and merge output into sha1map
static int send_cls_stripe_request(librados::IoCtxImpl *ctx, struct stripe_parameters &call, const std::string &objname, std::map<int, bufferlist> &sha1map) {

	bufferlist in, out;

	::encode(call, in);

	int ret = ctx->exec(getObjectId(objname.c_str(), call.num), "stripesha1", "get_sha1", in, out);
	if (ret < 0) {
		return ret;
	}

	struct stripe_sha1 output;
	bufferlist::iterator iter = out.begin();
	try {
		::decode(output, iter);
	} catch (buffer::error &err) {
		return -EIO;
	}
	sha1map.insert(output.sha1_map.begin(), output.sha1_map.end());
	return ret;
}

extern "C" int cls_client_stripesha1_get(rados_ioctx_t io, const char * oid, char ** buf, int * buflen, uint64_t *piece_length, uint64_t *length)
{

	int ret;
	librados::IoCtxImpl *ctx = (librados::IoCtxImpl *) io;
	string first_object_name = getObjectId(oid, 0);


	bufferlist bl;
	if ((ret = ctx->getxattr(first_object_name, "striper.layout.stripe_unit", bl)) < 0){
		return ret;
	}
	std::string s_xattr(bl.c_str(), bl.length());
	uint64_t stripe_unit = strtoull(s_xattr.c_str(), NULL, 10);


	bl.clear();
	if ((ret=ctx->getxattr(first_object_name, "striper.layout.stripe_count", bl)) < 0) 
		return ret;
	s_xattr = std::string(bl.c_str(), bl.length());
	uint64_t stripe_count = strtoull(s_xattr.c_str(), NULL, 10);


	bl.clear();
	if ((ret = ctx->getxattr(first_object_name, "striper.layout.object_size", bl)) < 0)
		return ret;
	s_xattr = std::string(bl.c_str(), bl.length());
	uint64_t object_size = strtoull(s_xattr.c_str(), NULL, 10);

	bl.clear();
	if ((ret = ctx->getxattr(first_object_name, "striper.size", bl)) < 0)
		return ret;
	s_xattr = std::string(bl.c_str(), bl.length());
	uint64_t size = strtoull(s_xattr.c_str(), NULL, 10);


	//copy from libradosstriper/RadosStriperImpl.cc
	//the following is to calculate nb_objects.
	uint64_t su = stripe_unit;
	uint64_t nb_complete_sets = size / (object_size*stripe_count);
	uint64_t remaining_data = size % (object_size*stripe_count);
	uint64_t remaining_stripe_units = (remaining_data + su -1) / su;
	uint64_t remaining_objects = std::min(remaining_stripe_units, stripe_count);
	uint64_t nb_objects = nb_complete_sets * stripe_count + remaining_objects;

	//prepare cls input 
	struct stripe_parameters call;
	call.stripe_unit= stripe_unit;
	call.stripe_count= stripe_count;
	call.object_size = object_size;
	call.num = 0;

	std::map<int, bufferlist> sha1map;
	for(unsigned int i = 0; i < nb_objects; i ++) {
		call.num = i;
		if ((ret = send_cls_stripe_request(ctx, call, oid, sha1map)) < 0) {
			return ret;
		}
	}

	//put all bufferlist into one, and check every stripe_unit
	std::map<int, bufferlist>::iterator iter = sha1map.begin();
	bufferlist total_bufferlist;
	for(int i = 0; iter != sha1map.end() ; ++ iter, ++ i) {
		if (iter->first == i) {
			total_bufferlist.append(iter->second);
		} else {
			return -EINVAL;
		}
	}

	*buf = (char*)malloc(total_bufferlist.length());
	total_bufferlist.copy(0, total_bufferlist.length(), *buf);
	if (buflen != NULL)
		*buflen = total_bufferlist.length();
	if (piece_length != NULL)
		*piece_length = stripe_unit;
	if (length != NULL)
		*length = size;

	return 0;
}

