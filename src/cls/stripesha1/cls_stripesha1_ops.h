#ifndef CLS_STRIPESHA1_H
#define CLS_STRIPESHA1_H

#include <map>
#include "include/types.h"
#include <string.h>
#include <errno.h>

/* input */
struct stripe_parameters
{
	uint32_t num;
	uint32_t stripe_unit;
	uint32_t object_size;
	uint32_t stripe_count;

	stripe_parameters(){}

	void encode(bufferlist &bl) const {
		ENCODE_START(1,1,bl)
		::encode(num, bl);
		::encode(stripe_unit, bl);
		::encode(object_size, bl);
		::encode(stripe_count,bl);
		ENCODE_FINISH(bl);
	}

	void decode(bufferlist::iterator &bl) {
		DECODE_START(1,bl)
		::decode(num, bl);
		::decode(stripe_unit, bl);
		::decode(object_size, bl);
		::decode(stripe_count,bl);
		DECODE_FINISH(bl);
	}
};
WRITE_CLASS_ENCODER(stripe_parameters)


/* output */
struct stripe_sha1
{
	std::map<int, bufferlist> sha1_map;
	void encode(bufferlist &bl) const {
		ENCODE_START(1,1,bl)
		::encode(sha1_map, bl);
		ENCODE_FINISH(bl);
	}
	void decode(bufferlist::iterator &bl) {
		DECODE_START(1,bl)
		::decode(sha1_map, bl);
		DECODE_FINISH(bl);

	}
};
WRITE_CLASS_ENCODER(stripe_sha1)
#endif


