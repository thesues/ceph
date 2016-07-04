#include <iostream>
#include <string.h>

#include "objclass/objclass.h"
#include "cls/stripesha1/cls_stripesha1_ops.h"
#include <openssl/sha.h>



CLS_VER(1,0)
CLS_NAME(stripesha1)
cls_handle_t h_class;
cls_method_handle_t h_stripesha1_get;

static int cls_stripesha1_get(cls_method_context_t hctx, bufferlist *in, bufferlist *out) {
	// decode stripe_parameters
	struct stripe_parameters op;
	struct stripe_sha1 output;
	size_t size;
	bufferlist::iterator iter = in->begin();
	try {
		::decode(op, iter);
	} catch (buffer::error & err) {
		CLS_LOG(1, "ERROR: cls_stripesha1_get(): failed to decode stripe_parameters");
		return -EINVAL;
	}
	//check op.stripe_unit is 2^n;
	if (op.stripe_unit & (op.stripe_unit - 1)) {
		CLS_LOG(1, "ERROR, stripe_unit must be powerof2");
		return -EINVAL;
	}

	if (cls_cxx_stat(hctx, &size, NULL) < 0) {
		return -ENOENT;
	}


	if (op.stripe_unit <= 0 || op.stripe_count <=0) {
		return -EINVAL;
	}

	bufferlist data;

	int num_stripe_per_active_set = (op.object_size / op.stripe_unit ) * op.stripe_count;
	int start = op.num / op.stripe_count * num_stripe_per_active_set;

	SHA_CTX c;

	/* SHA_DIGEST_LENGTH must be 20 according to torrent */
	char sha1[SHA_DIGEST_LENGTH];

	size_t offset = 0;
	int i = 0;
	int j = 0;
	int len;
	bufferlist bl;

	if (cls_cxx_read(hctx, 0 , op.object_size, &data) < 0)
		return -EINVAL;

	for (; offset < size; i ++) {
		j = start + (op.num % op.stripe_count) + i * op.stripe_count;


		len = std::min(size - offset, static_cast<size_t>(op.stripe_unit));
		SHA1_Init(&c);
		SHA1_Update(&c, data.c_str() + offset, len);
		SHA1_Final((unsigned char*)sha1, &c);

		bl.clear();
		bl.append(sha1, SHA_DIGEST_LENGTH);

		/* put sha1 to map */
		output.sha1_map[j] = bl;
		offset += op.stripe_unit;
	}

	::encode(output, *out);
	CLS_LOG(10, "returned map size %d", out->length());
	return 0;
}

void __cls_init()
{
	CLS_LOG(1, "Loaded stripsha1 class");
	cls_register("stripesha1", &h_class);
	cls_register_cxx_method(h_class, "get_sha1", CLS_METHOD_RD, cls_stripesha1_get, &h_stripesha1_get);
}

