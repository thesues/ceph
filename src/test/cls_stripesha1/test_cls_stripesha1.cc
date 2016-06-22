#include <iostream>
#include <errno.h>

#include "include/rados/librados.hpp"
#include "include/rados/librados.h"
#include "include/radosstriper/libradosstriper.h"
#include "include/radosstriper/libradosstriper.hpp"
#include "test/librados/test.h"
#include "gtest/gtest.h"
#include "include/types.h"
#include "cls/stripesha1/cls_stripesha1_client.h"
#include "cls/stripesha1/cls_stripesha1_ops.h"
#include <sstream>
#include <openssl/sha.h>
#include <common/Clock.h>

using namespace librados;
using namespace libradosstriper;


static const char hexchars[] = "0123456789abcdef";


static bufferlist content;
static int throttle = 4;

int main(int argc, char ** argv) {
	/* ./ceph_test_cls_stripesha1 --gtest_filter=cls_stripesha1.getstripesha1_client /home/zhangdongmao/Application-Software-Configuration-Using-Heat.mp4 */
	::testing::InitGoogleTest(&argc, argv);
	if (argc > 1) {
		std::string err;
		int ret = content.read_file((char*)argv[1], &err);
		std::cout << ret << std::endl;
		if (ret < 0) {
			std::cerr << err << std::endl;
			return -1;
		}

		//it has a throttle, default is 4
		if (argc == 3) {
			throttle = atoi(argv[2]);
		}

	} else {
		std::cerr << "must have a file to be uploaded" << std::endl;
		return -1;
	}
	RUN_ALL_TESTS();
}

std::string bin_to_hex(const bufferlist & bl) {
        std::string result;
        char hex[2];
	unsigned char b;
	unsigned int i;
        for (i = 0; i < bl.length(); i++)
        {
            b = bl[i];
            hex[0] = hexchars[(b >> 4) & 0x0F ];
            hex[1] = hexchars[b & 0x0F];
	    result.append(hex,2);
        }
	return result;
}

static void calcuate_striped_sha1sum(bufferlist &input, uint32_t unit, bufferlist &output) {

	uint32_t offset = 0;
	uint32_t size = input.length();
	uint32_t len = 0;
	SHA_CTX c;
	char sha1[SHA_DIGEST_LENGTH];
	for(; offset < size; offset += unit) {
		len = std::min(size - offset, unit);
		SHA1_Init(&c);
		SHA1_Update(&c, input.c_str() + offset, len);
		SHA1_Final((unsigned char*)sha1, &c);
		output.append(sha1, SHA_DIGEST_LENGTH);
	}
}

TEST(cls_stripesha1, getstripesha1) {
	int ret;
	Rados cluster;             
	std::string pool_name = get_temp_pool_name();
	ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
	IoCtx ioctx;                                          
	cluster.ioctx_create(pool_name.c_str(), ioctx);
	bufferlist in, out;
	bufferlist content;
	content.append("HELLO");
	ioctx.write_full("obj", content);

	struct stripe_parameters call;
	call.num = 0;
	call.stripe_unit= 512 << 10; /* 512K */
	call.stripe_count= 4;
	call.object_size = 4 << 20; /* 4M */

	::encode(call, in);

	ret = ioctx.exec("obj", "stripesha1", "get_sha1", in, out);
	if (ret < 0) {
		std::cout << strerror(-ret);
		destroy_one_pool_pp(pool_name, cluster);
		return;
	}

	//decode
	struct stripe_sha1 output;
	bufferlist::iterator iter = out.begin();
	try {
		::decode(output, iter);
	} catch (buffer::error &err) {
		ASSERT_EQ(0, -1);
		return;
	}
	
	const char * output_sha1 = "c65f99f8c5376adadddc46d5cbcf5762f9e55eb7";

	ASSERT_EQ(0, strncmp(output_sha1, bin_to_hex(output.sha1_map[0]).c_str(), 20));
	ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

static std::string  getObjectId(const char * oid, long long unsigned objectno) {
	std::ostringstream s;
	s << oid << '.' << std::setfill('0') << std::setw(16) << std::hex << objectno;
	return s.str();
}

int send_request(IoCtx &ioctx, struct stripe_parameters &call, const std::string &objname, std::map<int, bufferlist> &sha1map) {

	bufferlist in, out;

	::encode(call, in);

	std::cout << "remote object name:" << getObjectId(objname.c_str(), call.num) << std::endl;

	int ret = ioctx.exec(getObjectId(objname.c_str(), call.num), "stripesha1", "get_sha1", in, out);
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

TEST(cls_stripesha1, writestripe_getstripe) {

	Rados cluster;             
	std::string pool_name = get_temp_pool_name();
	ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
	IoCtx ioctx;                                          
	cluster.ioctx_create(pool_name.c_str(), ioctx);

	bufferlist in, out;


	int settings_stripe_unit = 512 << 10;
	int settings_stripe_count = 4;
	int settings_object_size = 16 << 20;

	bufferlist sha1_output;
	//caculate local file's sha1sum
	calcuate_striped_sha1sum(content, settings_stripe_unit, sha1_output);

	RadosStriper striper;
	RadosStriper::striper_create(ioctx, &striper);
	striper.set_object_layout_stripe_unit(settings_stripe_unit);
	striper.set_object_layout_stripe_count(settings_stripe_count);
	striper.set_object_layout_object_size(settings_object_size);


	/*
	int to_be_checked_sector = 3;
	char sha1[SHA_DIGEST_LENGTH];
	SHA1_Init(&c);
	SHA1_Update(&c, content.c_str() + 3 * settings_stripe_unit, settings_stripe_unit);
	SHA1_Final(sha1, &c);
	std::string expected_sha1sum = sha1;
	*/

	std::string objname = "obj";

	striper.write_full(objname, content);

	uint64_t nb_objects = 0;
	uint64_t size = content.length();
	uint64_t stripe_count = settings_stripe_count;
	uint64_t object_size = settings_object_size;
	uint64_t su = settings_stripe_unit;


	//copy from libradosstriper/RadosStriperImpl.cc
	uint64_t nb_complete_sets = size / (object_size*stripe_count);
	uint64_t remaining_data = size % (object_size*stripe_count);
	uint64_t remaining_stripe_units = (remaining_data + su -1) / su;
	uint64_t remaining_objects = std::min(remaining_stripe_units, stripe_count);
	nb_objects = nb_complete_sets * stripe_count + remaining_objects;

	struct stripe_parameters call;
	call.num = 0;
	call.stripe_unit= settings_stripe_unit;
	call.stripe_count= settings_stripe_count;
	call.object_size = settings_object_size;

	std::cout <<  "nb_objects :" << nb_objects << std::endl;

	utime_t now = ceph_clock_now(NULL);
	std::map<int, bufferlist> sha1map;
	for(int i = 0; i < nb_objects; i ++) {
		if (send_request(ioctx, call, objname, sha1map) < 0) {
			cluster.pool_delete(pool_name.c_str());
			return;
		}
		call.num += 1;
	}
	std::cout << "escapsed time:" << ceph_clock_now(NULL) - now << std::endl;

	std::map<int, bufferlist>::iterator iter = sha1map.begin();

	for(;iter != sha1map.end(); ++ iter) {
		std::cout << iter->first << ":" << bin_to_hex(iter->second) << std::endl;
	}
	cluster.pool_delete(pool_name.c_str());
}



//test C API
TEST(cls_stripesha1, getstripesha1_client) {


	int ret;
	rados_t cluster;
	std::string pool_name = get_temp_pool_name();
	create_one_pool(pool_name, &cluster);
	rados_ioctx_t ioctx;
	rados_striper_t striper; 
	rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);


	int settings_stripe_unit = 512 << 10;
	int settings_stripe_count = 8;
	int settings_object_size = 4 << 20;

	rados_striper_create(ioctx, &striper);

	rados_striper_set_object_layout_stripe_unit(striper, settings_stripe_unit);
	rados_striper_set_object_layout_object_size(striper, settings_object_size);
	rados_striper_set_object_layout_stripe_count(striper, settings_stripe_count);


	bufferlist sha1_output;
	//caculate local file's sha1sum
	calcuate_striped_sha1sum(content, settings_stripe_unit, sha1_output);
	

	rados_striper_write(striper, "obj", content.c_str(), content.length(), 0);

	char * buf;
	int buflen;
	uint64_t piece_length;

	utime_t now = ceph_clock_now(NULL);
	ret = cls_client_stripesha1_get(ioctx, "obj", throttle,  &buf, &buflen, &piece_length, NULL);
	std::cout << "escapsed time:" << ceph_clock_now(NULL) - now << std::endl;

	if (ret < 0) {
		std::cout << strerror(-ret) << std::endl;
		rados_ioctx_destroy(ioctx);
		destroy_one_pool(pool_name, &cluster);
		return;
	}

	bufferlist to_be_converted;
	std::cout << "buflen:" << buflen << std::endl;
	std::cout << "piece_length:" << piece_length << std::endl;

	for(int i = 0; i < buflen; i += 20) {
		to_be_converted.clear();
		to_be_converted.append(buf + i , 20);
		std::cout << "sha1:" << bin_to_hex(to_be_converted) << std::endl;
	}

	ASSERT_EQ(buflen, sha1_output.length());
	ASSERT_EQ(0, memcmp(sha1_output.c_str(), buf, buflen));

	/* */
	{
	SHA_CTX c;
	char sha1[SHA_DIGEST_LENGTH];
	SHA1_Init(&c);
	SHA1_Update(&c, buf, buflen);
	SHA1_Final((unsigned char*)sha1, &c);

	bufferlist hashbl;
	hashbl.append(sha1, SHA_DIGEST_LENGTH);
	std::cout << "hashinfo:" << bin_to_hex(hashbl) << std::endl;
	}

	free(buf);
	rados_ioctx_destroy(ioctx);
	destroy_one_pool(pool_name, &cluster);
}

