#include <iostream>
#include <errno.h>

#include "include/rados/librados.hpp"
#include "include/rados/librados.h"
#include "test/librados/test.h"
#include "gtest/gtest.h"
#include "include/types.h"
#include "cls/compound/cls_compound_client.h"
#include "librados/IoCtxImpl.h"
#include <sstream>
#include <common/Clock.h>
#include <boost/functional/hash.hpp>
#include "common/obj_bencher.h"
#include "common/strtol.h"


int con_current = 16;
int size = 4096;
int seconds = 10;
int distribute = 10;

int main(int argc, char ** argv) {
	/* ./ceph_test_cls_stripesha1 --gtest_filter=cls_stripesha1.getstripesha1_client /home/zhangdongmao/Application-Software-Configuration-Using-Heat.mp4 */
	::testing::InitGoogleTest(&argc, argv);
	std::string err;
	if (argc == 5) {

		con_current = strict_strtol(argv[1], 10, &err);
		if (!err.empty()) 
		return -1;
		size = strict_strtol(argv[2],10, &err);
		if (!err.empty()) 
		return -1;
		seconds = strict_strtol(argv[3],10, &err);
		if (!err.empty()) 
		return -1;
		distribute = strict_strtol(argv[4],10, &err);
		std::cout << "distribute to " << distribute << "objects" << std::endl;
		if (!err.empty()) 
		return -1;
	} else {
		std::cout << "usage:con_current, size, seconds distribute" << std::endl;
		return -1;
	}

	RUN_ALL_TESTS();
}

using namespace librados;


class FuckBencher : public ObjBencher {
  librados::AioCompletion **completions;
  librados::Rados& rados;
  librados::IoCtx& io_ctx;
  librados::NObjectIterator oi;
  bool iterator_valid;
  int max_size;
  boost::hash<std::string> m_hash;


protected:
  int completions_init(int concurrentios) {
    completions = new librados::AioCompletion *[concurrentios];
    return 0;
  }
  void completions_done() {
    delete[] completions;
    completions = NULL;
  }
  int create_completion(int slot, void (*cb)(void *, void*), void *arg) {
    completions[slot] = rados.aio_create_completion((void *) arg, 0, cb);

    if (!completions[slot])
      return -EINVAL;

    return 0;
  }
  void release_completion(int slot) {
    completions[slot]->release();
    completions[slot] = 0;
  }

  int aio_read(const std::string& oid, int slot, bufferlist *pbl, size_t len) {
	std::ostringstream oss;
	oss << "fuck" << m_hash(oid) % max_size;

	bufferlist in;
	::encode(oid,in);
	return io_ctx.aio_exec(oss.str(), completions[slot], "compound", "read", in, pbl);
  }

  int aio_write(const std::string& oid, int slot, bufferlist& bl, size_t len) {

	std::ostringstream oss;
	oss << "fuck" << m_hash(oid) % max_size;

	bufferlist in;

	bufferlist origin_name_bl;

	origin_name_bl.append(oid.c_str(), oid.length());

	::encode(origin_name_bl, in);
	::encode(bl, in);

	return io_ctx.aio_exec(oss.str(), completions[slot], "compound", "append", in, NULL);
 }

  int aio_remove(const std::string& oid, int slot) {
	  return 0;
  }

  int sync_read(const std::string& oid, bufferlist& bl, size_t len) {
    return io_ctx.read(oid, bl, len, 0);
  }
  int sync_write(const std::string& oid, bufferlist& bl, size_t len) {
    return io_ctx.write_full(oid, bl);
  }

  int sync_remove(const std::string& oid) {
	  return 0;
  }

  bool completion_is_done(int slot) {
    return completions[slot]->is_safe();
  }

  int completion_wait(int slot) {
    return completions[slot]->wait_for_safe_and_cb();
  }
  int completion_ret(int slot) {
    return completions[slot]->get_return_value();
  }

  bool get_objects(std::list<Object>* objects, int num) {
    int count = 0;

    if (!iterator_valid) {
      oi = io_ctx.nobjects_begin();
      iterator_valid = true;
    }

    librados::NObjectIterator ei = io_ctx.nobjects_end();

    if (oi == ei) {
      iterator_valid = false;
      return false;
    }

    objects->clear();
    for ( ; oi != ei && count < num; ++oi) {
      Object obj(oi->get_oid(), oi->get_nspace());
      objects->push_back(obj);
      ++count;
    }

    return true;
  }

  void set_namespace( const std::string& ns) {
    io_ctx.set_namespace(ns);
  }

public:
  FuckBencher(CephContext *cct_, librados::Rados& _r, librados::IoCtx& _i)
    : ObjBencher(cct_), completions(NULL), rados(_r), io_ctx(_i) {max_size=distribute;}
  ~FuckBencher() { }
};


std::string random_string(int len) {
  string ret;
  string alphanum = "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";
  for (int i = 0; i < len; ++i) {
    ret.push_back(alphanum[rand() % (alphanum.size() - 1)]);
  }

  return ret;
}

TEST(cls_compound, bench) {
	Rados rados;
	IoCtx io_ctx;
	rados.init("admin");
	rados.conf_read_file("/home/zhangdongmao/upstream/ceph/src/ceph.conf");
	int ret = rados.connect();
	if (ret < 0) {
		std::cout << strerror(-ret);
		return;
	}
	ret = rados.ioctx_create("fuck", io_ctx);
	if (ret < 0) {
		std::cout << strerror(-ret);
		return;
	}

	FuckBencher bencher(NULL, rados, io_ctx);
        ret = bencher.aio_bench(OP_WRITE, seconds, con_current, size, false, "");
}

TEST(cls_compound, benchread) {
	Rados rados;
	IoCtx io_ctx;
	rados.init("admin");
	rados.conf_read_file("/home/zhangdongmao/upstream/ceph/src/ceph.conf");
	int ret = rados.connect();
	if (ret < 0) {
		std::cout << strerror(-ret);
		return;
	}
	ret = rados.ioctx_create("fuck", io_ctx);
	if (ret < 0) {
		std::cout << strerror(-ret);
		return;
	}

	FuckBencher bencher(NULL, rados, io_ctx);
        ret = bencher.aio_bench(OP_SEQ_READ, seconds, con_current, size, false, "");

}

TEST(cls_compound, append) {

	int ret;
	rados_t cluster;
	rados_create(&cluster,"admin");
	rados_conf_read_file(cluster, "/home/zhangdongmao/upstream/ceph/src/ceph.conf");
	rados_connect(cluster);
	std::string pool_name = "fuck";
	//create_one_pool(pool_name, &cluster);
	rados_ioctx_t ioctx;
	ret = rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);
	if (ret<0) {
	        printf("%s", strerror(-ret));
		return;
	}


       std::cout << "poolname " << pool_name << std::endl;
       int i = 0;


       std::string name;

       utime_t now = ceph_clock_now(NULL);

       librados::AioCompletion *carray[1000];
       for(;i<1000;i++) {
	       name = random_string(10);
	       name.push_back(0);
	       name.push_back('e');
	       ret = cls_compound_append(ioctx, name.c_str(), name.length(), "oid", name.c_str(), name.length(), &(carray[i]));
	       std::cout << carray[i] << std::endl;
	       if (ret < 0) {
		       printf("%s", strerror(-ret));
	       }
       }

       /*
       while( i < 1000 && carray[i]->wait_for_complete() == 0) {
	       carray[i]->release();
	       i++;
       }
       */

       carray[999]->wait_for_complete();
       std::cout << "escapsed time:" << ceph_clock_now(NULL) - now << std::endl;


       bufferlist in, out;
       //read the last one
       ///
       ::encode(name, in);
       librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)ioctx;
       ret = ctx->exec("oid", "compound", "read", in, out);
       if (ret < 0) {
	       printf("%s", strerror(-ret));
	       return;
       } 
       std::cout << "read result " << out << std::endl;
       printf("%s\n", out.c_str());


       rados_ioctx_destroy(ioctx);
       destroy_one_pool(pool_name, &cluster);
}
