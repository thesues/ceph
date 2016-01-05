#include <errno.h>

#include "include/types.h"
#include "cls/log/cls_log_ops.h"
#include "include/rados/librados.hpp"
#include "include/compat.h"
#include "common/Formatter.h"


using namespace librados;



void cls_log_add(librados::ObjectWriteOperation& op, list<cls_log_entry>& entries)
{
  bufferlist in;
  cls_log_add_op call;
  call.entries = entries;
  ::encode(call, in);
  op.exec("log", "add", in);
}

void cls_log_add(librados::ObjectWriteOperation& op, cls_log_entry& entry)
{
  bufferlist in;
  cls_log_add_op call;
  call.entries.push_back(entry);
  ::encode(call, in);
  op.exec("log", "add", in);
}

void cls_log_add_prepare_entry(cls_log_entry& entry, const utime_t& timestamp,
                 const string& section, const string& name, bufferlist& bl)
{
  entry.timestamp = timestamp;
  entry.section = section;
  entry.name = name;
  entry.data = bl;
}

void cls_log_add(librados::ObjectWriteOperation& op, const utime_t& timestamp,
                 const string& section, const string& name, bufferlist& bl)
{
  cls_log_entry entry;

  cls_log_add_prepare_entry(entry, timestamp, section, name, bl);
  cls_log_add(op, entry);
}

void cls_log_trim(librados::ObjectWriteOperation& op, const utime_t& from_time, const utime_t& to_time,
                  const string& from_marker, const string& to_marker)
{
  bufferlist in;
  cls_log_trim_op call;
  call.from_time = from_time;
  call.to_time = to_time;
  call.from_marker = from_marker;
  call.to_marker = to_marker;
  ::encode(call, in);
  op.exec("log", "trim", in);
}

int cls_log_trim(librados::IoCtx& io_ctx, const string& oid, const utime_t& from_time, const utime_t& to_time,
                 const string& from_marker, const string& to_marker)
{
  bool done = false;

  do {
    ObjectWriteOperation op;

    cls_log_trim(op, from_time, to_time, from_marker, to_marker);

    int r = io_ctx.operate(oid, &op);
    if (r == -ENODATA)
      done = true;
    else if (r < 0)
      return r;

  } while (!done);


  return 0;
}

class LogListCtx : public ObjectOperationCompletion {
  list<cls_log_entry> *entries;
  string *marker;
  bool *truncated;
public:
  LogListCtx(list<cls_log_entry> *_entries, string *_marker, bool *_truncated) :
                                      entries(_entries), marker(_marker), truncated(_truncated) {}
  void handle_completion(int r, bufferlist& outbl) {
    if (r >= 0) {
      cls_log_list_ret ret;
      try {
        bufferlist::iterator iter = outbl.begin();
        ::decode(ret, iter);
        if (entries)
	  *entries = ret.entries;
        if (truncated)
          *truncated = ret.truncated;
        if (marker)
          *marker = ret.marker;
      } catch (buffer::error& err) {
        // nothing we can do about it atm
      }
    }
  }
};

void cls_log_list(librados::ObjectReadOperation& op, utime_t& from, utime_t& to,
                  const string& in_marker, int max_entries,
		  list<cls_log_entry>& entries,
                  string *out_marker, bool *truncated)
{
  bufferlist inbl;
  cls_log_list_op call;
  call.from_time = from;
  call.to_time = to;
  call.marker = in_marker;
  call.max_entries = max_entries;

  ::encode(call, inbl);

  op.exec("log", "list", inbl, new LogListCtx(&entries, out_marker, truncated));
}

class LogInfoCtx : public ObjectOperationCompletion {
  cls_log_header *header;
public:
  LogInfoCtx(cls_log_header *_header) : header(_header) {}
  void handle_completion(int r, bufferlist& outbl) {
    if (r >= 0) {
      cls_log_info_ret ret;
      try {
        bufferlist::iterator iter = outbl.begin();
        ::decode(ret, iter);
        if (header)
	  *header = ret.header;
      } catch (buffer::error& err) {
        // nothing we can do about it atm
      }
    }
  }
};

void cls_log_info(librados::ObjectReadOperation& op, cls_log_header *header)
{
  bufferlist inbl;
  cls_log_info_op call;

  ::encode(call, inbl);

  op.exec("log", "info", inbl, new LogInfoCtx(header));
}

extern "C" void c_cls_log_add(rados_ioctx_t io, const char * oid, time_t timestamp, const char * section, const char * name) {
    librados::IoCtx ctx;
    librados::IoCtx::from_rados_ioctx_t(io, ctx);
    librados::ObjectWriteOperation op; 
    bufferlist bl; 
    //bl.append(name, strlen(name));
    //section and name is enough
    utime_t now(timestamp,0);
    cls_log_add(op, now, section, name, bl);
    string soid(oid);
    ctx.operate(soid, &op);
}

extern "C" void c_cls_log_list(rados_ioctx_t io, const char *oid, time_t from, time_t to, const char * in_marker, char ** out_marker,int max_entries, bool *truncated, char ** p_buf) {
    librados::IoCtx ctx;
    librados::IoCtx::from_rados_ioctx_t(io, ctx);
    librados::ObjectReadOperation rop; 
    string s_in_marker(in_marker);
    string s_out_marker;
    list<cls_log_entry> entries;
    utime_t from_time(from, 0);
    utime_t to_time(to, 0);
    cls_log_list(rop, from_time, to_time, s_in_marker, max_entries, entries, &s_out_marker, truncated);

    ctx.operate(oid, &rop, NULL);

    JSONFormatter f(false);

    ostringstream oss;

    //dump data to json
    f.open_array_section("entries");
    for (list<cls_log_entry>::iterator iter = entries.begin(); iter != entries.end(); ++iter) {
      cls_log_entry& entry = *iter;
      f.open_object_section("entry");
      f.dump_string("id", entry.id);
      f.dump_string("section", entry.section);
      f.dump_string("name", entry.name);
      f.close_section();
    }
    f.close_section();

    f.flush(oss);
    std::string s = oss.str();
    *p_buf = (char*)malloc(s.length());
    strcpy(*p_buf, s.c_str());
    *out_marker = (char*)malloc(s_out_marker.length());
    strcpy(*out_marker, s_out_marker.c_str());
}

