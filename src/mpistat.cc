// standard headers
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <cerrno>
#include <climits>
#include <vector>
#include <string>
#include <iostream>
#include <fstream>
#include <algorithm>
#include <cstdint>
#include <unordered_map>
#include <memory>
#include <cmath>
#include <ctime>

// boost headers
#include <boost/iostreams/filter/gzip.hpp>                                  
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/device/file.hpp>
#include <boost/asio/ip/host_name.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>

// external library headers
#include <libcircle.h>
#include <mpi.h>

// linux system headers
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>

// protobuf headers
#include <google/protobuf/util/delimited_message_util.h>

// local headers
#include "FileSuffix.hpp"
#include "mpistat.proto3.pb.h"

// shorthand for boost::iostreams
namespace io = boost::iostreams;

// globals
int rank;
char item_buf[PATH_MAX]; // place to hold items popped off the queue
int argc;
char **argv;
uint64_t digits;
std::unordered_map<char, std::shared_ptr<io::filtering_ostream> > out_map;
uint64_t linecount;
char out_file_format[1000];
MpistatMsg msg; // protobuf message

// calculate the depth of a path
uint64_t depth(char *path) {
  uint64_t count = 0;
  while (*path != '\0') {
    if (*path == '/') {
      ++count;
    }
    ++path;
  }
  return count;
}


// output files...
// want to create a seperate worker output file for each inode type
// get the output file associated with an inode type
// create it if it doesn't exist
std::shared_ptr<io::filtering_ostream> type_cout(char type) {
  // do we have an entry for the inode type in the map
  if (out_map.find(type) == out_map.end()) {
    // no...
    // create the object
    auto out = std::make_shared< io::filtering_ostream>();

    // create the stream with gzip compression
    out->push(io::gzip_compressor());

    // sort out the output path
    char out_file[1000];
    sprintf(out_file, out_file_format, rank, type);

    // open output file for this particular rank
    out->push(io::file_sink(out_file));

    // insert the newly created stream into the map
    out_map.insert(std::make_pair(type, out));
  }
  return out_map[type];
}

char filetype(mode_t st_mode) {
  char val = 'u';
  if (S_ISREG(st_mode)) {
      val = 'f';
  } else if (S_ISDIR(st_mode)) {
      val = 'd';
  } else if (S_ISLNK(st_mode)) {
      val = 'l';
  } else if (S_ISFIFO(st_mode)) {
      val = 'p';
  } else if (S_ISSOCK(st_mode)) {
      val = 's';
  } else if (S_ISBLK(st_mode)) {
      val = 'b';
  } else if (S_ISCHR(st_mode)) {
      val = 'c';
  }
  return val;
}

bool do_lstat() {
  static struct stat buf; // place to hold the struct returned by the lstat call
  bool is_dir=false;
  if (lstat(item_buf,&buf) == 0) {
    ++linecount;

    // set the fields of the protobuf message

    // the full path
    msg.set_full_path(item_buf);

    // directory, file name, suffix, suffix class
    if (S_ISREG(buf.st_mode)) {
        char *last_slash=strrchr(item_buf,'/'); // get position of last slash
        *last_slash = '\0'; // set the last slash to null
        msg.set_directory(item_buf); // this is now just the directory part
        msg.set_file_name(last_slash+1); // this is now the null terminated filename part
        msg.set_suffix(suffix(last_slash+1));
        *last_slash = '/'; // put the last slash back to a '/' character instead of a null
        msg.set_suffix_class(""); // still need to do the suffix classifier
    } else {
        msg.set_directory("");
        msg.set_file_name("");
        msg.set_suffix("");
        msg.set_suffix_class("");
    }
    msg.set_mode(buf.st_mode);
    msg.set_lsize(buf.st_size);

    // use tenerary operator to set size taking into account sparse files
    off_t size = buf.st_size > (buf.st_blocks*512) ? (buf.st_blocks*512) : buf.st_size;
    msg.set_size(size);

    // gid, uid
    msg.set_gid(buf.st_gid);
    msg.set_uid(buf.st_uid);

    // atime, mtime
    msg.set_atime(buf.st_atime);
    msg.set_mtime(buf.st_mtime);

    // path depth
    msg.set_depth(depth(item_buf));

    // number of blocks, hard links, inode id and device id
    msg.set_blocks(buf.st_blocks);
    msg.set_nlinks(buf.st_nlink);
    msg.set_inode(buf.st_ino);
    msg.set_device(buf.st_dev);

    // write delimited protobuf message
    google::protobuf::util::SerializeDelimitedToOstream(msg, &(*type_cout(filetype(buf.st_mode))));

    // return true if this is a directory 
    is_dir=S_ISDIR(buf.st_mode);
  } else {
    // the lstat failed
    // its possible the file was deleted between adding it to the work queue and
    // removing it to do the lstat so check it exists
    if (!boost::filesystem::exists(item_buf)) {
      std::cerr << "File deleted : '" << item_buf << "' : " << strerror(errno) << std::endl;
    } else {
      std::cerr << "Cannot lstat '" << item_buf << "' : " << strerror(errno) << std::endl;
    }
  }
  return is_dir;
}

void do_readdir(CIRCLE_handle *handle) {
  int path_len=strlen(item_buf);
  DIR *d = opendir(item_buf);
  if (!d) {
    std::cerr << "Cannot open '" << item_buf <<"' : " << strerror (errno) << std::endl;
    return;
  }
  while (true) {
    struct dirent *entry;
    entry = readdir(d);
    if (entry==0) {
      break;
    }
    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") ==0 || strcmp(entry->d_name, ".snapshot") ==0) {
      continue;
    }
    *(item_buf+path_len)='/';
    strcpy(item_buf+path_len+1,entry->d_name);
    handle->enqueue(item_buf);
  }
  closedir(d);
}

// create work callback
// this is called once at the start on rank 0
// use to seed rank 0 with the initial dirs to start
// searching from
void create_work(CIRCLE_handle *handle) {
  for (int i=2; i<::argc; i++) {
    realpath(::argv[i],item_buf);
    std::cout << "adding '" << item_buf << "' to the work queue" << std::endl;
    handle->enqueue(item_buf);
  }
}

// process work callback
void process_work(CIRCLE_handle *handle) {
  // dequeue the next item
  handle->dequeue(item_buf);
  if (do_lstat()) {
    do_readdir(handle);
  }
}

// arguments :
// first argument is data directory to store the lstat files
// rest of arguments are directories to start lstating from
int main(int argc, char **argv) {

  // check we have at least 2 arguments 
  if (argc < 3) {
    std::cerr << "Usage : mpistat <data dir> <start dir 1> [<start dir 2>...<start dir n>]" << std::endl;
    return 1;
  }

  // copy arguments to global namespace
  ::argv=argv;
  ::argc=argc;
  linecount = 0;

  // initialise MPI and the libcircle stuff	
  ::rank = CIRCLE_init(0,0,CIRCLE_SPLIT_RANDOM);

  // get number of workers
  int workers;
  MPI_Comm_size(MPI_COMM_WORLD, &workers);

  // print the arguments on rank 0
  if (::rank == 0 ) {
    std::cout << "args are:";
    for (int i=0; i<::argc; i++) {
      std::cout << " " << ::argv[i];
    }
    std::cout << std::endl;
  }

  // print hostname associated with the rank
  // does it serially so that we don't get lines overlapping
  // in the output file
  for(int i = 0; i < workers; i++){
    if(i == rank){
      std::cout << rank << " of " << workers << " : "<< boost::asio::ip::host_name() << std::endl;
    }
    MPI_Barrier(MPI_COMM_WORLD);
  }

  // sort out the per-type output file format
  uint64_t digits = std::log10(workers-1)+1;
  sprintf(out_file_format, "%s/%%0%ldld_%%c.out.gz", argv[1],digits);

  // set the create work callback
  CIRCLE_cb_create(&create_work);

  // set the process work callback
  CIRCLE_cb_process(&process_work);

  // enter the processing loop
  CIRCLE_begin();

  // wait for all processing to finish and then clean up
  CIRCLE_finalize();
  std::cout << "*** FINISHED *** : wrote " << linecount << " lines" << std::endl;
  return 0;
}
