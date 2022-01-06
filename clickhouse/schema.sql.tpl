CREATE DATABASE IF NOT EXISTS {{ database }};
CREATE TABLE {{ database }}.files
(
  `full_path` String,
  `directory` String,
  `file_name` String,
  `suffix` String,
  `suffix_class`String,
  `mode` UInt16,
  `lsize` UInt64,
  `size` UInt64,
  `gid` UInt32,
  `uid` UInt32,
  `atime` UInt32,
  `mtime` UInt32,
  `ttime` UInt32 MATERIALIZED max(atime,mtime),
  `depth` UInt64,
  `blocks` UInt64,
  `nlinks` UInt32,
  `inode` UInt64,
  `device`UInt64,
  `atime_cost` Float64 MATERIALIZED 10.0*(blocks*512/(1024*1024*1024*1024))*(({{ now }}-atime)/(365*24*3600/12)),
  `mtime_cost` Float64 MATERIALIZED 10.0*(blocks*512/(1024*1024*1024*1024))*(({{ now }}-mtime)/(365*24*3600/12)),
  `ttime_cost` Float64 MATERIALIZED 10.0*(blocks*512/(1024*1024*1024*1024))*(({{ now }}-ttime)/(365*24*3600/12)),
  `atime_days` Int64 MATERIALIZED ({{ now }}-atime)/(24*3600),
  `mtime_days` Int64 MATERIALIZED ({{ now }}-mtime)/(24*3600)
  `ttime_days` Int64 MATERIALIZED ({{ now }}-ttime)/(24*3600)
)
ENGINE = MergeTree()
PARTITION BY (gid,uid)
ORDER BY (gid,uid,full_path);

CREATE TABLE {{ database }}.directories
(
  `full_path` String,
  `directory` String,
  `file_name` String,
  `suffix` String,
  `suffix_class`String,
  `mode` UInt16,
  `lsize` UInt64,
  `size` UInt64,
  `gid` UInt32,
  `uid` UInt32,
  `atime` UInt32,
  `mtime` UInt32,
  `depth` UInt64,
  `blocks` UInt64,
  `nlinks` UInt32,
  `inode` UInt64,
  `device`UInt64
)
ENGINE = MergeTree()
PARTITION BY (gid,uid)
ORDER BY (gid,uid,depth,full_path);

CREATE TABLE {{ database }}.symlinks
(
  `full_path` String,
  `directory` String,
  `file_name` String,
  `suffix` String,
  `suffix_class`String,
  `mode` UInt16,
  `lsize` UInt64,
  `size` UInt64,
  `gid` UInt32,
  `uid` UInt32,
  `atime` UInt32,
  `mtime` UInt32,
  `depth` UInt64,
  `blocks` UInt64,
  `nlinks` UInt32,
  `inode` UInt64,
  `device`UInt64
)
ENGINE = MergeTree()
PARTITION BY (gid,uid)
ORDER BY (gid,uid,full_path);
