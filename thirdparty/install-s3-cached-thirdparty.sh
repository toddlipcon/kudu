#!/bin/bash
# Copyright (c) 2014, Cloudera, inc.

set -e

TP_DIR=$(dirname $BASH_SOURCE)
cd $TP_DIR

. vars.sh
. functions.sh

HASH=$(get_thirdparty_hash)

if has_local_changes ; then
  echo Will not use cached thirdparty build -- there are local changes > /dev/stderr
  exit 1
fi

system=$(generate_system_id)
url=$(generate_s3_url http $HASH $system)

# Quick header check to see if the cache is available.
status_header=$(curl -s -I $url | head -1)
if ! [[ "$status_header" =~ '200 OK' ]]; then
  echo No S3 cache available for build $HASH on system $system
  echo Tried URL: $url
  echo Status: $status_header
  exit 1
fi

# Download and install
echo Downloading thirdparty cache from $url ...
curl -o thirdparty-install.tgz $url

echo ==================
echo Removing any existing thirdparty install...
rm -Rf thirdparty/

echo ==================
echo Unpacking cache...
tar xzf thirdparty-install.tgz

echo ==================
echo Successfully installed thirdparty from cache

