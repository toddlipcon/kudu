# Copyright (c) 2013, Cloudera, inc.

TP_DIR=$(dirname $BASH_SOURCE)
PREFIX=$TP_DIR/installed

# This URL corresponds to the CloudFront Distribution for the S3
# bucket cloudera-thirdparty-libs which is directly accessible at
# http://cloudera-thirdparty-libs.s3.amazonaws.com/
S3_BUCKET_S3_URL=s3://cloudera-thirdparty-libs
S3_BUCKET_HTTP_URL=http://cloudera-thirdparty-libs.s3.amazonaws.com
CLOUDFRONT_URL_PREFIX=http://d3dr9sfxru4sde.cloudfront.net

GFLAGS_VERSION=1.5
GFLAGS_DIR=$TP_DIR/gflags-$GFLAGS_VERSION

GLOG_VERSION=0.3.3
GLOG_DIR=$TP_DIR/glog-$GLOG_VERSION

GPERFTOOLS_VERSION=2.1
GPERFTOOLS_DIR=$TP_DIR/gperftools-$GPERFTOOLS_VERSION

GTEST_VERSION=1.6.0
GTEST_DIR=$TP_DIR/gtest-$GTEST_VERSION

PROTOBUF_VERSION=2.5.0
PROTOBUF_DIR=$TP_DIR/protobuf-$PROTOBUF_VERSION

CMAKE_VERSION=2.8.10.2
CMAKE_DIR=$TP_DIR/cmake-${CMAKE_VERSION}

SNAPPY_VERSION=1.1.0
SNAPPY_DIR=$TP_DIR/snappy-$SNAPPY_VERSION

LZ4_VERSION=svn
LZ4_DIR=$TP_DIR/lz4-$LZ4_VERSION

ZLIB_VERSION=1.2.8
ZLIB_DIR=$TP_DIR/zlib-$ZLIB_VERSION

LIBEV_VERSION=4.15
LIBEV_DIR=$TP_DIR/libev-$LIBEV_VERSION

CYRUS_SASL_VERSION=2.1.26
CYRUS_SASL_DIR=$TP_DIR/cyrus-sasl-$CYRUS_SASL_VERSION

RAPIDJSON_VERSION=0.11
RAPIDJSON_DIR=$TP_DIR/rapidjson-${RAPIDJSON_VERSION}

# Hash of the squeasel git revision to use.
# (from http://github.mtv.cloudera.com/CDH/squeasel)
#
# To re-build this tarball use the following in the squeasel repo:
#  export NAME=squeasel-$(git rev-parse HEAD)
#  git archive HEAD --prefix=$NAME/ -o /tmp/$NAME.tar.gz
#  s3cmd put -P /tmp/$NAME.tar.gz s3://cloudera-thirdparty-libs/$NAME.tar.gz
#
# File a HD ticket for access to the cloudera-dev AWS instance to push to S3.
SQUEASEL_VERSION=48355449ecf93256cd0bf9411f7807197c9e0e8e
SQUEASEL_DIR=$TP_DIR/squeasel-${SQUEASEL_VERSION}

# SVN revision of google style guide:
# https://code.google.com/p/google-styleguide/source/list
GSG_REVISION=134
GSG_DIR=$TP_DIR/google-styleguide-r${GSG_REVISION}

GCOVR_VERSION=3.0
GCOVR_DIR=$TP_DIR/gcovr-${GCOVR_VERSION}

CURL_VERSION=7.32.0
CURL_DIR=$TP_DIR/curl-${CURL_VERSION}

# Hash of the crcutil git revision to use.
# (from http://github.mtv.cloudera.com/CDH/crcutil)
#
# To re-build this tarball use the following in the crcutil repo:
#  export NAME=crcutil-$(git rev-parse HEAD)
#  git archive HEAD --prefix=$NAME/ -o /tmp/$NAME.tar.gz
#  s3cmd put -P /tmp/$NAME.tar.gz s3://cloudera-thirdparty-libs/$NAME.tar.gz
CRCUTIL_VERSION=440ba7babeff77ffad992df3a10c767f184e946e
CRCUTIL_DIR=$TP_DIR/crcutil-${CRCUTIL_VERSION}

LIBUNWIND_VERSION=1.1a
LIBUNWIND_DIR=$TP_DIR/libunwind-${LIBUNWIND_VERSION}
