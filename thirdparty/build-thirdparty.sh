#!/bin/bash
# Copyright (c) 2012, Cloudera, inc.

set -x
set -e
TP_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)

# We use -O2 instead of -O3 for thirdparty since benchmarks indicate
# that the benefits of a smaller code size outweight the benefits of
# more inlining.
EXTRA_CXXFLAGS="-O2 -g $CXXFLAGS"
if [[ "$OSTYPE" =~ ^linux ]]; then
  OS_LINUX=1
elif [[ "$OSTYPE" == "darwin"* ]]; then
  OS_OSX=1
  EXTRA_CXXFLAGS="$EXTRA_CXXFLAGS -stdlib=libstdc++"
fi

EXTRA_LDFLAGS="$LDFLAGS"

source $TP_DIR/vars.sh

################################################################################

if [ "$#" = "0" ]; then
  F_ALL=1
else
  # Allow passing specific libs to build on the command line
  for arg in "$*"; do
    case $arg in
      "cmake")      F_CMAKE=1 ;;
      "gflags")     F_GFLAGS=1 ;;
      "glog")       F_GLOG=1 ;;
      "gmock")      F_GMOCK=1 ;;
      "gperftools") F_GPERFTOOLS=1 ;;
      "libev")      F_LIBEV=1 ;;
      "lz4")        F_LZ4=1 ;;
      "protobuf")   F_PROTOBUF=1 ;;
      "rapidjson")  F_RAPIDJSON=1 ;;
      "snappy")     F_SNAPPY=1 ;;
      "zlib")       F_ZLIB=1 ;;
      "squeasel")   F_SQUEASEL=1 ;;
      "gsg")        F_GSG=1 ;;
      "gcovr")      F_GCOVR=1 ;;
      "curl")       F_CURL=1 ;;
      "crcutil")    F_CRCUTIL=1 ;;
      "libunwind")  F_LIBUNWIND=1 ;;
      "llvm")       F_LLVM=1 ;;
      "trace-viewer") F_TRACE_VIEWER=1 ;;
      *)            echo "Unknown module: $arg"; exit 1 ;;
    esac
  done
fi

################################################################################

# Determine how many parallel jobs to use for make based on the number of cores
if [ "$OS_LINUX" ]; then
  PARALLEL=$(grep -c processor /proc/cpuinfo)
elif [ "$OS_OSX" ]; then
  PARALLEL=$(sysctl -n hw.ncpu)
else
  echo Unsupported platform $OSTYPE
  exit 1
fi

mkdir -p "$PREFIX/include"
mkdir -p "$PREFIX/lib"

# On some systems, autotools installs libraries to lib64 rather than lib.  Fix
# this by setting up lib64 as a symlink to lib.  We have to do this step first
# to handle cases where one third-party library depends on another.
ln -sf lib "$PREFIX/lib64"

# use the compiled tools
export PATH=$PREFIX/bin:$PATH

# build cmake
if [ -n "$F_ALL" -o -n "$F_CMAKE" ]; then
  cd $CMAKE_DIR
  ./bootstrap --prefix=$PREFIX --parallel=8
  make -j$PARALLEL
  make install
fi

# build gflags
if [ -n "$F_ALL" -o -n "$F_GFLAGS" ]; then
  cd $GFLAGS_DIR
  CXXFLAGS=$EXTRA_CXXFLAGS ./configure --with-pic --prefix=$PREFIX
  make -j$PARALLEL install
fi

# build libunwind (glog consumes it)
# It depends on link.h which is unavaible on OSX, use MacPorts' instead.
if [ "$OS_LINUX" ]; then
  if [ -n "$F_ALL" -o -n "$F_LIBUNWIND" ]; then
    cd $LIBUNWIND_DIR
    # Disable minidebuginfo, which depends on liblzma, until/unless we decide to
    # add liblzma to thirdparty.
    ./configure --disable-minidebuginfo --with-pic --prefix=$PREFIX
    make -j$PARALLEL install
  fi
fi

# build glog
if [ -n "$F_ALL" -o -n "$F_GLOG" ]; then
  cd $GLOG_DIR
  # We need to set "-g -O2" because glog only provides those flags when CXXFLAGS is unset.
  # Help glog find libunwind.
  CXXFLAGS="$EXTRA_CXXFLAGS" \
    CPPFLAGS=-I$PREFIX/include \
    LDFLAGS=-L$PREFIX/lib \
    ./configure --with-pic --prefix=$PREFIX --with-gflags=$PREFIX
  make -j$PARALLEL install
fi

# build gperftools
if [ -n "$F_ALL" -o -n "$F_GPERFTOOLS" ]; then
  cd $GPERFTOOLS_DIR
  CXXFLAGS=$EXTRA_CXXFLAGS ./configure --enable-frame-pointers --with-pic --prefix=$PREFIX
  make -j$PARALLEL install
fi

# build gmock
if [ -n "$F_ALL" -o -n "$F_GMOCK" ]; then
  cd $GMOCK_DIR
  # Run the static library build, then the shared library build.
  for SHARED in OFF ON; do
    rm -rf CMakeCache.txt CMakeFiles/
    CXXFLAGS="-fPIC -g $EXTRA_CXXFLAGS" \
      $PREFIX/bin/cmake -DBUILD_SHARED_LIBS=$SHARED .
    make -j$PARALLEL
  done
fi

# build protobuf
if [ -n "$F_ALL" -o -n "$F_PROTOBUF" ]; then
  cd $PROTOBUF_DIR
  CXXFLAGS="$EXTRA_CXXFLAGS" \
      LDFLAGS="$EXTRA_LDFLAGS" \
      ./configure --with-pic --enable-shared --enable-static --prefix=$PREFIX
  make -j$PARALLEL install
fi

# build snappy
if [ -n "$F_ALL" -o -n "$F_SNAPPY" ]; then
  cd $SNAPPY_DIR
  CXXFLAGS=$EXTRA_CXXFLAGS \
    ./configure --with-pic --prefix=$PREFIX
  make -j$PARALLEL install
fi

# build zlib
if [ -n "$F_ALL" -o -n "$F_ZLIB" ]; then
  cd $ZLIB_DIR
  CFLAGS="$EXTRA_CXXFLAGS -fPIC" ./configure --prefix=$PREFIX
  make -j$PARALLEL install
fi

# build lz4
if [ -n "$F_ALL" -o -n "$F_LZ4" ]; then
  cd $LZ4_DIR
  CFLAGS="$EXTRA_CXXFLAGS -fPIC" $PREFIX/bin/cmake -DCMAKE_INSTALL_PREFIX:PATH=$PREFIX .
  make -j$PARALLEL install
fi

## build libev
if [ -n "$F_ALL" -o -n "$F_LIBEV" ]; then
  cd $LIBEV_DIR
  ./configure --with-pic --prefix=$PREFIX
  make -j$PARALLEL install
fi

# Build rapidjson
if [ -n "$F_ALL" -o -n "$F_RAPIDJSON" ]; then
  # rapidjson is actually a header-only library, so our "build" is really
  # just installing it into our prefix
  cd $RAPIDJSON_DIR
  rsync -av --delete $RAPIDJSON_DIR/include/rapidjson/ $PREFIX/include/rapidjson/
fi

# Build squeasel
if [ -n "$F_ALL" -o -n "$F_SQUEASEL" ]; then
  # Mongoose's Makefile builds a standalone web server, whereas we just want
  # a static lib
  cd $SQUEASEL_DIR
  ${CC:-gcc} $CFLAGS -fno-omit-frame-pointer -std=c99 -O3 -DNDEBUG -DNO_SSL_DL -fPIC -c squeasel.c
  ar rs libsqueasel.a squeasel.o
  cp libsqueasel.a $PREFIX/lib/
  cp squeasel.h $PREFIX/include/
fi

# Build curl
if [ -n "$F_ALL" -o -n "$F_CURL" ]; then
  # Configure for a very minimal install - basically only HTTP,
  # since we only use this for testing our own HTTP endpoints
  # at this point in time.
  cd $CURL_DIR
  ./configure --prefix=$PREFIX \
    --disable-ftp \
    --disable-file \
    --disable-ldap \
    --disable-ldaps \
    --disable-rtsp \
    --disable-dict \
    --disable-telnet \
    --disable-tftp \
    --disable-pop3 \
    --disable-imap \
    --disable-smtp \
    --disable-gopher \
    --disable-manual \
    --without-rtmp \
    --disable-ipv6
  make -j$PARALLEL
  make install
fi

## build crcutil
if [ -n "$F_ALL" -o -n "$F_CRCUTIL" ]; then
  cd $CRCUTIL_DIR
  ./autogen.sh
  CXXFLAGS=$EXTRA_CXXFLAGS \
    ./configure --prefix=$PREFIX
  make -j$PARALLEL install
fi

# Copy boost_uuid into the include directory.
# This is a header-only library which isn't present in some older versions of
# boost (eg the one on el6). So, we check it in and put it in our own include
# directory.
rsync -a $TP_DIR/boost_uuid/boost/ $PREFIX/include/boost/

# Copy cpplint tool into bin directory
if [ -n "$F_ALL" -o -n "$F_GSG" ]; then
  cp $GSG_DIR/cpplint/cpplint.py $PREFIX/bin/cpplint.py
fi

# Copy gcovr tool into bin directory
if [ -n "$F_ALL" -o -n "$F_GCOVR" ]; then
  cp -a $GCOVR_DIR/scripts/gcovr $PREFIX/bin/gcovr
fi

# build llvm
if [ -n "$F_ALL" -o -n "$F_LLVM" ]; then
  mkdir -p $LLVM_BUILD
  cd $LLVM_BUILD

  # Build LLVM with a pre-existing clang, if it exists.
  #
  # This isn't strictly necessary, but compiler-rt doesn't compile with
  # gcc 4.4, so we use a pre-existing clang to safely bootstrap it.
  #
  # If we can't find clang, we'll use the system compiler and hope for
  # the best.
  # - http://llvm.org/bugs/show_bug.cgi?id=16532
  # - http://code.google.com/p/address-sanitizer/issues/detail?id=146
  CLANG=$(which clang || :)
  CLANGXX=$(which clang++ || :)
  if [ -n "$CLANG" -a -n "$CLANGXX" -a -z "$CC" -a -z "$CXX" ]; then
    export CC=$CLANG
    export CXX=$CLANGXX
  fi

  # Rebuild the CMake cache every time.
  rm -Rf CMakeCache.txt CMakeFiles/

  $PREFIX/bin/cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=$PREFIX \
    -DLLVM_TARGETS_TO_BUILD=X86 \
    -DCMAKE_CXX_FLAGS=$EXTRA_CXXFLAGS \
    $LLVM_DIR
  if [ -n $CLANG -a -n $CLANGXX ]; then
    unset CC
    unset CXX
  fi

  make -j$PARALLEL install
fi

# Build trace-viewer (by copying it into www/)
if [ -n "$F_ALL" -o -n "$F_TRACE_VIEWER" ]; then
  echo Installing trace-viewer into the www directory
  cp -a $TRACE_VIEWER_DIR/* $TP_DIR/../www/
fi

echo "---------------------"
echo "Thirdparty dependencies built and installed into $PREFIX successfully"
