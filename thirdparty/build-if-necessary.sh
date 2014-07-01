#!/bin/bash
# Copyright (c) 2013, Cloudera, inc.
#
# Script which downloads and builds the thirdparty dependencies
# only if necessary (i.e if they have changed or the local repository
# is dirty)

set -e
set -o pipefail

TP_DIR=$(dirname $BASH_SOURCE)
cd $TP_DIR

NEEDS_BUILD=

. vars.sh
. functions.sh

# Determine whether this subtree in the git repo has changed since thirdparty
# was last built
CUR_THIRDPARTY_HASH=$(get_thirdparty_hash)
LAST_BUILD_HASH=$(cat .build-hash || :)
if [ "$CUR_THIRDPARTY_HASH" != "$LAST_BUILD_HASH" ]; then
  echo The git repository has changed since thirdparty was last
  echo downloaded/built. Requiring rebuild.
  NEEDS_BUILD=1
else
  # Determine whether the developer has any local changes
  if has_local_changes ; then
    echo There are local changes in the git repo. Requiring rebuild.
    NEEDS_BUILD=1
  fi
fi

if [ -n "$NEEDS_BUILD" ]; then
  if ./install-s3-cached-thirdparty.sh $CUR_THIRDPARTY_HASH ; then
    echo Successfully installed thirdparty from S3 cache
  else
    ./download-thirdparty.sh
    ./build-thirdparty.sh
  fi
  echo $CUR_THIRDPARTY_HASH > .build-hash
else
  echo Not rebuilding thirdparty. No changes since last build.
fi

