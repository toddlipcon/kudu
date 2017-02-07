#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -x

# Time marker for both stderr and stdout
date 1>&2

# Preference order:
# 1. KUDU_HOME (set by kudu_env.sh in the KUDU parcel).
# 2. CDH_KUDU_HOME (set by cdh_env.sh in the CDH parcel).
# 3. Hardcoded default value (where the Cloudera packages install Kudu).
DEFAULT_KUDU_HOME=/usr/lib/kudu
export KUDU_HOME=${KUDU_HOME:-$CDH_KUDU_HOME}
export KUDU_HOME=${KUDU_HOME:-$DEFAULT_KUDU_HOME}

CMD=$1
shift 2

function log {
  timestamp=$(date)
  echo "$timestamp: $1"       #stdout
  echo "$timestamp: $1" 1>&2; #stderr
}

# Reads a line in the format "$host:$key=$value", setting those variables.
function readconf {
  local conf
  IFS=':' read host conf <<< "$1"
  IFS='=' read key value <<< "$conf"
}

log "KUDU_HOME: $KUDU_HOME"
log "CONF_DIR: $CONF_DIR"
log "CMD: $CMD"

# Make sure we've got the main gflagfile.
GFLAG_FILE="$CONF_DIR/gflagfile"
if [ ! -r "$GFLAG_FILE" ]; then
  log "Could not find $GFLAG_FILE, exiting"
  exit 1
fi

# Make sure we've got a file describing the master config.
MASTER_FILE="$CONF_DIR/master.properties"
if [ ! -r "$MASTER_FILE" ]; then
  log "Could not find $MASTER_FILE, exiting"
  exit 1
fi

# Parse the master config.
MASTER_IPS=
for line in $(cat "$MASTER_FILE")
do
  readconf "$line"
  case $key in
    server.address)
      # Fall back to the host only if there's no defined value.
      if [ -n "$value" ]; then
        actual_value="$value"
      else
        actual_value="$host"
      fi

      # Append to comma-separated MASTER_IPS.
      if [ -n "$MASTER_IPS" ]; then
        MASTER_IPS="${MASTER_IPS},"
      fi
      MASTER_IPS="${MASTER_IPS}${actual_value}"
      ;;
  esac
done
log "Found master(s) on $MASTER_IPS"

# Enable core dumping if requested.
if [ "$ENABLE_CORE_DUMP" == "true" ]; then
  # The core dump directory should already exist.
  if [ -z "$CORE_DUMP_DIRECTORY" -o ! -d "$CORE_DUMP_DIRECTORY" ]; then
    log "Could not find core dump directory $CORE_DUMP_DIRECTORY, exiting"
    exit 1
  fi
  # It should also be writable.
  if [ ! -w "$CORE_DUMP_DIRECTORY" ]; then
    log "Core dump directory $CORE_DUMP_DIRECTORY is not writable, exiting"
    exit 1
  fi

  ulimit -c unlimited
  cd "$CORE_DUMP_DIRECTORY"
  STATUS=$?
  if [ $STATUS != 0 ]; then
    log "Could not change to core dump directory to $CORE_DUMP_DIRECTORY, exiting"
    exit $STATUS
  fi
fi

KUDU_ARGS=

if [ "$ENABLE_KERBEROS" == "true" ]; then
  # Ideally the value of the enable_kerberos parameter would dictate whether CM
  # emits --keytab_file to the flagfile in the first place. However, that isn't
  # possible, so we emit it on the command line here instead.
  #
  # CM guarantees [1] this keytab filename.
  #
  # 1. https://github.com/cloudera/cm_ext/wiki/Service-Descriptor-Language-Reference#kerberosprincipals
  KUDU_ARGS="$KUDU_ARGS --keytab_file=$CONF_DIR/kudu.keytab"
fi

if [ "$CMD" = "master" ]; then
  # Only pass --master_addresses if there's more than one master.
  #
  # Need to use [[ ]] for regex support.
  if [[ "$MASTER_IPS" =~ , ]]; then
    KUDU_ARGS="$KUDU_ARGS --master_addresses=$MASTER_IPS"
  fi

  exec "$KUDU_HOME/sbin/kudu-master" \
    $KUDU_ARGS \
    --flagfile="$GFLAG_FILE"
elif [ "$CMD" = "tserver" ]; then
  KUDU_ARGS="$KUDU_ARGS --tserver_master_addrs=$MASTER_IPS"
  exec "$KUDU_HOME/sbin/kudu-tserver" \
    $KUDU_ARGS \
    --flagfile="$GFLAG_FILE"
else
  log "Unknown command: $CMD"
  exit 2
fi
