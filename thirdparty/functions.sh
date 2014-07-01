# Determine a unique name for this system.
# This magic results in a string like 'Ubuntu_13_10_x86_64' or
# '_CentOS_release_6_4_Final__x86_64'.
function generate_system_id() {
  (lsb_release -ds ; echo "_"; uname -m) | perl -p -e 'chomp; s,[^a-zA-Z0-9]+,_,g;'
}

function generate_s3_url() {
  local scheme=$1
  local hash=$2
  local system=$3
  local path=thirdparty-install-cache/$system/$hash.tgz
  case $scheme in
    s3)
      echo ${S3_BUCKET_S3_URL}/$path
      ;;
    http)
      echo ${S3_BUCKET_HTTP_URL}/$path
      ;;
    *)
      echo unknown scheme: $scheme >/dev/stderr
      exit 1
  esac
}

function get_thirdparty_hash() {
  (cd $TP_DIR/.. && git ls-tree -d HEAD thirdparty | awk '{print $3}')
}

function has_local_changes() {
  ! ( git diff --quiet .  && git diff --cached --quiet . )
}