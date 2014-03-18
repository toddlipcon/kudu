// Copyright (c) 2013, Cloudera, inc.

#include <boost/thread/thread.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <google/heap-profiler.h>
#include <iostream>

#include "common/schema.h"
#include "server/metadata.h"
#include "server/rpc_server.h"
#include "tablet/tablet.h"
#include "consensus/log.h"
#include "consensus/consensus.h"
#include "consensus/local_consensus.h"
#include "tablet/tablet_peer.h"
#include "tserver/tablet_server.h"
#include "tserver/ts_tablet_manager.h"
#include "util/logging.h"

using kudu::tablet::Tablet;
using kudu::tablet::TabletPeer;
using kudu::tserver::TabletServer;

DECLARE_string(heap_profile_dir);
DECLARE_bool(enable_process_lifetime_heap_profiling);

DEFINE_int32(flush_threshold_mb, 64, "Minimum memrowset size to flush");

namespace kudu {
namespace tserver {

// Pick the next tablet to perform some maintenance on. This simply round-robins
// through all the tablets in the server. It may return NULL if there are no
// tablets.
static shared_ptr<Tablet> PickATablet(const TSTabletManager* tablet_mgr, int iteration) {
  vector<shared_ptr<TabletPeer> > peers;
  tablet_mgr->GetTabletPeers(&peers);

  if (peers.empty()) {
    return shared_ptr<Tablet>();
  } else {
    return peers[iteration % peers.size()]->shared_tablet();
  }
}

static void FlushThread(const TSTabletManager* tablet_mgr) {
  int iter = 0;
  while (true) {
    shared_ptr<Tablet> tablet = PickATablet(tablet_mgr, iter++);
    if (!tablet) {
      VLOG(1) << "Not flushing: no tablets to flush";
    } else if (tablet->MemRowSetSize() > FLAGS_flush_threshold_mb * 1024 * 1024) {
      CHECK_OK(tablet->Flush());
    } else {
      VLOG(1) << "Not flushing " << tablet->tablet_id() << ": memrowset not very full";
    }
    usleep(250 * 1000);
  }
}

static void FlushDeltaMemStoresThread(const TSTabletManager* tablet_mgr) {
  int iter = 0;
  while (true) {
    shared_ptr<Tablet> tablet = PickATablet(tablet_mgr, iter++);
    if (!tablet) {
      VLOG(1) << "Not flushing deltas: no tablets";
    } else if (tablet->DeltaMemStoresSize() > FLAGS_flush_threshold_mb * 1024 * 1024) {
      CHECK_OK(tablet->FlushBiggestDMS());
    } else {
      VLOG(1) << "Not flushing deltas for " << tablet->tablet_id()
              << ": DeltaMemStores not very full";
    }
    usleep(250 * 1000);
  }
}

static void CompactThread(const TSTabletManager* tablet_mgr) {
  int iter = 0;
  while (true) {
    shared_ptr<Tablet> tablet = PickATablet(tablet_mgr, iter++);
    if (tablet) {
      CHECK_OK(tablet->Compact(Tablet::COMPACT_NO_FLAGS));
    }

    usleep(3000 * 1000);
  }
}

static void CompactDeltasThread(const TSTabletManager* tablet_mgr) {
  int iter = 0;
  while (true) {
    shared_ptr<Tablet> tablet = PickATablet(tablet_mgr, iter++);
    if (tablet) {
      CHECK_OK(tablet->MinorCompactWorstDeltas());
    }

    usleep(3000 * 1000);
  }
}

static int TabletServerMain(int argc, char** argv) {
  InitGoogleLoggingSafe(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);
  if (argc != 1) {
    std::cerr << "usage: " << argv[0] << std::endl;
    return 1;
  }

#ifndef ADDRESS_SANITIZER
  // tcmalloc and address sanitizer can not be used together
  if (FLAGS_enable_process_lifetime_heap_profiling) {
    HeapProfilerStart(FLAGS_heap_profile_dir.c_str());
  }
#endif

  TabletServerOptions opts;
  TabletServer server(opts);
  LOG(INFO) << "Initializing tablet server...";
  CHECK_OK(server.Init());

  // Temporary hack for demos: start threads which compact/flush the tablet.
  // Eventually this will be part of TabletServer itself, and take care of deciding
  // which tablet to perform operations on. But as a stop-gap, just start these
  // simple threads here from main.
  LOG(INFO) << "Starting flush/compact threads";
  const TSTabletManager* ts_tablet_manager = server.tablet_manager();
  boost::thread compact_thread(CompactThread, ts_tablet_manager);
  boost::thread compact_deltas_thread(CompactDeltasThread, ts_tablet_manager);
  boost::thread flush_thread(FlushThread, ts_tablet_manager);
  boost::thread flushdm_thread(FlushDeltaMemStoresThread, ts_tablet_manager);

  LOG(INFO) << "Starting tablet server...";
  CHECK_OK(server.Start());

  LOG(INFO) << "Tablet server successfully started.";
  while (true) {
    sleep(60);
  }

  return 0;
}

} // namespace tserver
} // namespace kudu

int main(int argc, char** argv) {
  return kudu::tserver::TabletServerMain(argc, argv);
}
