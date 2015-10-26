// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package org.kududb.client;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ImmutableList;
import com.stumbleupon.async.Callback;

import org.junit.After;
import org.junit.Test;
import org.kududb.ColumnSchema;
import org.kududb.Schema;
import org.kududb.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

/**
 * Test Get API performance using async client.
 * 
 * To get real performance numbers, log levels(both java slf4j and 
 * tserver glog) should be set to INFO.
 */
public class TestGetPerf {
  private static final Logger LOG = LoggerFactory.getLogger(TestGetPerf.class);

  private static final String TABLE_NAME = TestAsyncKuduClient.class.getName();

  private static final int NUM_TABLET_SERVERS = 1;

  private MiniKuduCluster miniCluster;

  // Comma separate describing the master addresses and ports.
  protected String masterAddresses;

  protected final int DEFAULT_SLEEP = 50000;

  public static Schema createSchema() {
    ArrayList<ColumnSchema> columns = new ArrayList<ColumnSchema>(5);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("int32_val", Type.INT32).nullable(false).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("int64_val", Type.INT64).nullable(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("float_val", Type.FLOAT).nullable(false).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("binary", Type.BINARY)
        .desiredBlockSize(10240).build());
    return new Schema(columns);
  }

  public static Operation createInsert(KuduTable t, int key) {
    Insert insert = t.newInsert();
    PartialRow row = insert.getRow();
    row.addInt(0, key);
    row.addInt(1, key * 2);
    row.addLong(2, key * 3);
    row.addFloat(3, key);
    row.addBinary(4, String.format("binary %d", key).getBytes());
    return insert;
  }

  static private class GetWorkload {
    private final Schema schema;
    private final AsyncKuduGetter getter[];
    private volatile boolean run;
    private final AtomicLong total = new AtomicLong();
    private final int rowCount;
    private final AtomicLong stopCounter = new AtomicLong();
    GetWorkload(Schema schema, AsyncKuduGetter[] getter, int rowCount) {
      this.schema = schema;
      this.getter = getter;
      this.rowCount = rowCount;
    }

    class GetCallback implements Callback<Void, SingleRowResult> {
      final Random rand;
      final PartialRow rowkey;
      final int idx;
      int key;

      public GetCallback(Schema schema, int idx, int seed) {
        this.rand = new Random(seed);
        this.rowkey = schema.newPartialRow();
        this.idx = idx;
      }

      @Override
      public Void call(SingleRowResult result) throws Exception {
        if (result != null) {
          total.incrementAndGet();
        }
        if (run) {
          key = rand.nextInt() % rowCount;
          rowkey.addInt(0, key);
          getter[idx].get(rowkey).addCallback(this);
        } else {
          stopCounter.decrementAndGet();
        }
        return null;
      }
    }

    void run(int concurrency, int seconds) throws Exception {
      run = true;
      stopCounter.set(concurrency);
      for (int i = 0; i < concurrency; i++) {
        new GetCallback(schema, i % getter.length, i).call(null);
      }
      long lastCount = total.get();
      long lastTime = System.currentTimeMillis();
      long endTime = lastTime + seconds * 1000;
      while (true) {
        Thread.sleep(2000);
        long now = System.currentTimeMillis();
        long curCount = total.get();
        LOG.info(String.format("Total: %d QPS: %.0f", curCount,
            (curCount - lastCount) / ((now - lastTime) / 1000.0 + 0.0001)));
        if (now >= endTime) {
          break;
        }
        lastCount = curCount;
        lastTime = now;
      }
      run = false;
      while (stopCounter.get() > 0) {
        Thread.sleep(100);
      }
    }
  }

  static int getEnvOrDefault(String name, int defaultValue) {
    String v = System.getenv(name);
    if (v != null) {
      return Integer.parseInt(v);
    } else {
      return defaultValue;
    }
  }

  void runWorkload(int rowCount, int threads, int concurrency, int seconds) throws Exception {
    AsyncKuduClient [] client = new AsyncKuduClient[threads];
    AsyncKuduGetter [] getter = new AsyncKuduGetter[threads];
    KuduTable t = null;
    for (int i = 0; i < threads; i++) {
      client[i] = new AsyncKuduClient.AsyncKuduClientBuilder(masterAddresses)
          .defaultAdminOperationTimeoutMs(DEFAULT_SLEEP)
          .bossCount(1)
          .workerCount(1)
          .build();
      t = client[i].openTable(TABLE_NAME).join();
      getter[i] = client[i].newGetterBuilder(t)
          .setProjectedColumnIndexes(ImmutableList.of(4)).build();
    }
    GetWorkload workload = new GetWorkload(t.getSchema(), getter, rowCount);
    workload.run(concurrency, seconds);
    for (int i = 0; i < client.length; i++) {
      client[i].close();
    }
  }

  public void setupServerSide(int rowCount) throws Exception {
    miniCluster = new MiniKuduCluster.MiniKuduClusterBuilder()
        .numMasters(1)
        .numTservers(NUM_TABLET_SERVERS)
        .defaultTimeoutMs(DEFAULT_SLEEP).build();
    masterAddresses = miniCluster.getMasterAddresses();

    LOG.info("Waiting for tablet servers...");
    if (!miniCluster.waitForTabletServers(NUM_TABLET_SERVERS)) {
      fail("Couldn't get " + NUM_TABLET_SERVERS
          + " tablet servers running, aborting");
    }
    // create table and insert rows.
    KuduClient client = new KuduClient.KuduClientBuilder(masterAddresses).build();
    try {
      CreateTableOptions options = new CreateTableOptions().setNumReplicas(1);
      int tablets = getEnvOrDefault("tablets", 1);
      if (tablets > 1) {
        LOG.info("create table with " + tablets + " tablets");
        options.addHashPartitions(ImmutableList.of("key"), tablets);
      }
      KuduTable table = client.createTable(TABLE_NAME, createSchema(), options);
      KuduSession session = client.newSession();
      try {
        session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
        LOG.info(String.format("Inserting %d rows", rowCount));
        for (int i = 0; i < rowCount; i++) {
          session.apply(createInsert(table, i));
        }
        session.flush();
      } finally {
        session.close();
      }
      assertFalse(session.hasPendingOperations());
      LOG.info("Insert complete");
    } finally {
      client.close();
    }
  }

  @After
  public void tearDown() throws Exception {
    if (miniCluster != null) {
      miniCluster.shutdown();
    }
  }

  @Test(timeout = 200000)
  public void testGetPerf() throws Exception {
    int rowCount = getEnvOrDefault("rows", 100000);
    String server = System.getenv("server");

    if (server == null) {
      setupServerSide(rowCount);
      runWorkload(rowCount, getEnvOrDefault("clients", 1),
          getEnvOrDefault("concurrency", 40), getEnvOrDefault("time", 5));
    } else if (server.equals("self")) {
      setupServerSide(rowCount);
      LOG.info("Server address: " + masterAddresses);
      while (true) {
        Thread.sleep(1000);
      }
    } else {
      masterAddresses = server;
      runWorkload(rowCount, getEnvOrDefault("clients", 1),
          getEnvOrDefault("concurrency", 40), getEnvOrDefault("time", 5));
    }
  }
}

