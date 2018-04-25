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

package org.apache.kudu.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This test contains scenarios to verify that client re-acquires authn token upon expiration
 * of the current one and automatically retries the call.
 */
public class TestAuthnTokenReacquire extends BaseKuduTest {

  private static final String TABLE_NAME = "TestAuthnTokenReacquire-table";
  private static final int TOKEN_TTL_SEC = 1;
  private static final int OP_TIMEOUT_MS = 60 * TOKEN_TTL_SEC * 1000;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Inject additional INVALID_AUTHENTICATION_TOKEN responses from both the master and tablet
    // servers, even for not-yet-expired tokens.
    miniClusterBuilder
        .enableKerberos()
        .addMasterFlag(String.format("--authn_token_validity_seconds=%d", TOKEN_TTL_SEC))
        .addMasterFlag("--rpc_inject_invalid_authn_token_ratio=0.5")
        .addTserverFlag("--rpc_inject_invalid_authn_token_ratio=0.5");

    BaseKuduTest.setUpBeforeClass();
  }

  private static void dropConnections() {
    for (Connection c : client.getConnectionListCopy()) {
      c.disconnect();
    }
  }

  private static void dropConnectionsAndExpireToken() throws InterruptedException {
    // Drop all connections from the client to Kudu servers.
    dropConnections();
    // Wait for authn token expiration.
    Thread.sleep(TOKEN_TTL_SEC * 1000);
  }

  @Test
  public void testBasicMasterOperations() throws Exception {
    // To ratchet up the intensity a bit, run the scenario by several concurrent threads.
    List<Thread> threads = new ArrayList<>();
    final Map<Integer, Throwable> exceptions =
        Collections.synchronizedMap(new HashMap<Integer, Throwable>());
    for (int i = 0; i < 8; ++i) {
      final int threadIdx = i;
      Thread thread = new Thread(new Runnable() {
        @Override
        @SuppressWarnings("AssertionFailureIgnored")
        public void run() {
          final String tableName = "TestAuthnTokenReacquire-table-" + threadIdx;
          try {
            ListTabletServersResponse response = syncClient.listTabletServers();
            assertNotNull(response);
            dropConnectionsAndExpireToken();

            ListTablesResponse tableList = syncClient.getTablesList(tableName);
            assertNotNull(tableList);
            assertTrue(tableList.getTablesList().isEmpty());
            dropConnectionsAndExpireToken();

            syncClient.createTable(tableName, basicSchema, getBasicCreateTableOptions());
            dropConnectionsAndExpireToken();

            KuduTable table = syncClient.openTable(tableName);
            assertEquals(basicSchema.getColumnCount(), table.getSchema().getColumnCount());
            dropConnectionsAndExpireToken();

            syncClient.deleteTable(tableName);
            assertFalse(syncClient.tableExists(tableName));
          } catch (Throwable e) {
            //noinspection ThrowableResultOfMethodCallIgnored
            exceptions.put(threadIdx, e);
          }
        }
      });
      thread.run();
      threads.add(thread);
    }
    for (Thread thread : threads) {
      thread.join();
    }
    if (!exceptions.isEmpty()) {
      for (Map.Entry<Integer, Throwable> e : exceptions.entrySet()) {
        LOG.error("exception in thread {}: {}", e.getKey(), e.getValue());
      }
      fail("test failed: unexpected errors");
    }
  }

  @Test
  public void testBasicWorkflow() throws Exception {
    KuduTable table = syncClient.createTable(TABLE_NAME, basicSchema,
        getBasicCreateTableOptions());
    dropConnectionsAndExpireToken();

    KuduSession session = syncClient.newSession();
    session.setTimeoutMillis(OP_TIMEOUT_MS);
    session.apply(createBasicSchemaInsert(table, 1));
    session.flush();
    RowErrorsAndOverflowStatus errors = session.getPendingErrors();
    assertFalse(errors.isOverflowed());
    assertEquals(0, session.countPendingErrors());
    dropConnectionsAndExpireToken();

    KuduTable scanTable = syncClient.openTable(TABLE_NAME);
    AsyncKuduScanner scanner = new AsyncKuduScanner.AsyncKuduScannerBuilder(client, scanTable)
        .scanRequestTimeout(OP_TIMEOUT_MS)
        .build();
    assertEquals(1, countRowsInScan(scanner));
    dropConnectionsAndExpireToken();

    syncClient.deleteTable(TABLE_NAME);
    assertFalse(syncClient.tableExists(TABLE_NAME));
  }
}
