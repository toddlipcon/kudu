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
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;
import org.kududb.annotations.InterfaceAudience;
import org.kududb.annotations.InterfaceStability;
import org.kududb.ColumnSchema;
import org.kududb.Schema;
import org.kududb.WireProtocol;
import org.kududb.tserver.Tserver.GetRequestPB;
import org.kududb.tserver.Tserver.GetResponsePB;
import org.kududb.tserver.Tserver.TabletServerErrorPB;
import org.kududb.util.Pair;
import org.kududb.util.Slice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;
import com.google.protobuf.ZeroCopyLiteralByteString;
import com.stumbleupon.async.Deferred;


/**
 * Creates a getter to get a row by key from Kudu.
 * <p>
 * This class is <strong>not synchronized</strong> as it's expected to be
 * used from a single thread at a time.
 * <p>
 * A {@code AsyncKuduGetter} is not reusable.
 *
 * <h1>A note on passing {@code byte} arrays in argument</h1>
 * None of the method that receive a {@code byte[]} in argument will copy it.
 * For more info, please refer to the documentation of {@link KuduRpc}.
 * <h1>A note on passing {@code String}s in argument</h1>
 * All strings are assumed to use the platform's default charset.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class AsyncKuduGetter {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncKuduGetter.class);

  private final AsyncKuduClient client;
  private final KuduTable table;
  private final Schema schema;

  final long getRequestTimeout;

  AsyncKuduGetter(AsyncKuduClient client, KuduTable table,
      List<String> projectedColumnNames, List<Integer> projectedColumnIndexes,
      long getRequestTimeout) {
    this.client = client;
    this.table = table;
    this.getRequestTimeout = getRequestTimeout;
    if (projectedColumnNames != null) {
      List<ColumnSchema> columns = new ArrayList<ColumnSchema>();
      for (String columnName : projectedColumnNames) {
        ColumnSchema columnSchema = table.getSchema().getColumn(columnName);
        if (columnSchema == null) {
          throw new IllegalArgumentException("Unkown column " + columnName);
        }
        columns.add(columnSchema);
      }
      this.schema = new Schema(columns);
    } else if (projectedColumnIndexes != null) {
      List<ColumnSchema> columns = new ArrayList<ColumnSchema>();
      for (Integer columnIndex : projectedColumnIndexes) {
        ColumnSchema columnSchema = table.getSchema().getColumnByIndex(columnIndex);
        if (columnSchema == null) {
          throw new IllegalArgumentException("Unknown column index " + columnIndex);
        }
        columns.add(columnSchema);
      }
      this.schema = new Schema(columns);
    } else {
      this.schema = table.getSchema();
    }
  }

  public Deferred<SingleRowResult> get(PartialRow row) {
    return client.sendRpcToTablet(new GetRequest(table, schema, row, getRequestTimeout));
  }

  private static RowResult toRowResult(Schema schema,
      WireProtocol.RowwiseRowBlockPB data,
      final CallResponse callResponse) {
    if (data == null || data.getNumRows() == 0) {
      return null;
    }
    if (data.getNumRows() > 1) {
      throw new NonRecoverableException("Get operation return multiple rows");
    }

    Slice bs = callResponse.getSidecar(data.getRowsSidecar());
    Slice indirectBs = callResponse.getSidecar(data.getIndirectDataSidecar());
    int expectSize = schema.getRowSize();
    // Integrity check
    if (expectSize != bs.length()) {
      throw new NonRecoverableException("RowResult block has " + bs.length() + " bytes of data " +
          "but expected " + expectSize + " for 1 row");
    }
    RowResult ret = new RowResult(schema, bs, indirectBs);
    ret.advancePointer();
    return ret;
  }

  private static final class GetRequest extends KuduRpc<SingleRowResult> implements KuduRpc.HasKey {
    private final Schema schema;
    private final PartialRow row;

    public GetRequest(KuduTable table, Schema schema, PartialRow row, long timeout) {
      super(table);
      this.schema = schema;
      this.row = row;
      this.setTimeoutMillis(timeout);
    }

    @Override
    String serviceName() { return TABLET_SERVER_SERVICE_NAME; }

    @Override
    String method() {
      return "Get";
    }

    @Override
    public byte[] partitionKey() {
      return this.getTable().getPartitionSchema().encodePartitionKey(row);
    }

    @Override
    ChannelBuffer serialize(Message header) {
      final GetRequestPB.Builder builder = GetRequestPB.newBuilder();
      builder.setTabletId(ZeroCopyLiteralByteString.wrap(getTablet().getTabletIdAsBytes()));
      builder.setKey(ZeroCopyLiteralByteString.copyFrom(row.encodePrimaryKey()));
      builder.addAllProjectedColumns(ProtobufHelper.schemaToListPb(schema));
      if (table.getAsyncClient().getLastPropagatedTimestamp() != AsyncKuduClient.NO_TIMESTAMP) {
        builder.setPropagatedTimestamp(table.getAsyncClient().getLastPropagatedTimestamp());
      }
      GetRequestPB request = builder.build();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Sending get req: " + request.toString());
      }
      return toChannelBuffer(header, request);
    }

    @Override
    Pair<SingleRowResult, Object> deserialize(CallResponse callResponse, String tsUUID)
        throws Exception {
      GetResponsePB.Builder builder = GetResponsePB.newBuilder();
      readProtobuf(callResponse.getPBMessage(), builder);
      GetResponsePB resp = builder.build();
      TabletServerErrorPB error = resp.hasError() ? resp.getError() : null;
      if (error != null) {
        if (error.getCode().equals(TabletServerErrorPB.Code.TABLET_NOT_FOUND)) {
          // Doing this will trigger finding the new location.
          return new Pair<SingleRowResult, Object>(null, error);
        }
      }
      RowResult row = toRowResult(schema, resp.getData(), callResponse);
      SingleRowResult result = new SingleRowResult(row,
          deadlineTracker.getElapsedMillis(), tsUUID);
      if (LOG.isDebugEnabled()) {
        LOG.debug(result.toString());
      }
      return new Pair<SingleRowResult, Object>(result, error);
    }
  }

  /**
   * A Builder class to build {@link AsyncKuduGetter}.
   * Use {@link AsyncKuduClient#newGetterBuilder} in order to get a builder instance.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static class AsyncKuduGetterBuilder
      extends AbstractKuduGetterBuilder<AsyncKuduGetterBuilder, AsyncKuduGetter> {

    AsyncKuduGetterBuilder(AsyncKuduClient client, KuduTable table) {
      super(client, table);
    }

    /**
     * Builds an {@link AsyncKuduScanner} using the passed configurations.
     * @return a new {@link AsyncKuduScanner}
     */
    public AsyncKuduGetter build() {
      return new AsyncKuduGetter(client, table, projectedColumnNames,
          projectedColumnIndexes, getRequestTimeout);
    }
  }
}
