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

import java.util.List;

import com.google.common.collect.ImmutableList;

import org.kududb.annotations.InterfaceAudience;
import org.kududb.annotations.InterfaceStability;

/**
 * Abstract class to extend in order to create builders for getters.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class AbstractKuduGetterBuilder
    <S extends AbstractKuduGetterBuilder<? super S, T>, T> {
  protected final AsyncKuduClient client;
  protected final KuduTable table;
  protected List<String> projectedColumnNames = null;
  protected List<Integer> projectedColumnIndexes = null;
  protected long getRequestTimeout;

  AbstractKuduGetterBuilder(AsyncKuduClient client, KuduTable table) {
    this.client = client;
    this.table = table;
    this.getRequestTimeout = client.getDefaultOperationTimeoutMs();
  }

  /**
   * Set which columns will be read by the Getter.
   * Calling this method after {@link #setProjectedColumnIndexes(List)} will reset the projected
   * columns to those specified in {@code columnNames}.
   * @param columnNames the names of columns to read, or 'null' to read all columns
   * (the default)
   */
  public S setProjectedColumnNames(List<String> columnNames) {
    projectedColumnIndexes = null;
    if (columnNames != null) {
      projectedColumnNames = ImmutableList.copyOf(columnNames);
    } else {
      projectedColumnNames = null;
    }
    return (S) this;
  }

  /**
   * Set which columns will be read by the Getter.
   * Calling this method after {@link #setProjectedColumnNames(List)} will reset the projected
   * columns to those specified in {@code columnIndexes}.
   * @param columnIndexes the indexes of columns to read, or 'null' to read all columns
   * (the default)
   */
  public S setProjectedColumnIndexes(List<Integer> columnIndexes) {
    projectedColumnNames = null;
    if (columnIndexes != null) {
      projectedColumnIndexes = ImmutableList.copyOf(columnIndexes);
    } else {
      projectedColumnIndexes = null;
    }
    return (S) this;
  }

  /**
   * Sets how long each get request to a server can last.
   * Defaults to {@link KuduClient#getDefaultOperationTimeoutMs()}.
   * @param getRequestTimeout a long representing time in milliseconds
   * @return this instance
   */
  public S getRequestTimeout(long getRequestTimeout) {
    this.getRequestTimeout = getRequestTimeout;
    return (S) this;
  }

  public abstract T build();
}
