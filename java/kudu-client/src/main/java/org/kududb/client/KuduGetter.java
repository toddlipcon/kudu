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

import com.stumbleupon.async.Deferred;
import org.kududb.annotations.InterfaceAudience;
import org.kududb.annotations.InterfaceStability;

/**
 * Synchronous version of {@link AsyncKuduGetter}. Offers the same API but with blocking methods.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class KuduGetter {
  private final AsyncKuduGetter asyncGetter;

  KuduGetter(AsyncKuduGetter asyncGetter) {
    this.asyncGetter = asyncGetter;
  }

  public SingleRowResult get(PartialRow row) throws Exception {
    Deferred<SingleRowResult> d = asyncGetter.get(row);
    return d.join(asyncGetter.getRequestTimeout);
  }

  /**
   * A Builder class to build {@link KuduGetter}.
   * Use {@link KuduClient#newGetterBuilder} in order to get a builder instance.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static class KuduGetterBuilder
      extends AbstractKuduGetterBuilder<KuduGetterBuilder, KuduGetter> {

    KuduGetterBuilder(AsyncKuduClient client, KuduTable table) {
      super(client, table);
    }

    /**
     * Builds an {@link AsyncKuduScanner} using the passed configurations.
     * @return a new {@link AsyncKuduScanner}
     */
    public KuduGetter build() {
      return new KuduGetter(new AsyncKuduGetter(client, table,
          projectedColumnNames, projectedColumnIndexes, getRequestTimeout));
    }
  }
}
