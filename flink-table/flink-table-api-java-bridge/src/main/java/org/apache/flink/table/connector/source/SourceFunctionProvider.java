/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.connector.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.RowData;

/**
 * Provider of a {@link SourceFunction} instance as a runtime implementation for {@link
 * ScanTableSource}.
 *
 * @deprecated This interface is based on the {@link
 *     org.apache.flink.streaming.api.functions.source.SourceFunction} API, which is due to be
 *     removed. Use {@link org.apache.flink.table.connector.source.SourceProvider} instead.
 */
@Deprecated
@PublicEvolving
public interface SourceFunctionProvider extends ScanTableSource.ScanRuntimeProvider {

    /** Helper method for creating a static provider. */
    static SourceFunctionProvider of(SourceFunction<RowData> sourceFunction, boolean isBounded) {
        return new SourceFunctionProvider() {
            @Override
            public SourceFunction<RowData> createSourceFunction() {
                return sourceFunction;
            }

            @Override
            public boolean isBounded() {
                return isBounded;
            }
        };
    }

    /** Creates a {@link SourceFunction} instance. */
    SourceFunction<RowData> createSourceFunction();
}
