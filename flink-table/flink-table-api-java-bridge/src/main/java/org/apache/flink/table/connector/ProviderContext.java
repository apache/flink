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

package org.apache.flink.table.connector;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.connector.sink.DynamicTableSink.SinkRuntimeProvider;
import org.apache.flink.table.connector.source.ScanTableSource.ScanRuntimeProvider;

import java.util.Optional;

/**
 * Context provided by some {@link ScanRuntimeProvider} and {@link SinkRuntimeProvider} for
 * generating the runtime objects.
 */
@PublicEvolving
public interface ProviderContext {

    /**
     * Generates a new unique id for a Transformation/DataStream operator. The {@code name} must be
     * unique across the provider implementation.
     *
     * <p>This method returns empty if no uid cannot be generated, i.e. because the job is bounded.
     */
    Optional<String> generateUid(String name);
}
