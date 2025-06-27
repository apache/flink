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

package org.apache.flink.state.api;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;

/** IT Case for reading keyed state from a memory state backend. */
public class EmbeddedRocksDBStateBackendReaderKeyedStateITCase
        extends SavepointReaderKeyedStateITCase<EmbeddedRocksDBStateBackend> {

    @Override
    protected Tuple2<Configuration, EmbeddedRocksDBStateBackend> getStateBackendTuple() {
        return Tuple2.of(
                new Configuration().set(StateBackendOptions.STATE_BACKEND, "rocksdb"),
                new EmbeddedRocksDBStateBackend());
    }
}
