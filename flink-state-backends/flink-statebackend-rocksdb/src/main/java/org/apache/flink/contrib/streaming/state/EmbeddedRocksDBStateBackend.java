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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.util.TernaryBoolean;

/**
 * @deprecated This class has been moved to {@link
 *     org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend}. Please use the one under the new
 *     package instead.
 */
@Deprecated
@PublicEvolving
public class EmbeddedRocksDBStateBackend
        extends org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend {

    public EmbeddedRocksDBStateBackend() {
        super();
    }

    public EmbeddedRocksDBStateBackend(boolean enableIncrementalCheckpointing) {
        super(enableIncrementalCheckpointing);
    }

    public EmbeddedRocksDBStateBackend(TernaryBoolean enableIncrementalCheckpointing) {
        super(enableIncrementalCheckpointing);
    }

    private EmbeddedRocksDBStateBackend(
            EmbeddedRocksDBStateBackend original, ReadableConfig config, ClassLoader classLoader) {
        super(original, config, classLoader);
    }

    /**
     * Creates a copy of this state backend that uses the values defined in the configuration for
     * fields where that were not yet specified in this state backend.
     *
     * @param config The configuration.
     * @param classLoader The class loader.
     * @return The re-configured variant of the state backend
     */
    @Override
    public org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend configure(
            ReadableConfig config, ClassLoader classLoader) {
        return new org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend(
                this, config, classLoader);
    }
}
