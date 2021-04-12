/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.configuration.ReadableConfig;

/** An interface for options factory that pick up additional parameters from a configuration. */
public interface ConfigurableRocksDBOptionsFactory extends RocksDBOptionsFactory {

    /**
     * Creates a variant of the options factory that applies additional configuration parameters.
     *
     * <p>If no configuration is applied, or if the method directly applies configuration values to
     * the (mutable) options factory object, this method may return the original options factory
     * object. Otherwise it typically returns a modified copy.
     *
     * @param configuration The configuration to pick the values from.
     * @return A reconfigured options factory.
     */
    RocksDBOptionsFactory configure(ReadableConfig configuration);
}
