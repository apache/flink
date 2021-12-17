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

package org.apache.flink.runtime.entrypoint;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Properties;

/** Basic {@link ClusterConfiguration} for entry points. */
public class EntrypointClusterConfiguration extends ClusterConfiguration {

    @Nullable private final String hostname;

    private final int restPort;

    public EntrypointClusterConfiguration(
            @Nonnull String configDir,
            @Nonnull Properties dynamicProperties,
            @Nonnull String[] args,
            @Nullable String hostname,
            int restPort) {
        super(configDir, dynamicProperties, args);
        this.hostname = hostname;
        this.restPort = restPort;
    }

    public int getRestPort() {
        return restPort;
    }

    @Nullable
    public String getHostname() {
        return hostname;
    }
}
