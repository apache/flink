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

package org.apache.flink.runtime.testutils;

import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.core.testutils.CustomExtension;
import org.apache.flink.runtime.minicluster.MiniCluster;

import org.junit.jupiter.api.extension.ExtensionContext;

import java.net.URI;

/** An extension which starts a {@link MiniCluster} for testing purposes. */
public class MiniClusterExtension implements CustomExtension {
    private final MiniClusterResource miniClusterResource;

    public MiniClusterExtension(
            final MiniClusterResourceConfiguration miniClusterResourceConfiguration) {
        this.miniClusterResource = new MiniClusterResource(miniClusterResourceConfiguration);
    }

    public int getNumberSlots() {
        return miniClusterResource.getNumberSlots();
    }

    public MiniCluster getMiniCluster() {
        return miniClusterResource.getMiniCluster();
    }

    public UnmodifiableConfiguration getClientConfiguration() {
        return miniClusterResource.getClientConfiguration();
    }

    public URI getRestAddres() {
        return miniClusterResource.getRestAddres();
    }

    @Override
    public void before(ExtensionContext context) throws Exception {
        miniClusterResource.before();
    }

    @Override
    public void after(ExtensionContext context) throws Exception {
        miniClusterResource.after();
    }
}
