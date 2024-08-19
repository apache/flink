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

package org.apache.flink.fs.gs;

import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link GSFileSystemFactory}. */
class GSFileSystemFactoryTest {

    @Test
    public void testOverrideStorageRootUrl() {
        Configuration flinkConfig = new Configuration();
        flinkConfig.setString("gs.storage.root.url", "http://240.0.0.0:12345");

        GSFileSystemFactory factory = new GSFileSystemFactory();
        factory.configure(flinkConfig);

        String gsStorageClientHost = factory.getStorage().getOptions().getHost();
        assertThat(gsStorageClientHost).isEqualTo("http://240.0.0.0:12345");
    }
}
