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

package org.apache.flink.yarn;

import org.apache.flink.util.FlinkException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link YarnLocalResourceDescriptor}. */
class YarnLocalResourceDescriptionTest {

    private final String key = "fli'nk 2.jar";
    private final Path path = new Path("hdfs://nn/tmp/fli'nk 2.jar");
    private final long size = 100 * 1024 * 1024;
    private final long ts = System.currentTimeMillis();

    @Test
    void testFromString() throws Exception {
        final YarnLocalResourceDescriptor localResourceDesc =
                new YarnLocalResourceDescriptor(
                        key,
                        path,
                        size,
                        ts,
                        LocalResourceVisibility.PUBLIC,
                        LocalResourceType.FILE);

        final String desc = localResourceDesc.toString();
        YarnLocalResourceDescriptor newLocalResourceDesc =
                YarnLocalResourceDescriptor.fromString(desc);
        assertThat(newLocalResourceDesc.getResourceKey()).isEqualTo(key);
        assertThat(newLocalResourceDesc.getPath()).isEqualTo(path);
        assertThat(newLocalResourceDesc.getSize()).isEqualTo(size);
        assertThat(newLocalResourceDesc.getModificationTime()).isEqualTo(ts);
        assertThat(newLocalResourceDesc.getVisibility()).isEqualTo(LocalResourceVisibility.PUBLIC);
        assertThat(newLocalResourceDesc.getResourceType()).isEqualTo(LocalResourceType.FILE);
    }

    @Test
    void testFromStringMalformed() {
        final String desc =
                String.format(
                        "{'resourceKey':'%s','path':'%s','size':%s,'modificationTime':%s,'visibility':'%s'}",
                        key, path, size, ts, LocalResourceVisibility.PUBLIC);
        assertThrows(desc);
        assertThrows("{}");
        assertThrows("{");
    }

    private void assertThrows(final String desc) {
        assertThatThrownBy(() -> YarnLocalResourceDescriptor.fromString(desc))
                .isInstanceOf(FlinkException.class)
                .hasMessageContaining("Error to parse YarnLocalResourceDescriptor from " + desc);
    }
}
