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

package org.apache.flink.formats.parquet.utils;

import org.apache.hadoop.conf.Configuration;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;

import static org.apache.flink.util.InstantiationUtil.deserializeObject;
import static org.apache.flink.util.InstantiationUtil.serializeObject;

/** Test for {@link SerializableConfiguration}. */
class SerializableConfigurationTest {

    @Test
    void testResource() throws IOException, ClassNotFoundException {
        ClassLoader cl =
                new ClassLoader(Thread.currentThread().getContextClassLoader()) {
                    @Nullable
                    @Override
                    public URL getResource(String name) {
                        throw new RuntimeException();
                    }
                };
        SerializableConfiguration conf =
                deserializeObject(
                        serializeObject(new SerializableConfiguration(new Configuration())), cl);
        conf.conf().getInt("one-key", 0);
    }
}
