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

package org.apache.flink.state.forst;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateBackendParametersImpl;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import java.io.IOException;
import java.util.Collections;

/** Test utils for the ForSt state backend. */
public final class ForStTestUtils {
    public static <K> ForStKeyedStateBackend<K> createKeyedStateBackend(
            ForStStateBackend forStStateBackend, Environment env, TypeSerializer<K> keySerializer)
            throws IOException {

        return forStStateBackend.createAsyncKeyedStateBackend(
                new KeyedStateBackendParametersImpl<>(
                        env,
                        env.getJobID(),
                        "test_op",
                        keySerializer,
                        1,
                        new KeyGroupRange(0, 0),
                        env.getTaskKvStateRegistry(),
                        TtlTimeProvider.DEFAULT,
                        new UnregisteredMetricsGroup(),
                        (name, value) -> {},
                        Collections.emptyList(),
                        new CloseableRegistry(),
                        1.0));
    }
}
