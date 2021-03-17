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

package org.apache.flink.state.changelog;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import java.util.Collections;

/** Test Utilities for Changelog StateBackend. */
public class ChangelogStateBackendTestUtils {

    public static <K> CheckpointableKeyedStateBackend<K> createKeyedBackend(
            StateBackend stateBackend,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            Environment env)
            throws Exception {

        return stateBackend.createKeyedStateBackend(
                env,
                new JobID(),
                "test_op",
                keySerializer,
                numberOfKeyGroups,
                keyGroupRange,
                env.getTaskKvStateRegistry(),
                TtlTimeProvider.DEFAULT,
                new UnregisteredMetricsGroup(),
                Collections.emptyList(),
                new CloseableRegistry());
    }

    public static CheckpointableKeyedStateBackend<Integer> createKeyedBackend(
            StateBackend stateBackend, Environment env) throws Exception {

        return createKeyedBackend(
                stateBackend, IntSerializer.INSTANCE, 10, new KeyGroupRange(0, 9), env);
    }
}
