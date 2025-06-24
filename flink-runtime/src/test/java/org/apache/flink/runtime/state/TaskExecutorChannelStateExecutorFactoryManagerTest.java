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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteRequestExecutorFactory;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TaskExecutorChannelStateExecutorFactoryManager}. */
public class TaskExecutorChannelStateExecutorFactoryManagerTest {

    @Test
    void testReuseFactory() {
        TaskExecutorChannelStateExecutorFactoryManager manager =
                new TaskExecutorChannelStateExecutorFactoryManager();

        JobID jobID = new JobID();
        ChannelStateWriteRequestExecutorFactory factory = manager.getOrCreateExecutorFactory(jobID);
        assertThat(manager.getOrCreateExecutorFactory(jobID))
                .as("Same job should share the executor factory.")
                .isSameAs(factory);

        assertThat(manager.getOrCreateExecutorFactory(new JobID()))
                .as("Different jobs cannot share executor factory.")
                .isNotSameAs(factory);
        manager.shutdown();
    }

    @Test
    void testReleaseForJob() {
        TaskExecutorChannelStateExecutorFactoryManager manager =
                new TaskExecutorChannelStateExecutorFactoryManager();

        JobID jobID = new JobID();
        assertThat(manager.getFactoryByJobId(jobID)).isNull();
        manager.getOrCreateExecutorFactory(jobID);
        assertThat(manager.getFactoryByJobId(jobID)).isNotNull();

        manager.releaseResourcesForJob(jobID);
        assertThat(manager.getFactoryByJobId(jobID)).isNull();
        manager.shutdown();
    }

    @Test
    void testShutdown() {
        TaskExecutorChannelStateExecutorFactoryManager manager =
                new TaskExecutorChannelStateExecutorFactoryManager();

        JobID jobID = new JobID();
        manager.getOrCreateExecutorFactory(jobID);
        manager.shutdown();

        assertThatThrownBy(() -> manager.getOrCreateExecutorFactory(jobID))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> manager.getOrCreateExecutorFactory(new JobID()))
                .isInstanceOf(IllegalStateException.class);
    }
}
