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

package org.apache.flink.runtime.rest.messages.taskmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.resourcemanager.TaskManagerInfoWithSlots;
import org.apache.flink.runtime.rest.messages.RestResponseMarshallingTestBase;
import org.apache.flink.testutils.junit.extensions.parameterized.NoOpTestExtension;

import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/** Tests (un)marshalling of {@link TaskManagerDetailsInfo}. */
@ExtendWith(NoOpTestExtension.class)
class TaskManagerDetailsInfoTest extends RestResponseMarshallingTestBase<TaskManagerDetailsInfo> {

    private static final Random random = new Random();

    @Override
    protected Class<TaskManagerDetailsInfo> getTestResponseClass() {
        return TaskManagerDetailsInfo.class;
    }

    @Override
    protected TaskManagerDetailsInfo getTestResponseInstance() throws Exception {
        final TaskManagerInfoWithSlots taskManagerInfoWithSlots =
                new TaskManagerInfoWithSlots(
                        TaskManagerInfoTest.createRandomTaskManagerInfo(),
                        Collections.singletonList(new SlotInfo(new JobID(), ResourceProfile.ANY)));
        final TaskManagerMetricsInfo taskManagerMetricsInfo = createRandomTaskManagerMetricsInfo();

        return new TaskManagerDetailsInfo(taskManagerInfoWithSlots, taskManagerMetricsInfo);
    }

    static TaskManagerMetricsInfo createRandomTaskManagerMetricsInfo() {
        final List<TaskManagerMetricsInfo.GarbageCollectorInfo> garbageCollectorsInfo =
                createRandomGarbageCollectorsInfo();

        return new TaskManagerMetricsInfo(
                random.nextLong(),
                random.nextLong(),
                random.nextLong(),
                random.nextLong(),
                random.nextLong(),
                random.nextLong(),
                random.nextLong(),
                random.nextLong(),
                random.nextLong(),
                random.nextLong(),
                random.nextLong(),
                random.nextLong(),
                random.nextLong(),
                random.nextLong(),
                random.nextLong(),
                random.nextLong(),
                random.nextLong(),
                random.nextLong(),
                garbageCollectorsInfo);
    }

    static List<TaskManagerMetricsInfo.GarbageCollectorInfo> createRandomGarbageCollectorsInfo() {
        final int numberGCs = random.nextInt(10);
        final List<TaskManagerMetricsInfo.GarbageCollectorInfo> garbageCollectorInfos =
                new ArrayList<>(numberGCs);

        for (int i = 0; i < numberGCs; i++) {
            garbageCollectorInfos.add(
                    new TaskManagerMetricsInfo.GarbageCollectorInfo(
                            UUID.randomUUID().toString(), random.nextLong(), random.nextLong()));
        }

        return garbageCollectorInfos;
    }
}
