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

package org.apache.flink.runtime.messages.webmonitor;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.ApplicationState;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.messages.GenericMessageTester;
import org.apache.flink.runtime.rest.messages.RestResponseMarshallingTestBase;
import org.apache.flink.testutils.junit.extensions.parameterized.NoOpTestExtension;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Random;

/** Tests (un)marshalling for the {@link ApplicationDetailsInfo}. */
@ExtendWith(NoOpTestExtension.class)
class ApplicationDetailsInfoTest extends RestResponseMarshallingTestBase<ApplicationDetailsInfo> {

    @Override
    protected Class<ApplicationDetailsInfo> getTestResponseClass() {
        return ApplicationDetailsInfo.class;
    }

    @Override
    protected ApplicationDetailsInfo getTestResponseInstance() throws Exception {
        final Random rnd = new Random();

        Map<String, Long> timestamps =
                CollectionUtil.newHashMapWithExpectedSize(ApplicationState.values().length);

        for (ApplicationState status : ApplicationState.values()) {
            timestamps.put(status.toString(), rnd.nextLong());
        }

        long time = rnd.nextLong();
        long endTime = rnd.nextBoolean() ? -1L : time + rnd.nextInt();

        String name = GenericMessageTester.randomString(rnd);
        ApplicationID id = GenericMessageTester.randomApplicationId(rnd);
        ApplicationState status = GenericMessageTester.randomApplicationState(rnd);
        Collection<JobDetails> jobs = randomJobDetails(rnd);

        return new ApplicationDetailsInfo(
                id, name, status.toString(), time, endTime, endTime - time, timestamps, jobs);
    }

    private Collection<JobDetails> randomJobDetails(Random rnd) {
        final JobDetails[] details = new JobDetails[rnd.nextInt(10)];
        for (int k = 0; k < details.length; k++) {
            int[] numVerticesPerState = new int[ExecutionState.values().length];
            int numTotal = 0;

            for (int i = 0; i < numVerticesPerState.length; i++) {
                int count = rnd.nextInt(55);
                numVerticesPerState[i] = count;
                numTotal += count;
            }

            long time = rnd.nextLong();
            long endTime = rnd.nextBoolean() ? -1L : time + rnd.nextInt();
            long lastModified = endTime == -1 ? time + rnd.nextInt() : endTime;

            String name = new GenericMessageTester.StringInstantiator().instantiate(rnd);
            JobID jid = new JobID();
            JobStatus status = JobStatus.values()[rnd.nextInt(JobStatus.values().length)];

            details[k] =
                    new JobDetails(
                            jid,
                            name,
                            time,
                            endTime,
                            endTime - time,
                            status,
                            lastModified,
                            numVerticesPerState,
                            numTotal);
        }
        return Arrays.asList(details);
    }
}
