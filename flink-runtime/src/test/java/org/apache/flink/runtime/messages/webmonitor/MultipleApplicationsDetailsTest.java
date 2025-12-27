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
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.messages.GenericMessageTester;
import org.apache.flink.runtime.rest.messages.RestResponseMarshallingTestBase;
import org.apache.flink.testutils.junit.extensions.parameterized.NoOpTestExtension;

import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/** Tests (un)marshalling for the {@link MultipleApplicationsDetails} class. */
@ExtendWith(NoOpTestExtension.class)
class MultipleApplicationsDetailsTest
        extends RestResponseMarshallingTestBase<MultipleApplicationsDetails> {

    @Override
    protected Class<MultipleApplicationsDetails> getTestResponseClass() {
        return MultipleApplicationsDetails.class;
    }

    @Override
    protected MultipleApplicationsDetails getTestResponseInstance() throws Exception {
        return new MultipleApplicationsDetails(randomApplicationDetails(new Random()));
    }

    private Collection<ApplicationDetails> randomApplicationDetails(Random rnd) {
        final ApplicationDetails[] details = new ApplicationDetails[rnd.nextInt(10)];
        for (int k = 0; k < details.length; k++) {
            Map<String, Integer> jobInfo = new HashMap<>();

            for (int i = 0; i < JobStatus.values().length; i++) {
                int count = rnd.nextInt(55);
                jobInfo.put(JobStatus.values()[i].name(), count);
            }

            long time = rnd.nextLong();
            long endTime = rnd.nextBoolean() ? -1L : time + rnd.nextInt();

            String name = GenericMessageTester.randomString(rnd);
            ApplicationID id = GenericMessageTester.randomApplicationId(rnd);
            ApplicationState status = GenericMessageTester.randomApplicationState(rnd);

            details[k] =
                    new ApplicationDetails(
                            id, name, time, endTime, endTime - time, status.toString(), jobInfo);
        }
        return Arrays.asList(details);
    }
}
