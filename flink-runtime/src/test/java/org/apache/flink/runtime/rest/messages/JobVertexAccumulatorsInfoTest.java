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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.runtime.rest.messages.job.UserAccumulator;

import java.util.ArrayList;
import java.util.List;

/** Tests that the {@link JobVertexAccumulatorsInfo} can be marshalled and unmarshalled. */
public class JobVertexAccumulatorsInfoTest
        extends RestResponseMarshallingTestBase<JobVertexAccumulatorsInfo> {
    @Override
    protected Class<JobVertexAccumulatorsInfo> getTestResponseClass() {
        return JobVertexAccumulatorsInfo.class;
    }

    @Override
    protected JobVertexAccumulatorsInfo getTestResponseInstance() throws Exception {
        List<UserAccumulator> userAccumulatorList = new ArrayList<>(3);
        userAccumulatorList.add(new UserAccumulator("test name1", "test type1", "test value1"));
        userAccumulatorList.add(new UserAccumulator("test name2", "test type2", "test value2"));
        userAccumulatorList.add(new UserAccumulator("test name3", "test type3", "test value3"));

        return new JobVertexAccumulatorsInfo("testId", userAccumulatorList);
    }
}
