/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.rest.messages.taskmanager;

import org.apache.flink.runtime.rest.messages.LogInfo;
import org.apache.flink.runtime.rest.messages.LogListInfo;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.RestResponseMarshallingTestBase;
import org.apache.flink.testutils.junit.extensions.parameterized.NoOpTestExtension;

import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;

/** Tests for (un)marshalling of {@link LogListInfo}. */
@ExtendWith(NoOpTestExtension.class)
class LogListInfoTest extends RestResponseMarshallingTestBase {

    @Override
    protected Class getTestResponseClass() {
        return LogListInfo.class;
    }

    @Override
    protected ResponseBody getTestResponseInstance() throws Exception {
        return new LogListInfo(
                Arrays.asList(
                        new LogInfo("taskmanager.log", 0, 1632844800000L),
                        new LogInfo("taskmanager.out", Integer.MAX_VALUE, 1632844800000L)));
    }
}
