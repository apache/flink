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

import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.testutils.junit.extensions.parameterized.NoOpTestExtension;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests that the {@link JobExceptionsInfoWithHistory} can be marshalled and unmarshalled. */
@ExtendWith(NoOpTestExtension.class)
class JobExceptionsInfoWithHistoryTest
        extends RestResponseMarshallingTestBase<JobExceptionsInfoWithHistory> {
    @Override
    protected Class<JobExceptionsInfoWithHistory> getTestResponseClass() {
        return JobExceptionsInfoWithHistory.class;
    }

    @Override
    protected JobExceptionsInfoWithHistory getTestResponseInstance() throws Exception {
        return new JobExceptionsInfoWithHistory(
                new JobExceptionsInfoWithHistory.JobExceptionHistory(
                        Collections.emptyList(), false));
    }

    /**
     * {@code taskName} and {@code location} should not be exposed if not set.
     *
     * @throws JsonProcessingException is not expected to be thrown
     */
    @Test
    void testNullFieldsNotSet() throws JsonProcessingException {
        ObjectMapper objMapper = RestMapperUtils.getStrictObjectMapper();
        String json =
                objMapper.writeValueAsString(
                        new JobExceptionsInfoWithHistory.ExceptionInfo(
                                "exception name", "stacktrace", 0L));

        assertThat(json).doesNotContain("taskName");
        assertThat(json).doesNotContain("location");
    }
}
