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

import java.util.ArrayList;
import java.util.List;

/** Tests that the {@link JobVertexBackPressureInfo} can be marshalled and unmarshalled. */
public class JobVertexBackPressureInfoTest
        extends RestResponseMarshallingTestBase<JobVertexBackPressureInfo> {
    @Override
    protected Class<JobVertexBackPressureInfo> getTestResponseClass() {
        return JobVertexBackPressureInfo.class;
    }

    @Override
    protected JobVertexBackPressureInfo getTestResponseInstance() throws Exception {
        List<JobVertexBackPressureInfo.SubtaskBackPressureInfo> subtaskList = new ArrayList<>();
        subtaskList.add(
                new JobVertexBackPressureInfo.SubtaskBackPressureInfo(
                        0, JobVertexBackPressureInfo.VertexBackPressureLevel.LOW, 0.1));
        subtaskList.add(
                new JobVertexBackPressureInfo.SubtaskBackPressureInfo(
                        1, JobVertexBackPressureInfo.VertexBackPressureLevel.OK, 0.4));
        subtaskList.add(
                new JobVertexBackPressureInfo.SubtaskBackPressureInfo(
                        2, JobVertexBackPressureInfo.VertexBackPressureLevel.HIGH, 0.9));
        return new JobVertexBackPressureInfo(
                JobVertexBackPressureInfo.VertexBackPressureStatus.OK,
                JobVertexBackPressureInfo.VertexBackPressureLevel.LOW,
                System.currentTimeMillis(),
                subtaskList);
    }
}
