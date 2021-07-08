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

import org.apache.flink.api.common.JobID;

/** Path parameter identifying jobs. */
public class JobIDPathParameter extends MessagePathParameter<JobID> {

    public static final String KEY = "jobid";

    public JobIDPathParameter() {
        super(KEY);
    }

    @Override
    protected JobID convertFromString(String value) {
        return JobID.fromHexString(value);
    }

    @Override
    protected String convertToString(JobID value) {
        return value.toString();
    }

    @Override
    public String getDescription() {
        return "32-character hexadecimal string value that identifies a job.";
    }
}
