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

package org.apache.flink.table.operations.command;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.operations.Operation;

/** Operation to stop a running job. */
@Internal
public class StopJobOperation implements Operation {

    private final String jobId;

    private final boolean isWithSavepoint;

    private final boolean isWithDrain;

    public StopJobOperation(String jobId, boolean isWithSavepoint, boolean isWithDrain) {
        this.jobId = jobId;
        this.isWithSavepoint = isWithSavepoint;
        this.isWithDrain = isWithDrain;
    }

    public String getJobId() {
        return jobId;
    }

    public boolean isWithSavepoint() {
        return isWithSavepoint;
    }

    public boolean isWithDrain() {
        return isWithDrain;
    }

    @Override
    public String asSummaryString() {
        StringBuilder summary = new StringBuilder("STOP JOB ");
        summary.append("'").append(jobId).append("'");
        if (isWithSavepoint) {
            summary.append(" WITH SAVEPOINT");
        }
        if (isWithDrain) {
            summary.append(" WITH DRAIN");
        }
        return summary.toString();
    }
}
