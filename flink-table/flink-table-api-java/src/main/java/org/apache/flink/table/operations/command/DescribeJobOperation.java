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
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.operations.ExecutableOperation;
import org.apache.flink.table.operations.Operation;

/** Operation to describe a DESCRIBE JOB statement. */
@Internal
public class DescribeJobOperation implements Operation, ExecutableOperation {

    private final String jobId;

    public DescribeJobOperation(String jobId) {
        this.jobId = jobId;
    }

    public String getJobId() {
        return jobId;
    }

    @Override
    public String asSummaryString() {
        return String.format("DESCRIBE JOB '%s'", jobId);
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        // TODO: We may need to migrate the execution for ShowJobsOperation from SQL Gateway
        //  OperationExecutor to here.
        throw new UnsupportedOperationException(
                "DescribeJobOperation does not support ExecutableOperation yet.");
    }
}
