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

package org.apache.flink.table.gateway.service.result;

import org.apache.flink.api.common.JobID;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.gateway.api.results.ResultSet;

import java.util.Collections;
import java.util.List;

/** To represent that the execution result is not ready to fetch. */
public class NotReadyResult implements ResultSet {

    @Override
    public ResultType getResultType() {
        return ResultType.NOT_READY;
    }

    @Override
    public Long getNextToken() {
        return 0L;
    }

    @Override
    public ResolvedSchema getResultSchema() {
        return ResolvedSchema.of(Collections.emptyList());
    }

    @Override
    public List<RowData> getData() {
        return Collections.emptyList();
    }

    @Override
    public boolean isQueryResult() {
        throw new UnsupportedOperationException(
                "Can't know whether a NOT_READY_RESULT is for a query.");
    }

    @Override
    public JobID getJobID() {
        throw new UnsupportedOperationException("Can't get job ID from a NOT_READY_RESULT.");
    }

    @Override
    public ResultKind getResultKind() {
        throw new UnsupportedOperationException("Can't get result kind from a NOT_READY_RESULT.");
    }
}
