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

package org.apache.flink.table.gateway.rest.message.statement;

import org.apache.flink.api.common.JobID;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.rest.serde.ResultInfo;

import javax.annotation.Nullable;

/** The implementation of the {@link FetchResultsResponseBody} with ready results. */
public class FetchResultResponseBodyImpl implements FetchResultsResponseBody {

    private final ResultInfo results;
    private final ResultSet.ResultType resultType;
    @Nullable private final String nextResultUri;
    private final boolean isQueryResult;
    @Nullable private final JobID jobID;
    private final ResultKind resultKind;

    public FetchResultResponseBodyImpl(
            ResultSet.ResultType resultType,
            boolean isQueryResult,
            @Nullable JobID jobID,
            ResultKind resultKind,
            ResultInfo results,
            @Nullable String nextResultUri) {
        this.results = results;
        this.resultType = resultType;
        this.nextResultUri = nextResultUri;
        this.isQueryResult = isQueryResult;
        this.jobID = jobID;
        this.resultKind = resultKind;
    }

    @Override
    public ResultInfo getResults() {
        return results;
    }

    @Override
    public ResultSet.ResultType getResultType() {
        return resultType;
    }

    @Nullable
    @Override
    public String getNextResultUri() {
        return nextResultUri;
    }

    @Override
    public boolean isQueryResult() {
        return isQueryResult;
    }

    @Nullable
    @Override
    public JobID getJobID() {
        return jobID;
    }

    @Override
    public ResultKind getResultKind() {
        return resultKind;
    }
}
