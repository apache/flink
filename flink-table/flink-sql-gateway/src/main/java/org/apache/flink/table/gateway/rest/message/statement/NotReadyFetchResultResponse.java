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
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.rest.serde.ResultInfo;

import javax.annotation.Nullable;

/** The {@link FetchResultsResponseBody} indicates the results is not ready. */
public class NotReadyFetchResultResponse implements FetchResultsResponseBody {

    private final String nextResultUri;

    public NotReadyFetchResultResponse(String nextResultUri) {
        this.nextResultUri = nextResultUri;
    }

    @Override
    public ResultInfo getResults() {
        throw new SqlGatewayException(
                "The result is not ready. Please fetch results until the result type is PAYLOAD or EOS.");
    }

    @Override
    public ResultSet.ResultType getResultType() {
        return ResultSet.ResultType.NOT_READY;
    }

    @Nullable
    @Override
    public String getNextResultUri() {
        return nextResultUri;
    }

    @Override
    public boolean isQueryResult() {
        throw new SqlGatewayException(
                "Don't know whether a NOT_READY_RESULT is for a query. Please continue fetching results until the result type is PAYLOAD or EOS.");
    }

    @Nullable
    @Override
    public JobID getJobID() {
        throw new SqlGatewayException(
                "Don't know the Job ID with NOT_READY_RESULT. Please continue fetching results until the result type is PAYLOAD or EOS.");
    }

    @Override
    public ResultKind getResultKind() {
        throw new SqlGatewayException(
                "Don't know the ResultKind with NOT_READY_RESULT. Please continue fetching results until the result type is PAYLOAD or EOS.");
    }
}
