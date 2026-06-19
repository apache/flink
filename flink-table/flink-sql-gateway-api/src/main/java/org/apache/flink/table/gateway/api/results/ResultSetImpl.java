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

package org.apache.flink.table.gateway.api.results;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.utils.print.RowDataToStringConverter;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** An implementation of {@link ResultSet}. */
@Internal
public class ResultSetImpl implements ResultSet {

    private final ResultType resultType;

    @Nullable private final Long nextToken;

    private final ResolvedSchema resultSchema;
    private final List<RowData> data;
    private final RowDataToStringConverter converter;

    private final boolean isQueryResult;

    @Nullable private final JobID jobID;

    private final ResultKind resultKind;

    public ResultSetImpl(
            ResultType resultType,
            @Nullable Long nextToken,
            ResolvedSchema resultSchema,
            List<RowData> data,
            RowDataToStringConverter converter,
            boolean isQueryResult,
            @Nullable JobID jobID,
            ResultKind resultKind) {
        this.nextToken = nextToken;
        this.resultType = resultType;
        this.resultSchema = resultSchema;
        this.data = data;
        this.converter = converter;
        this.isQueryResult = isQueryResult;
        this.jobID = jobID;
        this.resultKind = resultKind;
    }

    @Override
    public ResultType getResultType() {
        return resultType;
    }

    @Override
    public @Nullable Long getNextToken() {
        return nextToken;
    }

    @Override
    public ResolvedSchema getResultSchema() {
        return resultSchema;
    }

    @Override
    public List<RowData> getData() {
        return data;
    }

    public RowDataToStringConverter getConverter() {
        return converter;
    }

    @Override
    public boolean isQueryResult() {
        return isQueryResult;
    }

    @Override
    public JobID getJobID() {
        return jobID;
    }

    @Override
    public ResultKind getResultKind() {
        return resultKind;
    }

    @Override
    public String toString() {
        return String.format(
                "ResultSet{\n"
                        + "  resultType=%s,\n"
                        + "  nextToken=%s,\n"
                        + "  resultSchema=%s,\n"
                        + "  data=[%s],\n"
                        + "  isQueryResult=%s,\n"
                        + "  jobId=%s,\n"
                        + "  resultKind=%s\n"
                        + "}",
                resultType,
                nextToken,
                resultSchema.toString(),
                data.stream().map(Object::toString).collect(Collectors.joining(",")),
                isQueryResult,
                jobID,
                resultKind);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ResultSetImpl)) {
            return false;
        }
        ResultSetImpl resultSet = (ResultSetImpl) o;
        return resultType == resultSet.resultType
                && Objects.equals(nextToken, resultSet.nextToken)
                && Objects.equals(resultSchema, resultSet.resultSchema)
                && Objects.equals(data, resultSet.data)
                && isQueryResult == resultSet.isQueryResult
                && Objects.equals(jobID, resultSet.jobID)
                && resultKind == resultSet.resultKind;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                resultType, nextToken, resultSchema, data, isQueryResult, jobID, resultKind);
    }
}
