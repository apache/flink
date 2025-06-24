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

package org.apache.flink.table.client.gateway;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.gateway.rest.message.statement.FetchResultsResponseBody;
import org.apache.flink.table.utils.print.RowDataToStringConverter;
import org.apache.flink.util.CloseableIterator;

import javax.annotation.Nullable;

import static org.apache.flink.table.api.internal.StaticResultProvider.SIMPLE_ROW_DATA_TO_STRING_CONVERTER;

/** Wrapped results for the {@link FetchResultsResponseBody}. */
public class StatementResult implements CloseableIterator<RowData> {

    private final ResolvedSchema resultSchema;
    private final CloseableIterator<RowData> resultProvider;
    private final boolean isQueryResult;
    private final ResultKind resultKind;
    private final @Nullable JobID jobID;
    private final RowDataToStringConverter toStringConverter;

    public StatementResult(
            ResolvedSchema resultSchema,
            CloseableIterator<RowData> resultProvider,
            boolean isQueryResult,
            ResultKind resultKind,
            @Nullable JobID jobID) {
        this(
                resultSchema,
                resultProvider,
                isQueryResult,
                resultKind,
                jobID,
                SIMPLE_ROW_DATA_TO_STRING_CONVERTER);
    }

    @VisibleForTesting
    public StatementResult(
            ResolvedSchema resultSchema,
            CloseableIterator<RowData> resultProvider,
            boolean isQueryResult,
            ResultKind resultKind,
            @Nullable JobID jobID,
            RowDataToStringConverter toStringConverter) {
        this.resultSchema = resultSchema;
        this.resultProvider = resultProvider;
        this.isQueryResult = isQueryResult;
        this.resultKind = resultKind;
        this.jobID = jobID;
        this.toStringConverter = toStringConverter;
    }

    public ResolvedSchema getResultSchema() {
        return resultSchema;
    }

    public boolean isQueryResult() {
        return isQueryResult;
    }

    public @Nullable JobID getJobId() {
        return jobID;
    }

    public ResultKind getResultKind() {
        return resultKind;
    }

    public RowDataToStringConverter getRowDataToStringConverter() {
        return toStringConverter;
    }

    @Override
    public void close() {
        try {
            resultProvider.close();
        } catch (Exception e) {
            // ignore
        }
    }

    @Override
    public boolean hasNext() {
        return resultProvider.hasNext();
    }

    @Override
    public RowData next() {
        return resultProvider.next();
    }
}
