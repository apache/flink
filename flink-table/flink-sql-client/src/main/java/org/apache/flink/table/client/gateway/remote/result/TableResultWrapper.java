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

package org.apache.flink.table.client.gateway.remote.result;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.client.gateway.remote.RemoteExecutor;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.rest.message.statement.FetchResultsResponseBody;
import org.apache.flink.table.utils.print.RowDataToStringConverter;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/** To wrap the result returned by {@link RemoteExecutor#executeStatement}. */
public class TableResultWrapper implements TableResultInternal {

    private final ResolvedSchema resolvedSchema;
    private final CloseableIterator<RowData> dataIterator;

    private String resultId;
    private boolean isMaterialized = false;
    private ReadableConfig config;

    public TableResultWrapper(
            RemoteExecutor executor,
            OperationHandle operationHandle,
            ResultSet firstResult,
            Long nextToken) {
        this.resolvedSchema = firstResult.getResultSchema();
        dataIterator =
                new RowDataIterator(executor, operationHandle, firstResult.getData(), nextToken);
    }

    public void setResultId(String resultId) {
        this.resultId = resultId;
    }

    public String getResultId() {
        return resultId;
    }

    public void setMaterialized(boolean isMaterialized) {
        this.isMaterialized = isMaterialized;
    }

    public boolean isMaterialized() {
        return isMaterialized;
    }

    public void setConfig(ReadableConfig config) {
        this.config = config;
    }

    public ReadableConfig getConfig() {
        return config;
    }

    /** Cannot get job client through SQL Gateway. */
    @Override
    public Optional<JobClient> getJobClient() {
        return Optional.empty();
    }

    @Override
    public void await() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void await(long timeout, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResolvedSchema getResolvedSchema() {
        return resolvedSchema;
    }

    @Override
    public ResultKind getResultKind() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CloseableIterator<Row> collect() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void print() {
        throw new UnsupportedOperationException();
    }

    /** Returns an iterator that returns the iterator with the internal row data type. */
    @Override
    public CloseableIterator<RowData> collectInternal() {
        return dataIterator;
    }

    @Override
    public RowDataToStringConverter getRowDataToStringConverter() {
        // todo
        return rowData -> new String[] {"FAKE TEST RETURN"};
    }

    // --------------------------------------------------------------------------------------------

    private static class RowDataIterator implements CloseableIterator<RowData> {

        private final RemoteExecutor executor;
        private final OperationHandle operationHandle;
        private Iterator<RowData> currentData;

        private Long nextToken;

        public RowDataIterator(
                RemoteExecutor executor,
                OperationHandle operationHandle,
                List<RowData> data,
                Long nextToken) {
            this.executor = executor;
            this.operationHandle = operationHandle;
            this.currentData = data.iterator();
            this.nextToken = nextToken;
        }

        @Override
        public void close() {
            // do nothing
        }

        // todo: not sure whether if this is correct
        @Override
        public boolean hasNext() {
            while (!currentData.hasNext() && nextToken != null) {
                FetchResultsResponseBody response =
                        executor.fetchResults(operationHandle, nextToken);
                currentData = response.getResults().getData().iterator();
                nextToken = executor.parseTokenFromUri(response.getNextResultUri());
            }
            return currentData.hasNext();
        }

        @Override
        public RowData next() {
            return currentData.next();
        }
    }
}
