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

package org.apache.flink.table.api.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.utils.print.RowDataToStringConverter;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import javax.annotation.Nullable;

import java.util.NoSuchElementException;
import java.util.function.Supplier;

/** A {@link ResultProvider} with custom wait logic for insert operation result. */
@Internal
class InsertResultProvider implements ResultProvider {

    private final Long[] affectedRowCountsRow;

    private @Nullable Boolean hasNext = null;
    private JobClient jobClient;

    InsertResultProvider(Long[] affectedRowCountsRow) {
        this.affectedRowCountsRow = affectedRowCountsRow;
    }

    @Override
    public InsertResultProvider setJobClient(JobClient jobClient) {
        this.jobClient = jobClient;
        return this;
    }

    @Override
    public CloseableIterator<RowData> toInternalIterator() {
        return new Iterator<>(() -> GenericRowData.of(((Object[]) affectedRowCountsRow)));
    }

    @Override
    public CloseableIterator<Row> toExternalIterator() {
        return new Iterator<>(() -> Row.of(((Object[]) affectedRowCountsRow)));
    }

    @Override
    public RowDataToStringConverter getRowDataStringConverter() {
        return StaticResultProvider.SIMPLE_ROW_DATA_TO_STRING_CONVERTER;
    }

    @Override
    public boolean isFirstRowReady() {
        return hasNext != null || hasNext();
    }

    private void close() throws Exception {
        if (jobClient != null) {
            jobClient.cancel();
        }
    }

    private boolean hasNext() {
        if (hasNext == null) {
            try {
                jobClient.getJobExecutionResult().get();
            } catch (Exception e) {
                throw new TableException("Failed to wait job finish", e);
            }
            hasNext = true;
        }
        return hasNext;
    }

    private class Iterator<T> implements CloseableIterator<T> {
        private final Supplier<T> rowSupplier;

        private Iterator(Supplier<T> rowSupplier) {
            this.rowSupplier = rowSupplier;
        }

        @Override
        public void close() throws Exception {
            InsertResultProvider.this.close();
        }

        @Override
        public boolean hasNext() {
            return InsertResultProvider.this.hasNext();
        }

        @Override
        public T next() {
            if (hasNext()) {
                hasNext = false;
                return rowSupplier.get();
            } else {
                throw new NoSuchElementException();
            }
        }
    }
}
