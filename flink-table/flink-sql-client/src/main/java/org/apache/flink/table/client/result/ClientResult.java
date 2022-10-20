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

package org.apache.flink.table.client.result;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.utils.print.RowDataToStringConverter;
import org.apache.flink.util.CloseableIterator;

import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;

/** To wrap the result returned by {@link Executor}. */
public class ClientResult {

    private final ResolvedSchema resolvedSchema;
    private final CloseableIterator<RowData> dataIterator;

    public ClientResult(ResolvedSchema resolvedSchema, CloseableIterator<RowData> dataIterator) {
        this.resolvedSchema = resolvedSchema;
        this.dataIterator = dataIterator;
    }

    public ResolvedSchema getResolvedSchema() {
        return resolvedSchema;
    }

    public CloseableIterator<RowData> getDataIterator() {
        return dataIterator;
    }

    // For the result sent to client, the RowData only contains StringData
    public static final RowDataToStringConverter DEFAULT_ROW_DATA_TO_STRING_CONVERTER =
            rowData ->
                    IntStream.range(0, rowData.getArity())
                            .mapToObj(rowData::getString)
                            .map(StringData::toString)
                            .toArray(String[]::new);

    // --------------------------------------------------------------------------------------------

    public static RowDataIterator buildRowDataIterator(
            List<RowData> firstData, Long nextToken, ResultCollector collector) {
        return new RowDataIterator(firstData.iterator(), nextToken, collector);
    }

    /**
     * The SQL gateway doesn't return all data once, so a result collector is needed. It works with
     * {@link SqlGatewayService#fetchResults} .
     */
    @FunctionalInterface
    public interface ResultCollector {
        Tuple2<Iterator<RowData>, Long> collect(Long nextToken);
    }

    private static class RowDataIterator implements CloseableIterator<RowData> {
        private Iterator<RowData> currentData;
        @Nullable private Long nextToken;
        private final ResultCollector collector;

        private RowDataIterator(
                Iterator<RowData> firstData, Long nextToken, ResultCollector collector) {
            this.currentData = firstData;
            this.nextToken = nextToken;
            this.collector = collector;
        }

        @Override
        public void close() {
            // do nothing
        }

        @Override
        public boolean hasNext() {
            while (!currentData.hasNext() && nextToken != null) {
                Tuple2<Iterator<RowData>, Long> collection = collector.collect(nextToken);
                currentData = collection.f0;
                nextToken = collection.f1;
            }
            return currentData.hasNext();
        }

        @Override
        public RowData next() {
            return currentData.next();
        }
    }
}
