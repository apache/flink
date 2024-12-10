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

package org.apache.flink.table.gateway.service.application;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.service.result.ResultFetcher;
import org.apache.flink.util.CloseableIterator;

import javax.annotation.Nullable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.Iterator;

/** Printer to print the statement results in application mode. */
public class Printer {

    @VisibleForTesting public static final String STATEMENT_BEGIN = "Flink SQL> ";
    @VisibleForTesting public static final String LINE_BEGIN = "> ";

    @VisibleForTesting final PrintWriter writer;

    public Printer() {
        this(System.out);
    }

    public Printer(OutputStream output) {
        this.writer = new PrintWriter(output, true);
    }

    public void print(String statement) {
        try (BufferedReader reader = new BufferedReader(new StringReader(statement))) {
            writer.print(STATEMENT_BEGIN);
            writer.println(reader.readLine());
            String line;
            while ((line = reader.readLine()) != null) {
                writer.print(LINE_BEGIN);
                writer.println(line);
            }
        } catch (IOException e) {
            throw new SqlGatewayException("Failed to read the line.", e);
        }
    }

    public void print(ResultFetcher result) {
        result.getPrintStyle().print(new RowDataIterator(result), writer);
    }

    public void print(Throwable t) {
        t.printStackTrace(writer);
    }

    @VisibleForTesting
    public static class RowDataIterator implements CloseableIterator<RowData> {

        private final ResultFetcher fetcher;
        private Iterator<RowData> current;
        private @Nullable Long nextToken;

        public RowDataIterator(ResultFetcher fetcher) {
            this.fetcher = fetcher;
            this.nextToken = 0L;
        }

        @Override
        public boolean hasNext() {
            while (current == null || !current.hasNext()) {
                if (nextToken == null) {
                    return false;
                }
                ResultSet resultSet = fetcher.fetchResults(nextToken, Integer.MAX_VALUE);
                current = resultSet.getData().iterator();
                nextToken = resultSet.getNextToken();
                if (resultSet.getResultType() == ResultSet.ResultType.EOS) {
                    return current.hasNext();
                } else if (resultSet.getResultType() == ResultSet.ResultType.NOT_READY) {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        throw new SqlGatewayException("Failed to wait job finishes.", e);
                    }
                }
            }
            return true;
        }

        @Override
        public RowData next() {
            return current.next();
        }

        @Override
        public void close() {
            fetcher.close();
        }
    }
}
