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

package org.apache.flink.table.client.gateway.local.result;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.client.cli.utils.TestTableResult;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.Test;

import java.util.List;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

/** Tests for {@link ChangelogCollectResult}. */
public class ChangelogCollectResultTest {

    @Test
    public void testRetrieveChanges() throws Exception {
        int totalCount = ChangelogCollectResult.CHANGE_RECORD_BUFFER_SIZE * 2;
        CloseableIterator<Row> data =
                CloseableIterator.adapterForIterator(
                        IntStream.range(0, totalCount).mapToObj(Row::of).iterator());
        ChangelogCollectResult changelogResult =
                new ChangelogCollectResult(
                        new TestTableResult(
                                ResultKind.SUCCESS_WITH_CONTENT,
                                ResolvedSchema.of(Column.physical("id", DataTypes.INT())),
                                data));

        int count = 0;
        boolean running = true;
        while (running) {
            final TypedResult<List<Row>> result = changelogResult.retrieveChanges();
            Thread.sleep(100); // slow the processing down
            switch (result.getType()) {
                case EMPTY:
                    // do nothing
                    break;
                case EOS:
                    running = false;
                    break;
                case PAYLOAD:
                    count += result.getPayload().size();
                    break;
                default:
                    throw new SqlExecutionException("Unknown result type: " + result.getType());
            }
        }
        assertEquals(totalCount, count);
    }
}
