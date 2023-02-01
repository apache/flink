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

package org.apache.flink.table.client.gateway.result;

import org.apache.flink.api.common.JobID;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.StatementResult;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ChangelogCollectResult}. */
class ChangelogCollectResultTest {

    @Test
    void testRetrieveChanges() throws Exception {
        int totalCount = ChangelogCollectResult.CHANGE_RECORD_BUFFER_SIZE * 2;
        CloseableIterator<RowData> data =
                CloseableIterator.adapterForIterator(
                        IntStream.range(0, totalCount)
                                .mapToObj(i -> (RowData) GenericRowData.of(i))
                                .iterator());
        ChangelogCollectResult changelogResult =
                new ChangelogCollectResult(
                        new StatementResult(
                                ResolvedSchema.of(Column.physical("id", DataTypes.INT())),
                                data,
                                true,
                                ResultKind.SUCCESS_WITH_CONTENT,
                                JobID.generate()));

        int count = 0;
        boolean running = true;
        while (running) {
            final TypedResult<List<RowData>> result = changelogResult.retrieveChanges();
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
        assertThat(count).isEqualTo(totalCount);
    }
}
