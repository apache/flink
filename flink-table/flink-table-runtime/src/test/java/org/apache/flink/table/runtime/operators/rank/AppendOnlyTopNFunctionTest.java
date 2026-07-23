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

package org.apache.flink.table.runtime.operators.rank;

import org.apache.flink.runtime.metrics.util.InterceptingOperatorMetricGroup;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.rank.async.AsyncStateAppendOnlyTopNFunction;

import org.junit.jupiter.api.TestTemplate;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/** Tests for {@link AppendOnlyTopNFunction} and {@link AsyncStateAppendOnlyTopNFunction}. */
class AppendOnlyTopNFunctionTest extends TopNFunctionTestBase {

    @Override
    protected AbstractTopNFunction createFunction(
            RankType rankType,
            RankRange rankRange,
            boolean generateUpdateBefore,
            boolean outputRankNumber,
            boolean enableAsyncState) {
        if (enableAsyncState) {
            return new AsyncStateAppendOnlyTopNFunction(
                    ttlConfig,
                    inputRowType,
                    generatedSortKeyComparator,
                    sortKeySelector,
                    rankType,
                    rankRange,
                    generateUpdateBefore,
                    outputRankNumber,
                    cacheSize);
        } else {
            return new AppendOnlyTopNFunction(
                    ttlConfig,
                    inputRowType,
                    generatedSortKeyComparator,
                    sortKeySelector,
                    rankType,
                    rankRange,
                    generateUpdateBefore,
                    outputRankNumber,
                    cacheSize);
        }
    }

    @Override
    boolean supportedAsyncState() {
        return true;
    }

    @TestTemplate
    void testVariableRankRange() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new VariableRankRange(1), true, false);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 2L, 12));
        testHarness.processElement(insertRecord("book", 2L, 19));
        testHarness.processElement(insertRecord("book", 2L, 11));
        testHarness.processElement(insertRecord("fruit", 1L, 33));
        testHarness.processElement(insertRecord("fruit", 1L, 44));
        testHarness.processElement(insertRecord("fruit", 1L, 22));
        testHarness.close();

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("book", 2L, 12));
        expectedOutput.add(insertRecord("book", 2L, 19));
        expectedOutput.add(deleteRecord("book", 2L, 19));
        expectedOutput.add(insertRecord("book", 2L, 11));
        expectedOutput.add(insertRecord("fruit", 1L, 33));
        expectedOutput.add(deleteRecord("fruit", 1L, 33));
        expectedOutput.add(insertRecord("fruit", 1L, 22));
        assertorWithoutRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    /**
     * Verifies that {@code topn.cache.size} reflects the live cache state instead of staying at 0
     * for {@link AppendOnlyTopNFunction}.
     */
    @TestTemplate
    void testCacheMetricsReflectLiveState() throws Exception {
        // async harness does not allow injecting a custom metric group; sync path covers the fix.
        assumeFalse(enableAsyncState);
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), true, false);
        InterceptingOperatorMetricGroup metricGroup = new InterceptingOperatorMetricGroup();
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarnessWithMetrics(func, metricGroup);
        testHarness.open();
        // no partition cached yet -> cache size is 0.
        assertThat(readCacheSizeMetric(metricGroup)).isZero();
        // no requests issued yet -> hit rate falls back to 1.0.
        assertThat(readCacheHitRateMetric(metricGroup)).isEqualTo(1.0);

        testHarness.processElement(insertRecord("book", 1L, 12));
        testHarness.processElement(insertRecord("book", 2L, 19));
        testHarness.processElement(insertRecord("fruit", 4L, 33));
        // 2 partitions cached -> live size = 2 * topN(2) = 4.
        assertThat(readCacheSizeMetric(metricGroup)).isEqualTo(4L);
        // hit rate is now driven by live counters and lies in [0.0, 1.0].
        assertThat(readCacheHitRateMetric(metricGroup)).isBetween(0.0, 1.0);
        testHarness.close();
    }
}
