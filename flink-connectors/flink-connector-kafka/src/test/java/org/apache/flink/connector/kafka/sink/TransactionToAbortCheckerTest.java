/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for {@link TransactionsToAbortChecker}. */
public class TransactionToAbortCheckerTest extends TestLogger {

    public static final String ABORT = "abort";

    @Test
    public void testMustAbortTransactionsWithSameSubtaskIdAndHigherCheckpointOffset() {
        final TransactionsToAbortChecker checker =
                new TransactionsToAbortChecker(2, ImmutableMap.of(0, 1L, 2, 3L), 0);

        // abort recovered subtasksId with equal or higher checkpoint offset
        final Map<Integer, Map<Long, String>> openTransactions =
                ImmutableMap.of(
                        0, ImmutableMap.of(2L, ABORT, 1L, ABORT),
                        2, ImmutableMap.of(3L, ABORT, 4L, ABORT),
                        3, ImmutableMap.of(3L, "keep", 4L, "keep"));

        final List<String> transactionsToAbort = checker.getTransactionsToAbort(openTransactions);
        assertEquals(4, transactionsToAbort.size());
        assertThatAbortCorrectTransaction(transactionsToAbort);
    }

    @Test
    public void testMustAbortTransactionsIfLowestCheckpointOffsetIsMinimumOffset() {
        final TransactionsToAbortChecker checker =
                new TransactionsToAbortChecker(2, ImmutableMap.of(0, 1L), 0);

        // abort recovered subtasksId with equal or higher checkpoint offset
        final Map<Integer, Map<Long, String>> openTransactions =
                ImmutableMap.of(
                        0, ImmutableMap.of(2L, ABORT, 1L, ABORT),
                        2, ImmutableMap.of(1L, ABORT),
                        3, ImmutableMap.of(1L, "keep"),
                        4, ImmutableMap.of(1L, ABORT),
                        5, ImmutableMap.of(1L, "keep"));

        final List<String> transactionsToAbort = checker.getTransactionsToAbort(openTransactions);
        assertEquals(4, transactionsToAbort.size());
        assertThatAbortCorrectTransaction(transactionsToAbort);
    }

    private static void assertThatAbortCorrectTransaction(List<String> abortedTransactions) {
        assertTrue(abortedTransactions.stream().allMatch(t -> t.equals(ABORT)));
    }
}
