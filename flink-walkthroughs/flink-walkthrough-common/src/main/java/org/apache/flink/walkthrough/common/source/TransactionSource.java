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

package org.apache.flink.walkthrough.common.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.walkthrough.common.entity.Transaction;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

/**
 * A source of transaction events for the fraud detection walkthrough.
 *
 * <p>This source generates an effectively unbounded stream of credit card transactions at a rate of
 * 10 transactions per second. The transactions cycle through a predefined set of 50 transactions
 * with 5 different account IDs.
 *
 * <p>This implementation uses the FLIP-27/FLIP-238 {@link DataGeneratorSource} API.
 */
public final class TransactionSource {

    private static final long INITIAL_TIMESTAMP =
            Timestamp.valueOf("2019-01-01 00:00:00").getTime();
    private static final long SIX_MINUTES = 6 * 60 * 1000;
    private static final double RECORDS_PER_SECOND = 10.0;

    private static final List<Transaction> TRANSACTIONS =
            Arrays.asList(
                    new Transaction(1, 0L, 188.23),
                    new Transaction(2, 0L, 374.79),
                    new Transaction(3, 0L, 112.15),
                    new Transaction(4, 0L, 478.75),
                    new Transaction(5, 0L, 208.85),
                    new Transaction(1, 0L, 379.64),
                    new Transaction(2, 0L, 351.44),
                    new Transaction(3, 0L, 320.75),
                    new Transaction(4, 0L, 259.42),
                    new Transaction(5, 0L, 273.44),
                    new Transaction(1, 0L, 267.25),
                    new Transaction(2, 0L, 397.15),
                    new Transaction(3, 0L, 0.219),
                    new Transaction(4, 0L, 231.94),
                    new Transaction(5, 0L, 384.73),
                    new Transaction(1, 0L, 419.62),
                    new Transaction(2, 0L, 412.91),
                    new Transaction(3, 0L, 0.77),
                    new Transaction(4, 0L, 22.10),
                    new Transaction(5, 0L, 377.54),
                    new Transaction(1, 0L, 375.44),
                    new Transaction(2, 0L, 230.18),
                    new Transaction(3, 0L, 0.80),
                    new Transaction(4, 0L, 350.89),
                    new Transaction(5, 0L, 127.55),
                    new Transaction(1, 0L, 483.91),
                    new Transaction(2, 0L, 228.22),
                    new Transaction(3, 0L, 871.15),
                    new Transaction(4, 0L, 64.19),
                    new Transaction(5, 0L, 79.43),
                    new Transaction(1, 0L, 56.12),
                    new Transaction(2, 0L, 256.48),
                    new Transaction(3, 0L, 148.16),
                    new Transaction(4, 0L, 199.95),
                    new Transaction(5, 0L, 252.37),
                    new Transaction(1, 0L, 274.73),
                    new Transaction(2, 0L, 473.54),
                    new Transaction(3, 0L, 119.92),
                    new Transaction(4, 0L, 323.59),
                    new Transaction(5, 0L, 353.16),
                    new Transaction(1, 0L, 211.90),
                    new Transaction(2, 0L, 280.93),
                    new Transaction(3, 0L, 347.89),
                    new Transaction(4, 0L, 459.86),
                    new Transaction(5, 0L, 82.31),
                    new Transaction(1, 0L, 373.26),
                    new Transaction(2, 0L, 479.83),
                    new Transaction(3, 0L, 454.25),
                    new Transaction(4, 0L, 83.64),
                    new Transaction(5, 0L, 292.44));

    private TransactionSource() {}

    /**
     * Creates an unbounded source of transactions.
     *
     * @return A {@link DataGeneratorSource} that produces transactions at 10 records per second.
     */
    public static DataGeneratorSource<Transaction> unbounded() {
        return create(Long.MAX_VALUE);
    }

    /**
     * Creates a bounded source of transactions.
     *
     * @param count The number of transactions to generate.
     * @return A {@link DataGeneratorSource} that produces the specified number of transactions.
     */
    public static DataGeneratorSource<Transaction> bounded(long count) {
        return create(count);
    }

    private static DataGeneratorSource<Transaction> create(long count) {
        GeneratorFunction<Long, Transaction> generatorFunction =
                index -> {
                    int dataIndex = (int) (index % TRANSACTIONS.size());
                    Transaction template = TRANSACTIONS.get(dataIndex);
                    long timestamp = INITIAL_TIMESTAMP + (index * SIX_MINUTES);
                    return new Transaction(
                            template.getAccountId(), timestamp, template.getAmount());
                };

        return new DataGeneratorSource<>(
                generatorFunction,
                count,
                RateLimiterStrategy.perSecond(RECORDS_PER_SECOND),
                TypeInformation.of(Transaction.class));
    }
}
