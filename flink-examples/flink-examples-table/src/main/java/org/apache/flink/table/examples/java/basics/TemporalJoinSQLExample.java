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

package org.apache.flink.table.examples.java.basics;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.time.Instant;

/**
 * Example for demonstrating the use of temporal join between a table backed by a {@link DataStream}
 * and a table backed by a change log stream.
 *
 * <p>In particular, the example shows how to
 *
 * <ul>
 *   <li>create a change log stream from elements
 *   <li>rename the table columns
 *   <li>register a table as a view under a name,
 *   <li>run a stream temporal join query on registered tables,
 *   <li>and convert the table back to a data stream.
 * </ul>
 *
 * <p>The example executes a single Flink job. The results are written to stdout.
 */
public class TemporalJoinSQLExample {

    public static void main(String[] args) throws Exception {

        // set up the Java DataStream API
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set up the Java Table API
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Create a changelog stream of currency rate
        final DataStream<Row> currencyRate =
                env.fromElements(
                        Row.ofKind(RowKind.INSERT, Instant.ofEpochMilli(1000), "USD", 0.8),
                        Row.ofKind(RowKind.UPDATE_AFTER, Instant.ofEpochMilli(4000), "USD", 0.9),
                        Row.ofKind(RowKind.UPDATE_AFTER, Instant.ofEpochMilli(3000), "USD", 1.0),
                        Row.ofKind(RowKind.UPDATE_AFTER, Instant.ofEpochMilli(6000), "USD", 1.1));

        // Create a table from change log stream
        Table rateTable =
                tableEnv.fromChangelogStream(
                                currencyRate,
                                Schema.newBuilder()
                                        .column("f0", DataTypes.TIMESTAMP_LTZ(3))
                                        .column("f1", DataTypes.STRING().notNull())
                                        .column("f2", DataTypes.DOUBLE())
                                        .watermark("f0", "f0 - INTERVAL '2' SECONDS")
                                        .primaryKey("f1")
                                        .build(),
                                ChangelogMode.upsert())
                        .as("rate_time", "currency_code", "euro_rate");

        // Register the table as a view, it will be accessible under a name
        tableEnv.createTemporaryView("currency_rate", rateTable);

        // Create a data stream of transaction
        final DataStream<Transaction> transaction =
                env.fromElements(
                        new Transaction("trx1", Instant.ofEpochMilli(1000), "USD", 1),
                        new Transaction("trx2", Instant.ofEpochMilli(2000), "USD", 1),
                        new Transaction("trx3", Instant.ofEpochMilli(3000), "USD", 1),
                        new Transaction("trx4", Instant.ofEpochMilli(4000), "USD", 1));

        // convert the Transaction DataStream and register it as a view,
        // it will be accessible under a name
        Table trxTable =
                tableEnv.fromDataStream(
                                transaction,
                                Schema.newBuilder()
                                        .column("id", DataTypes.STRING())
                                        .column("trxTime", DataTypes.TIMESTAMP_LTZ(3))
                                        .column("currencyCode", DataTypes.STRING())
                                        .column("amount", DataTypes.DOUBLE())
                                        .watermark("trxTime", "trxTime - INTERVAL '2' SECONDS")
                                        .build())
                        .as("id", "trx_time", "currency_code", "amount");

        // Register the table as a view, it will be accessible under a name
        tableEnv.createTemporaryView("transaction", trxTable);

        // temporal join the two tables
        final Table result =
                tableEnv.sqlQuery(
                        "    SELECT\n"
                                + "        t.id,\n"
                                + "        t.trx_time,\n"
                                + "        c.currency_code,\n"
                                + "        t.amount,\n"
                                + "        t.amount * c.euro_rate AS total_euro\n"
                                + "    FROM transaction t\n"
                                + "    JOIN currency_rate FOR SYSTEM_TIME AS OF t.trx_time AS c\n"
                                + "    ON t.currency_code = c.currency_code; ");

        // convert the Table back to an insert-only DataStream of type `Order`
        tableEnv.toDataStream(result, EnrichedTransaction.class).print();

        // after the table program is converted to a DataStream program,
        // we must use `env.execute()` to submit the job
        env.execute();
    }

    /** A simple class to represent a transaction. */
    public static class Transaction {
        public String id;
        public Instant trxTime;
        public String currencyCode;
        // the rate comparing with euro
        public double amount;

        // for POJO detection in DataStream API
        public Transaction() {}

        // for structured type detection in Table API
        public Transaction(String id, Instant trxTime, String currencyCode, double amount) {
            this.id = id;
            this.trxTime = trxTime;
            this.currencyCode = currencyCode;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return "Transaction{"
                    + "id="
                    + id
                    + ", trxTime="
                    + trxTime
                    + ", currencyCode='"
                    + currencyCode
                    + '\''
                    + ", amount="
                    + amount
                    + '}';
        }
    }

    /** Enriched transaction by joining with the currency rate table. */
    public static class EnrichedTransaction extends Transaction {
        public double totalEuro;

        // for POJO detection in DataStream API
        public EnrichedTransaction() {}

        // for structured type detection in Table API
        public EnrichedTransaction(
                String id, Instant trxTime, String currencyCode, double amount, double totalEuro) {
            super(id, trxTime, currencyCode, amount);
            this.totalEuro = totalEuro;
        }

        @Override
        public String toString() {
            return "EnrichedTransaction{"
                    + "id="
                    + id
                    + ", trxTime="
                    + trxTime
                    + ", currencyCode='"
                    + currencyCode
                    + '\''
                    + ", amount="
                    + amount
                    + ", totalEuro="
                    + totalEuro
                    + '}';
        }
    }
}
