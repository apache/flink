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

package ${package};

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Spend Report using the Table API.
 *
 * <p>This example demonstrates how to build a streaming data pipeline using Flink's Table API.
 * It generates an infinite stream of credit card transactions using the DataGen connector
 * and computes hourly spending reports per account.
 *
 * <p>To run this example in your IDE:
 * <ul>
 *   <li>IntelliJ IDEA: Go to Run > Edit Configurations > Modify options > Select
 *       "include dependencies with 'Provided' scope"</li>
 * </ul>
 */
public class SpendReport {

    public static void main(String[] args) throws Exception {
        // Create a Table environment for streaming
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // Create the source table using Table API
        // The DataGen connector generates an infinite stream of transactions
        tEnv.createTemporaryTable("transactions",
            TableDescriptor.forConnector("datagen")
                .schema(Schema.newBuilder()
                    .column("accountId", DataTypes.BIGINT())
                    .column("amount", DataTypes.BIGINT())
                    .column("transactionTime", DataTypes.TIMESTAMP(3))
                    .watermark("transactionTime", "transactionTime - INTERVAL '5' SECOND")
                    .build())
                .option("rows-per-second", "100")
                .option("fields.accountId.min", "1")
                .option("fields.accountId.max", "5")
                .option("fields.amount.min", "1")
                .option("fields.amount.max", "1000")
                .build());

        // Read from the source table
        Table transactions = tEnv.from("transactions");

        // Apply the business logic
        Table result = report(transactions);

        // Print the results to the console
        result.execute().print();
    }

    /**
     * The business logic: aggregate spending by account and hour.
     *
     * <p>This method takes a table of transactions and computes the total spending
     * for each account within each hour.
     *
     * @param transactions A table with columns: accountId, amount, transactionTime
     * @return A table with columns: accountId, logTs (hour), amount (sum)
     */
    public static Table report(Table transactions) {
        return transactions
            .select(
                $("accountId"),
                $("transactionTime").floor(org.apache.flink.table.expressions.TimeIntervalUnit.HOUR).as("logTs"),
                $("amount"))
            .groupBy($("accountId"), $("logTs"))
            .select(
                $("accountId"),
                $("logTs"),
                $("amount").sum().as("amount"));
    }
}
