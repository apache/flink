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

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

/**
 * A User-Defined Function (UDF) that truncates a timestamp to hour granularity.
 *
 * <p>This demonstrates how to create custom functions for the Table API.
 * While Flink provides a built-in {@code floor()} function, this example shows
 * how you would implement similar functionality as a UDF.
 *
 * <p>Usage in Table API:
 * <pre>{@code
 * import static org.apache.flink.table.api.Expressions.*;
 *
 * Table result = transactions
 *     .select(
 *         $("accountId"),
 *         call(MyFloor.class, $("transactionTime")).as("logTs"),
 *         $("amount"));
 * }</pre>
 */
public class MyFloor extends ScalarFunction {

    /**
     * Truncates the given timestamp to the start of the hour.
     *
     * @param timestamp The timestamp to truncate
     * @return The timestamp truncated to the hour (e.g., 09:47:32 becomes 09:00:00)
     */
    public @DataTypeHint("TIMESTAMP(3)") LocalDateTime eval(
            @DataTypeHint("TIMESTAMP(3)") LocalDateTime timestamp) {

        return timestamp.truncatedTo(ChronoUnit.HOURS);
    }
}
