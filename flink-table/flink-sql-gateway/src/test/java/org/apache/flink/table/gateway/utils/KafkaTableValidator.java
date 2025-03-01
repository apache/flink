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

package org.apache.flink.table.gateway.utils;

import org.apache.flink.table.gateway.api.operation.OperationValidator;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;

import java.util.Optional;

/** Test implementation of {@link OperationValidator} for Sql Gateway ITCase. */
public class KafkaTableValidator implements OperationValidator {
    private static final long ONE_DAY = 24 * 60 * 60 * 1000L;

    @Override
    public Optional<String> validate(Operation op) {
        if (op instanceof CreateTableOperation) {
            final CreateTableOperation createTableOp = (CreateTableOperation) op;
            final String connector = createTableOp.getCatalogTable().getOptions().get("connector");
            if ("kafka".equals(connector)) {
                try {
                    final long startupTimestampValue =
                            Long.parseLong(
                                    createTableOp
                                            .getCatalogTable()
                                            .getOptions()
                                            .get("scan.startup.timestamp-millis"));
                    if (startupTimestampValue < System.currentTimeMillis() - ONE_DAY) {
                        return Optional.of(
                                String.format(
                                        "Validation failed in %s: 'scan.startup.timestamp-millis' is too old. Given value: %d. It must be within one day from the current time.",
                                        KafkaTableValidator.class.getName(),
                                        startupTimestampValue));
                    }
                } catch (NumberFormatException e) {
                    return Optional.empty();
                }
            }
        }
        return Optional.empty();
    }
}
