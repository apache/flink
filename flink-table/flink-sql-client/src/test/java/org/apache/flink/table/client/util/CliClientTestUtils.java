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

package org.apache.flink.table.client.util;

import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.client.SqlClient;
import org.apache.flink.table.client.gateway.StatementResult;
import org.apache.flink.table.gateway.rest.SqlGatewayRestEndpointStatementITCase;
import org.apache.flink.table.planner.functions.casting.RowDataToStringConverterImpl;
import org.apache.flink.table.utils.DateTimeUtils;
import org.apache.flink.util.CloseableIterator;

/** Test utils for {@link SqlClient}. */
public class CliClientTestUtils {

    public static StatementResult createTestClient(ResolvedSchema schema) {

        RowDataToStringConverterImpl converter =
                new RowDataToStringConverterImpl(
                        schema.toPhysicalRowDataType(),
                        DateTimeUtils.UTC_ZONE.toZoneId(),
                        SqlGatewayRestEndpointStatementITCase.class.getClassLoader(),
                        false);

        return new StatementResult(
                schema,
                CloseableIterator.empty(),
                true,
                ResultKind.SUCCESS_WITH_CONTENT,
                null,
                converter);
    }
}
