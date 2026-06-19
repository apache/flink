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

package org.apache.flink.table.gateway;

import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.service.utils.SqlGatewayServiceExtension;
import org.apache.flink.table.gateway.utils.TestSqlStatement;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.configuration.RestOptions.PORT;

/** Base ITCase tests for statements. */
@ExtendWith(ParameterizedTestExtension.class)
public abstract class AbstractSqlGatewayStatementITCase
        extends AbstractSqlGatewayStatementITCaseBase {

    private static final Logger LOG =
            LoggerFactory.getLogger(AbstractSqlGatewayStatementITCase.class);

    @RegisterExtension
    @Order(1)
    public static final MiniClusterExtension MINI_CLUSTER = new MiniClusterExtension();

    @RegisterExtension
    @Order(2)
    public static final SqlGatewayServiceExtension SQL_GATEWAY_SERVICE_EXTENSION =
            new SqlGatewayServiceExtension(MINI_CLUSTER::getClientConfiguration);

    protected static SqlGatewayService service;

    @BeforeAll
    static void setUp() {
        service = SQL_GATEWAY_SERVICE_EXTENSION.getService();
    }

    @BeforeEach
    public void before(@TempDir Path temporaryFolder) throws Exception {
        super.before(temporaryFolder);
        replaceVars.put(
                "$VAR_REST_PORT", MINI_CLUSTER.getClientConfiguration().get(PORT).toString());
    }

    /**
     * Returns printed results for each ran SQL statements.
     *
     * @param statements the SQL statements to run
     * @return the stringified results
     */
    protected String runStatements(List<TestSqlStatement> statements) throws Exception {
        List<String> output = new ArrayList<>();
        for (TestSqlStatement statement : statements) {
            StringBuilder builder = new StringBuilder();
            builder.append(statement.getComment());
            builder.append(statement.getSql());

            String trimmedSql = statement.getSql().trim();
            if (trimmedSql.endsWith(";")) {
                trimmedSql = trimmedSql.substring(0, trimmedSql.length() - 1);
            }
            try {
                builder.append(runSingleStatement(trimmedSql));
            } catch (Throwable t) {
                LOG.error("Failed to execute statements.", t);
                builder.append(
                        AbstractSqlGatewayStatementITCase.Tag.ERROR.addTag(
                                removeRowNumber(stringifyException(t).trim()) + "\n"));
            }
            output.add(builder.toString());
        }

        return String.join("", output);
    }

    // -------------------------------------------------------------------------------------------
    // Utility
    // -------------------------------------------------------------------------------------------

    /**
     * Returns printed results for each ran SQL statements.
     *
     * @param statement the SQL statement to run
     * @return the printed results in tableau style
     */
    protected abstract String runSingleStatement(String statement) throws Exception;

    protected abstract String stringifyException(Throwable t);
}
