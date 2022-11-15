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

package org.apache.flink.table.gateway.service.context;

import org.apache.flink.client.cli.DefaultCLI;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.MockedEndpointVersion;
import org.apache.flink.table.gateway.api.utils.ThreadUtils;
import org.apache.flink.table.gateway.service.operation.OperationExecutor;
import org.apache.flink.table.gateway.service.utils.SqlExecutionException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.ExecutorService;

import static org.apache.flink.configuration.PipelineOptions.MAX_PARALLELISM;
import static org.apache.flink.configuration.PipelineOptions.NAME;
import static org.apache.flink.configuration.PipelineOptions.OBJECT_REUSE;
import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_SQL_DIALECT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test {@link SessionContext}. */
class SessionContextTest {

    private static final ExecutorService EXECUTOR_SERVICE =
            ThreadUtils.newThreadPool(5, 500, 60_0000, "session-context-test");
    private SessionContext sessionContext;

    @BeforeEach
    public void setup() {
        sessionContext = createSessionContext();
    }

    @AfterEach
    public void cleanUp() {
        sessionContext.close();
    }

    @AfterAll
    public static void closeResources() {
        EXECUTOR_SERVICE.shutdown();
    }

    @Test
    public void testSetAndResetOption() {
        // table config option
        sessionContext.set(TABLE_SQL_DIALECT.key(), "hive");
        // runtime config option
        sessionContext.set(MAX_PARALLELISM.key(), "128");
        // runtime config option and doesn't have default value
        sessionContext.set(NAME.key(), "test");
        // runtime config from flink-conf
        sessionContext.set(OBJECT_REUSE.key(), "false");
        assertThat(getConfiguration().get(TABLE_SQL_DIALECT)).isEqualTo("hive");
        assertThat(getConfiguration().get(MAX_PARALLELISM)).isEqualTo(128);
        assertThat(getConfiguration().get(NAME)).isEqualTo("test");
        assertThat(getConfiguration().get(OBJECT_REUSE)).isFalse();

        sessionContext.reset();
        assertThat(getConfiguration().get(TABLE_SQL_DIALECT)).isEqualTo("default");
        assertThat(getConfiguration().get(NAME)).isNull();
        // The value of MAX_PARALLELISM in DEFAULTS_ENVIRONMENT_FILE is 16
        assertThat(getConfiguration().get(MAX_PARALLELISM)).isEqualTo(16);
        assertThat(getConfiguration().getOptional(NAME)).isEmpty();
        // The value of OBJECT_REUSE in origin configuration is true
        assertThat(getConfiguration().get(OBJECT_REUSE)).isTrue();
    }

    @Test
    public void testSetAndResetKeyInConfigOptions() {
        // table config option
        sessionContext.set(TABLE_SQL_DIALECT.key(), "hive");
        // runtime config option
        sessionContext.set(MAX_PARALLELISM.key(), "128");
        // runtime config option and doesn't have default value
        sessionContext.set(NAME.key(), "test");
        // runtime config from flink-conf
        sessionContext.set(OBJECT_REUSE.key(), "false");

        assertThat(getConfiguration().get(TABLE_SQL_DIALECT)).isEqualTo("hive");
        assertThat(getConfiguration().get(MAX_PARALLELISM)).isEqualTo(128);
        assertThat(getConfiguration().get(NAME)).isEqualTo("test");
        assertThat(getConfiguration().get(OBJECT_REUSE)).isFalse();

        sessionContext.reset(TABLE_SQL_DIALECT.key());
        assertThat(getConfiguration().get(TABLE_SQL_DIALECT)).isEqualTo("default");

        sessionContext.reset(MAX_PARALLELISM.key());
        assertThat(getConfiguration().get(MAX_PARALLELISM)).isEqualTo(16);

        sessionContext.reset(NAME.key());
        assertThat(getConfiguration().get(NAME)).isNull();

        sessionContext.reset(OBJECT_REUSE.key());
        assertThat(getConfiguration().get(OBJECT_REUSE)).isTrue();
    }

    @Test
    public void testSetAndResetArbitraryKey() {
        // other property not in flink-conf
        sessionContext.set("aa", "11");
        sessionContext.set("bb", "22");

        assertThat(sessionContext.getConfigMap().get("aa")).isEqualTo("11");
        assertThat(sessionContext.getConfigMap().get("bb")).isEqualTo("22");

        sessionContext.reset("aa");
        assertThat(sessionContext.getConfigMap().get("aa")).isNull();
        assertThat(sessionContext.getConfigMap().get("bb")).isEqualTo("22");

        sessionContext.reset("bb");
        assertThat(sessionContext.getConfigMap().get("bb")).isNull();
    }

    @Test
    public void testStatementSetStateTransition() {
        assertThat(sessionContext.isStatementSetState()).isFalse();

        OperationExecutor executor = sessionContext.createOperationExecutor(new Configuration());
        TableEnvironmentInternal tableEnv = executor.getTableEnvironment();
        tableEnv.executeSql("CREATE TABLE whatever (f STRING) WITH ('connector' = 'values')");

        executor.executeStatement(OperationHandle.create(), "BEGIN STATEMENT SET;");

        assertThat(sessionContext.isStatementSetState()).isTrue();

        // invalid statement in Statement Set
        assertThatThrownBy(() -> executor.executeStatement(OperationHandle.create(), "SELECT 1;"))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                SqlExecutionException.class,
                                "Wrong statement after 'BEGIN STATEMENT SET'.\n"
                                        + "Only 'INSERT/CREATE TABLE AS' statement is allowed in Statement Set or use 'END' statement to terminate Statement Set."));

        // 'isStatementSetState' is still true, and nothing in 'statementSetOperations'
        assertThat(sessionContext.isStatementSetState()).isTrue();
        assertThat(sessionContext.getStatementSetOperations().size()).isEqualTo(0);

        // valid statement in Statement Set
        String insert = "INSERT INTO whatever VALUES('test%s');";
        int repeat = 3;
        for (int i = 0; i < repeat; i++) {
            executor.executeStatement(OperationHandle.create(), String.format(insert, i));
        }

        assertThat(sessionContext.getStatementSetOperations().size()).isEqualTo(repeat);
        for (int i = 0; i < repeat; i++) {
            assertThat(sessionContext.getStatementSetOperations().get(i).asSummaryString())
                    .isEqualTo(
                            tableEnv.getParser()
                                    .parse(String.format(insert, i))
                                    .get(0)
                                    .asSummaryString());
        }

        // end Statement Set
        try {
            executor.executeStatement(OperationHandle.create(), "END;");
        } catch (Throwable t) {
            // just test the Statement Set state transition, so ignore the error that cluster
            // doesn't exist
        }

        assertThat(sessionContext.isStatementSetState()).isFalse();
        assertThat(sessionContext.getStatementSetOperations().size()).isEqualTo(0);

        // dangling 'END'
        assertThatThrownBy(() -> executor.executeStatement(OperationHandle.create(), "END;"))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                SqlExecutionException.class,
                                "No Statement Set to submit. 'END' statement should be used after 'BEGIN STATEMENT SET'."));

        // nothing happened to 'isStatementSetState' and 'statementSetOperations'
        assertThat(sessionContext.isStatementSetState()).isFalse();
        assertThat(sessionContext.getStatementSetOperations().size()).isEqualTo(0);
    }

    // --------------------------------------------------------------------------------------------

    private SessionContext createSessionContext() {
        Configuration flinkConfig = new Configuration();
        flinkConfig.set(OBJECT_REUSE, true);
        flinkConfig.set(MAX_PARALLELISM, 16);
        DefaultContext defaultContext =
                new DefaultContext(flinkConfig, Collections.singletonList(new DefaultCLI()));
        SessionEnvironment environment =
                SessionEnvironment.newBuilder()
                        .setSessionEndpointVersion(MockedEndpointVersion.V1)
                        .addSessionConfig(flinkConfig.toMap())
                        .build();
        return SessionContext.create(
                defaultContext, SessionHandle.create(), environment, EXECUTOR_SERVICE);
    }

    private ReadableConfig getConfiguration() {
        return Configuration.fromMap(sessionContext.getConfigMap());
    }
}
