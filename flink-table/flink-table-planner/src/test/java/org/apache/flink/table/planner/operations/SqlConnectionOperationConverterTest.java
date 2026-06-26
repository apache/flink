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

package org.apache.flink.table.planner.operations;

import org.apache.flink.sql.parser.error.SqlValidateException;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.SensitiveConnection;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.CreateConnectionOperation;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for converting connection statements to operations. */
class SqlConnectionOperationConverterTest extends SqlNodeToOperationConversionTestBase {

    @Test
    void testCreateConnection() {
        Operation operation = parse("CREATE CONNECTION my_conn WITH ('k' = 'v')");
        assertThat(operation).isInstanceOf(CreateConnectionOperation.class);
        CreateConnectionOperation op = (CreateConnectionOperation) operation;

        assertThat(op.getConnectionIdentifier())
                .isEqualTo(ObjectIdentifier.of("builtin", "default", "my_conn"));
        assertThat(op.getSensitiveConnection().getOptions()).isEqualTo(Map.of("k", "v"));
        assertThat(op.getSensitiveConnection().getComment()).isNull();
        assertThat(op.isIgnoreIfExists()).isFalse();
        assertThat(op.isTemporary()).isFalse();
    }

    @Test
    void testCreateConnectionIfNotExists() {
        Operation operation = parse("CREATE CONNECTION IF NOT EXISTS my_conn WITH ('k' = 'v')");
        CreateConnectionOperation op = (CreateConnectionOperation) operation;
        assertThat(op.isIgnoreIfExists()).isTrue();
        assertThat(op.isTemporary()).isFalse();
    }

    @Test
    void testCreateTemporaryConnection() {
        Operation operation = parse("CREATE TEMPORARY CONNECTION my_conn WITH ('k' = 'v')");
        CreateConnectionOperation op = (CreateConnectionOperation) operation;
        assertThat(op.isTemporary()).isTrue();
        assertThat(op.isIgnoreIfExists()).isFalse();
    }

    @Test
    void testCreateTemporarySystemConnection() {
        Operation operation = parse("CREATE TEMPORARY SYSTEM CONNECTION my_conn WITH ('k' = 'v')");
        CreateConnectionOperation op = (CreateConnectionOperation) operation;
        assertThat(op.isTemporary()).isTrue();
    }

    @Test
    void testCreateConnectionWithComment() {
        Operation operation =
                parse("CREATE CONNECTION my_conn COMMENT 'hi there' WITH ('k' = 'v')");
        CreateConnectionOperation op = (CreateConnectionOperation) operation;
        SensitiveConnection conn = op.getSensitiveConnection();
        assertThat(conn.getComment()).isEqualTo("hi there");
    }

    @Test
    void testCreateConnectionWithFullyQualifiedName() {
        Operation operation = parse("CREATE CONNECTION cat1.db1.my_conn WITH ('k' = 'v')");
        CreateConnectionOperation op = (CreateConnectionOperation) operation;
        assertThat(op.getConnectionIdentifier())
                .isEqualTo(ObjectIdentifier.of("cat1", "db1", "my_conn"));
    }

    @Test
    void testCreateConnectionOptions() {
        Operation operation =
                parse("CREATE CONNECTION my_conn WITH ('k1' = 'v1', 'k2' = 'v2', 'k3' = 'v3')");
        CreateConnectionOperation op = (CreateConnectionOperation) operation;
        assertThat(op.getSensitiveConnection().getOptions())
                .isEqualTo(Map.of("k1", "v1", "k2", "v2", "k3", "v3"));
    }

    @Test
    void testAsSummaryStringMasksOptionValues() {
        Operation operation =
                parse(
                        "CREATE CONNECTION my_conn WITH ('user' = 'alice', 'password' = 'super-secret')");
        String summary = operation.asSummaryString();
        assertThat(summary).contains("user").contains("password").contains("****");
        assertThat(summary).doesNotContain("alice").doesNotContain("super-secret");
    }

    @Test
    void testCreateSystemConnectionWithoutTemporaryRejected() {
        assertThatThrownBy(() -> parse("CREATE SYSTEM CONNECTION my_conn WITH ('k' = 'v')"))
                .isInstanceOf(SqlParserException.class)
                .hasMessageContaining("CREATE SYSTEM CONNECTION is not supported");
    }

    @Test
    void testCreateConnectionWithEmptyOptionsRejected() {
        assertThatThrownBy(() -> parse("CREATE CONNECTION my_conn WITH ()"))
                .isInstanceOf(SqlValidateException.class)
                .hasMessageContaining("Connection property list can not be empty.");
    }
}
