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

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.ProcedureNotExistException;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.table.procedures.Procedure;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test cases for the call statements for {@link SqlNodeToOperationConversion}. */
public class SqlNodeToCallOperationTest extends SqlNodeToOperationConversionTestBase {

    @BeforeEach
    public void before() {
        CatalogWithBuiltInProcedure procedureCatalog =
                new CatalogWithBuiltInProcedure("procedure_catalog");
        catalogManager.registerCatalog("p1", procedureCatalog);
        catalogManager.setCurrentCatalog("p1");
    }

    @Test
    void testCallStatement() {
        // test call the procedure which accepts primitive types as arguments
        String sql = "call `system`.primitive_arg(1, 2)";
        verifyCallOperation(
                sql,
                "CALL PROCEDURE:"
                        + " (procedureIdentifier: [`p1`.`system`.`primitive_arg`],"
                        + " inputTypes: [INT NOT NULL, BIGINT NOT NULL], outputTypes: [INT NOT NULL], arguments: [1, 2])");

        // test call the procedure which has different type mapping for single method
        // call with int
        sql = "call `system`.different_type_mapping(1)";
        verifyCallOperation(
                sql,
                "CALL PROCEDURE:"
                        + " (procedureIdentifier: [`p1`.`system`.`different_type_mapping`],"
                        + " inputTypes: [INT], outputTypes: [INT], arguments: [1])");
        // call with bigint
        sql = "call `system`.different_type_mapping(cast(1 as bigint))";
        verifyCallOperation(
                sql,
                "CALL PROCEDURE:"
                        + " (procedureIdentifier: [`p1`.`system`.`different_type_mapping`],"
                        + " inputTypes: [BIGINT], outputTypes: [BIGINT], arguments: [1])");

        // test call the procedure which has var arguments
        sql = "call `system`.var_arg(1)";
        verifyCallOperation(
                sql,
                "CALL PROCEDURE:"
                        + " (procedureIdentifier: [`p1`.`system`.`var_arg`],"
                        + " inputTypes: [INT NOT NULL], outputTypes: [STRING], arguments: [1])");
        sql = "call `system`.var_arg(1, 2)";
        verifyCallOperation(
                sql,
                "CALL PROCEDURE:"
                        + " (procedureIdentifier: [`p1`.`system`.`var_arg`],"
                        + " inputTypes: [INT NOT NULL, INT NOT NULL], outputTypes: [STRING], arguments: [1, 2])");
        sql = "call `system`.var_arg(1, 2, 1 + 2)";
        verifyCallOperation(
                sql,
                "CALL PROCEDURE:"
                        + " (procedureIdentifier: [`p1`.`system`.`var_arg`],"
                        + " inputTypes: [INT NOT NULL, INT NOT NULL, INT NOT NULL], outputTypes: [STRING], arguments: [1, 2, 3])");

        // test call the procedure with row as result and decimal as argument as well as
        // explict/implicit cast
        sql = "call `system`.row_result(cast(1.2 as decimal))";
        verifyCallOperation(
                sql,
                "CALL PROCEDURE:"
                        + " (procedureIdentifier: [`p1`.`system`.`row_result`],"
                        + " inputTypes: [DECIMAL(10, 2)], outputTypes: [ROW<`i` INT>], arguments: [1.20])");
        sql = "call `system`.row_result(1.2)";
        verifyCallOperation(
                sql,
                "CALL PROCEDURE:"
                        + " (procedureIdentifier: [`p1`.`system`.`row_result`],"
                        + " inputTypes: [DECIMAL(10, 2)], outputTypes: [ROW<`i` INT>], arguments: [1.20])");

        // test call the procedure with pojo as result
        sql = "call p1.`system`.pojo_result('name', 1)";
        verifyCallOperation(
                sql,
                "CALL PROCEDURE:"
                        + " (procedureIdentifier: [`p1`.`system`.`pojo_result`],"
                        + " inputTypes: [STRING, BIGINT NOT NULL],"
                        + " outputTypes: [*org.apache.flink.table.planner.operations.SqlNodeToCallOperationTest$MyPojo<`name` STRING, `id` BIGINT NOT NULL>*],"
                        + " arguments: [name, 1])");

        // test call the procedure with timestamp as arguments
        sql =
                "call p1.`system`.timestamp_arg(timestamp '2023-04-22 00:00:00.300', "
                        + "timestamp '2023-04-22 00:00:00.300' +  INTERVAL '1' day ) ";
        verifyCallOperation(
                sql,
                String.format(
                        "CALL PROCEDURE:"
                                + " (procedureIdentifier: [`p1`.`system`.`timestamp_arg`],"
                                + " inputTypes: [TIMESTAMP(3), TIMESTAMP(3)], outputTypes: [TIMESTAMP(3)],"
                                + " arguments: [%s, %s])",
                        LocalDateTime.parse("2023-04-22T00:00:00.300"),
                        LocalDateTime.parse("2023-04-23T00:00:00.300")));
        // should throw exception when the signature doesn't match
        assertThatThrownBy(() -> parse("call `system`.primitive_arg(1)"))
                .hasMessageContaining(
                        "No match found for function signature primitive_arg(<NUMERIC>)");

        // should throw exception when the expression argument can't be reduced
        // to literal
        assertThatThrownBy(() -> parse("call `system`.row_result(cast((1.2 + 2.4) as decimal))"))
                .hasMessageContaining(
                        "The argument at position 0 CAST(CAST(1.2 + 2.4 AS DECIMAL) AS DECIMAL(10, 2)) for calling procedure can't be converted to literal.");
    }

    private void verifyCallOperation(String sql, String expectSummary) {
        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(PlannerCallProcedureOperation.class);
        assertThat(parse(sql).asSummaryString()).isEqualTo(expectSummary);
    }

    /** A catalog with some built-in procedures for testing purpose. */
    private static class CatalogWithBuiltInProcedure extends GenericInMemoryCatalog {

        private static final Map<ObjectPath, Procedure> PROCEDURE_MAP = new HashMap<>();

        static {
            PROCEDURE_MAP.put(
                    ObjectPath.fromString("system.primitive_arg"), new ProcedureWithPrimitiveArg());
            PROCEDURE_MAP.put(
                    ObjectPath.fromString("system.different_type_mapping"),
                    new DifferentTypeMappingProcedure());
            PROCEDURE_MAP.put(ObjectPath.fromString("system.var_arg"), new VarArgProcedure());
            PROCEDURE_MAP.put(ObjectPath.fromString("system.row_result"), new RowResultProcedure());
            PROCEDURE_MAP.put(
                    ObjectPath.fromString("system.pojo_result"), new PojoResultProcedure());
            PROCEDURE_MAP.put(
                    ObjectPath.fromString("system.timestamp_arg"), new TimeStampArgProcedure());
        }

        public CatalogWithBuiltInProcedure(String name) {
            super(name);
        }

        @Override
        public Procedure getProcedure(ObjectPath procedurePath)
                throws ProcedureNotExistException, CatalogException {
            if (PROCEDURE_MAP.containsKey(procedurePath)) {
                return PROCEDURE_MAP.get(procedurePath);
            } else {
                throw new ProcedureNotExistException(getName(), procedurePath);
            }
        }
    }

    private static class ProcedureWithPrimitiveArg implements Procedure {
        public int[] call(ProcedureContext context, int arg1, long arg2) {
            return null;
        }
    }

    @ProcedureHint(input = @DataTypeHint("INT"), output = @DataTypeHint("INT"))
    @ProcedureHint(input = @DataTypeHint("BIGINT"), output = @DataTypeHint("BIGINT"))
    private static class DifferentTypeMappingProcedure implements Procedure {
        public Number[] call(ProcedureContext procedureContext, Number n) {
            return null;
        }
    }

    private static class VarArgProcedure implements Procedure {
        public String[] call(ProcedureContext procedureContext, int i, int... more) {
            return null;
        }
    }

    private static class RowResultProcedure implements Procedure {
        public @DataTypeHint("ROW<i INT>") Row[] call(
                ProcedureContext procedureContext,
                @DataTypeHint("DECIMAL(10, 2)") BigDecimal decimal) {
            return null;
        }
    }

    private static class PojoResultProcedure implements Procedure {
        public MyPojo[] call(ProcedureContext procedureContext, String name, long id) {
            return new MyPojo[0];
        }
    }

    private static class TimeStampArgProcedure implements Procedure {
        public @DataTypeHint("TIMESTAMP(3)") LocalDateTime[] call(
                ProcedureContext procedureContext,
                @DataTypeHint("TIMESTAMP(3)") LocalDateTime localDateTime,
                @DataTypeHint("TIMESTAMP(3)") TimestampData timestampData) {
            return null;
        }
    }

    /** A simple pojo class for testing purpose. */
    public static class MyPojo {
        private final String name;
        private final long id;

        public MyPojo(String name, long id) {
            this.name = name;
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public long getId() {
            return id;
        }
    }
}
