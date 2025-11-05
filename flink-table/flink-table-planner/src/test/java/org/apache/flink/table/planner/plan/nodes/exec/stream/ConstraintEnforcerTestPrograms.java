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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.ExecutionConfigOptions.NestedEnforcer;
import org.apache.flink.table.api.config.ExecutionConfigOptions.NotNullEnforcer;
import org.apache.flink.table.api.config.ExecutionConfigOptions.TypeLengthEnforcer;
import org.apache.flink.table.runtime.operators.sink.constraint.ConstraintEnforcer;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_SINK_NESTED_CONSTRAINT_ENFORCER;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_SINK_TYPE_LENGTH_ENFORCER;

/** Tests for {@link ConstraintEnforcer}. */
public class ConstraintEnforcerTestPrograms {

    public static final String SCHEMA_NOT_NULL =
            "a ROW<"
                    + "id BIGINT NOT NULL, "
                    + "ab ROW<"
                    + "     aba BIGINT NOT NULL,"
                    // extra column to make it easier to create a row with null as `aba`, without
                    // the
                    // column Row.of(null) can not differentiate if the vararg is null or the column
                    // is null
                    + "     ignore BIGINT"
                    + "> NOT NULL> NOT NULL";
    static final TableTestProgram NOT_NULL_DROP_NESTED_ROWS =
            TableTestProgram.of(
                            "constraint-enforcer-drop-not-null-nested-rows",
                            "validates"
                                    + " constraint enforcer drops records with nulls for NOT NULL"
                                    + " columns in nested rows")
                    .setupConfig(TABLE_EXEC_SINK_NESTED_CONSTRAINT_ENFORCER, NestedEnforcer.ROWS)
                    .setupConfig(TABLE_EXEC_SINK_NOT_NULL_ENFORCER, NotNullEnforcer.DROP)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(SCHEMA_NOT_NULL)
                                    .producedValues(
                                            // should be dropped because a value in a nested ROW is
                                            // null
                                            Row.of(
                                                    Row.of( // a
                                                            1L, // id
                                                            Row.of( // ab
                                                                    null, // aba,
                                                                    1L // ignore
                                                                    ))),
                                            // should be dropped because the entire nested ROW is
                                            // null
                                            Row.of(
                                                    Row.of( // a
                                                            2L, // id
                                                            null)), // ab
                                            // a valid record, which should be kept
                                            Row.of(
                                                    Row.of( // a
                                                            3L, // id
                                                            Row.of( // ab
                                                                    1L, // aba,
                                                                    1L // ignore
                                                                    ))))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SCHEMA_NOT_NULL)
                                    .consumedValues(
                                            Row.of(
                                                    Row.of( // a
                                                            3L, // id
                                                            Row.of( // ab
                                                                    1L, // aba,
                                                                    1L // ignore
                                                                    ))))
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT * FROM source_t")
                    .build();

    static final TableTestProgram NOT_NULL_ERROR_NESTED_ROWS =
            TableTestProgram.of(
                            "constraint-enforcer-error-not-null-nested-rows",
                            "validates"
                                    + " constraint enforcer throws an exception when trying to"
                                    + " write records null into NOT NULL columns in nested rows")
                    .setupConfig(TABLE_EXEC_SINK_NESTED_CONSTRAINT_ENFORCER, NestedEnforcer.ROWS)
                    .setupConfig(TABLE_EXEC_SINK_NOT_NULL_ENFORCER, NotNullEnforcer.ERROR)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(SCHEMA_NOT_NULL)
                                    .producedValues(
                                            // should be dropped because a value in a nested ROW is
                                            // null
                                            Row.of(
                                                    Row.of( // a
                                                            1L, // id
                                                            Row.of( // ab
                                                                    null, // aba,
                                                                    1L // ignore
                                                                    ))),
                                            // should be dropped because the entire nested ROW is
                                            // null
                                            Row.of(
                                                    Row.of( // a
                                                            2L, // id
                                                            null)) // ab
                                            )
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SCHEMA_NOT_NULL)
                                    .consumedValues(new Row[0])
                                    .build())
                    .runFailingSql(
                            "INSERT INTO sink_t SELECT * FROM source_t WHERE `a`.`id` = 1",
                            TableRuntimeException.class,
                            "Column 'a.ab.aba' is NOT NULL, however, a null"
                                    + " value is being written into it. You can set job"
                                    + " configuration 'table.exec.sink.not-null-enforcer'='DROP'"
                                    + " to suppress this exception and drop such records silently.")
                    .runFailingSql(
                            "INSERT INTO sink_t SELECT * FROM source_t WHERE `a`.`id` = 2",
                            TableRuntimeException.class,
                            "Column 'a.ab' is NOT NULL, however, a null"
                                    + " value is being written into it. You can set job"
                                    + " configuration 'table.exec.sink.not-null-enforcer'='DROP'"
                                    + " to suppress this exception and drop such records silently.")
                    .build();

    public static final String SCHEMA_LENGTH =
            "a ROW<"
                    + "id BIGINT NOT NULL, "
                    + "ab ROW<"
                    + "     aba CHAR(2) NOT NULL,"
                    + "     abb VARCHAR(2) NOT NULL,"
                    + "     abc BINARY(2) NOT NULL,"
                    + "     abd VARBINARY(2) NOT NULL"
                    + "> NOT NULL> NOT NULL";
    static final TableTestProgram LENGTH_TRIM_PAD_NESTED_ROWS =
            TableTestProgram.of(
                            "constraint-enforcer-trim-pad-length-nested-rows",
                            "validates"
                                    + " constraint enforcer trim or pads character and binary strings"
                                    + " in nested rows")
                    .setupConfig(TABLE_EXEC_SINK_NESTED_CONSTRAINT_ENFORCER, NestedEnforcer.ROWS)
                    .setupConfig(TABLE_EXEC_SINK_TYPE_LENGTH_ENFORCER, TypeLengthEnforcer.TRIM_PAD)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(SCHEMA_LENGTH)
                                    .producedValues(
                                            // should be padded
                                            Row.of(
                                                    Row.of( // a
                                                            1L, // aa
                                                            Row.of( // ab
                                                                    "a", // aba
                                                                    "a", // abb
                                                                    new byte[] {1}, // abc
                                                                    new byte[] {1} // abd
                                                                    ))),
                                            // should be trimmed
                                            Row.of(
                                                    Row.of( // a
                                                            2L, // aa
                                                            Row.of( // ab
                                                                    "abc", // aba,
                                                                    "abc", // abb
                                                                    new byte[] {1, 2, 3}, // abc
                                                                    new byte[] {1, 2, 3} // abd
                                                                    ))), // ab
                                            // a valid record, which should be kept
                                            Row.of(
                                                    Row.of( // a
                                                            3L, // aa
                                                            Row.of( // ab
                                                                    "ab", // aba,
                                                                    "ab", // abb
                                                                    new byte[] {1, 2}, // abc
                                                                    new byte[] {1, 2} // abd
                                                                    ))))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SCHEMA_LENGTH)
                                    .consumedValues(
                                            // should be padded
                                            Row.of(
                                                    Row.of( // a
                                                            1L, // aa
                                                            Row.of( // ab
                                                                    "a ", // aba
                                                                    "a", // abb
                                                                    new byte[] {1, 0}, // abc
                                                                    new byte[] {1} // abd
                                                                    ))),
                                            // should be trimmed
                                            Row.of(
                                                    Row.of( // a
                                                            2L, // aa
                                                            Row.of( // ab
                                                                    "ab", // aba,
                                                                    "ab", // abb
                                                                    new byte[] {1, 2}, // abc
                                                                    new byte[] {1, 2} // abd
                                                                    ))), // ab
                                            // a valid record, which should be kept
                                            Row.of(
                                                    Row.of( // a
                                                            3L, // aa
                                                            Row.of( // ab
                                                                    "ab", // aba,
                                                                    "ab", // abb
                                                                    new byte[] {1, 2}, // abc
                                                                    new byte[] {1, 2} // abd
                                                                    ))))
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT * FROM source_t")
                    .build();

    static final TableTestProgram LENGTH_ERROR_NESTED_ROWS =
            TableTestProgram.of(
                            "constraint-enforcer-error-length-nested-rows",
                            "validates"
                                    + " constraint enforcer errors or invalid length of character"
                                    + " and binary strings in nested rows")
                    .setupConfig(TABLE_EXEC_SINK_NESTED_CONSTRAINT_ENFORCER, NestedEnforcer.ROWS)
                    .setupConfig(TABLE_EXEC_SINK_TYPE_LENGTH_ENFORCER, TypeLengthEnforcer.ERROR)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(SCHEMA_LENGTH)
                                    .producedValues(
                                            // should be padded CHAR
                                            Row.of(
                                                    Row.of( // a
                                                            1L, // id
                                                            Row.of( // ab
                                                                    "a", // aba
                                                                    "a", // abb
                                                                    new byte[] {1, 2}, // abc
                                                                    new byte[] {1} // abd
                                                                    ))),
                                            // should be padded BINARY
                                            Row.of(
                                                    Row.of( // a
                                                            2L, // id
                                                            Row.of( // ab
                                                                    "ab", // aba
                                                                    "a", // abb
                                                                    new byte[] {1}, // abc
                                                                    new byte[] {1} // abd
                                                                    ))),
                                            // should be trimmed CHAR
                                            Row.of(
                                                    Row.of( // a
                                                            3L, // id
                                                            Row.of( // ab
                                                                    "abc", // aba,
                                                                    "ab", // abb
                                                                    new byte[] {1, 2}, // abc
                                                                    new byte[] {1, 2} // abd
                                                                    ))), // ab
                                            // should be trimmed BINARY
                                            Row.of(
                                                    Row.of( // a
                                                            4L, // id
                                                            Row.of( // ab
                                                                    "ab", // aba,
                                                                    "ab", // abb
                                                                    new byte[] {1, 2, 3}, // abc
                                                                    new byte[] {1, 2} // abd
                                                                    ))), // ab
                                            // should be trimmed VARCHAR
                                            Row.of(
                                                    Row.of( // a
                                                            5L, // id
                                                            Row.of( // ab
                                                                    "ab", // aba,
                                                                    "abc", // abb
                                                                    new byte[] {1, 2}, // abc
                                                                    new byte[] {1, 2} // abd
                                                                    ))), // ab
                                            // should be trimmed VARBINARY
                                            Row.of(
                                                    Row.of( // a
                                                            6L, // id
                                                            Row.of( // ab
                                                                    "ab", // aba,
                                                                    "ab", // abb
                                                                    new byte[] {1, 2}, // abc
                                                                    new byte[] {1, 2, 3} // abd
                                                                    ))) // ab
                                            )
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SCHEMA_LENGTH)
                                    .consumedValues(new Row[0])
                                    .build())
                    .runFailingSql(
                            "INSERT INTO sink_t SELECT * FROM source_t WHERE `a`.`id` = 1",
                            TableRuntimeException.class,
                            "Column 'a.ab.aba' is CHAR(2), however, a string of"
                                    + " length 1 is being written into it. You can set job"
                                    + " configuration 'table.exec.sink.type-length-enforcer' to"
                                    + " control this behaviour.")
                    .runFailingSql(
                            "INSERT INTO sink_t SELECT * FROM source_t WHERE `a`.`id` = 2",
                            TableRuntimeException.class,
                            "Column 'a.ab.abc' is BINARY(2), however, a string of"
                                    + " length 1 is being written into it. You can set job"
                                    + " configuration 'table.exec.sink.type-length-enforcer' to"
                                    + " control this behaviour.")
                    .runFailingSql(
                            "INSERT INTO sink_t SELECT * FROM source_t WHERE `a`.`id` = 3",
                            TableRuntimeException.class,
                            "Column 'a.ab.aba' is CHAR(2), however, a string of"
                                    + " length 3 is being written into it. You can set job"
                                    + " configuration 'table.exec.sink.type-length-enforcer' to"
                                    + " control this behaviour.")
                    .runFailingSql(
                            "INSERT INTO sink_t SELECT * FROM source_t WHERE `a`.`id` = 4",
                            TableRuntimeException.class,
                            "Column 'a.ab.abc' is BINARY(2), however, a string of"
                                    + " length 3 is being written into it. You can set job"
                                    + " configuration 'table.exec.sink.type-length-enforcer' to"
                                    + " control this behaviour.")
                    .runFailingSql(
                            "INSERT INTO sink_t SELECT * FROM source_t WHERE `a`.`id` = 5",
                            TableRuntimeException.class,
                            "Column 'a.ab.abb' is VARCHAR(2), however, a string of"
                                    + " length 3 is being written into it. You can set job"
                                    + " configuration 'table.exec.sink.type-length-enforcer' to"
                                    + " control this behaviour.")
                    .runFailingSql(
                            "INSERT INTO sink_t SELECT * FROM source_t WHERE `a`.`id` = 6",
                            TableRuntimeException.class,
                            "Column 'a.ab.abd' is VARBINARY(2), however, a string of"
                                    + " length 3 is being written into it. You can set job"
                                    + " configuration 'table.exec.sink.type-length-enforcer' to"
                                    + " control this behaviour.")
                    .build();

    public static final String SCHEMA_ARRAYS_NOT_NULL =
            "a ROW<"
                    + "id BIGINT NOT NULL, "
                    + "ab ARRAY<"
                    + " ARRAY<BIGINT NOT NULL> NOT NULL"
                    + "> NOT NULL> NOT NULL";
    static final TableTestProgram NOT_NULL_DROP_NESTED_ARRAYS =
            TableTestProgram.of(
                            "constraint-enforcer-drop-not-null-nested-arrays",
                            "validates"
                                    + " constraint enforcer drops records with nulls for NOT NULL"
                                    + " columns in nested arrays")
                    .setupConfig(
                            TABLE_EXEC_SINK_NESTED_CONSTRAINT_ENFORCER,
                            NestedEnforcer.ROWS_AND_COLLECTIONS)
                    .setupConfig(TABLE_EXEC_SINK_NOT_NULL_ENFORCER, NotNullEnforcer.DROP)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(SCHEMA_ARRAYS_NOT_NULL)
                                    .producedValues(
                                            // should be dropped because a value in a nested ARRAY
                                            // is null
                                            Row.of(
                                                    Row.of( // a
                                                            1L, // id
                                                            new Long[][] {new Long[] {null}} // ab
                                                            )),
                                            // should be dropped because the entire nested ARRAY is
                                            // null
                                            Row.of(
                                                    Row.of( // a
                                                            2L, // id
                                                            new Long[][] {null})), // ab
                                            // a valid record, which should be kept
                                            Row.of(
                                                    Row.of( // a
                                                            3L, // id
                                                            new Long[][] {new Long[] {1L}} // ab
                                                            )))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SCHEMA_ARRAYS_NOT_NULL)
                                    .consumedValues(
                                            Row.of(
                                                    Row.of( // a
                                                            3L, // id
                                                            new Long[][] {new Long[] {1L}} // ab
                                                            )))
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT * FROM source_t")
                    .build();

    static final TableTestProgram NOT_NULL_ERROR_NESTED_ARRAYS =
            TableTestProgram.of(
                            "constraint-enforcer-error-not-null-nested-arrays",
                            "validates"
                                    + " constraint enforcer throws an exception for records with"
                                    + " nulls for NOT NULL elements in nested arrays")
                    .setupConfig(
                            TABLE_EXEC_SINK_NESTED_CONSTRAINT_ENFORCER,
                            NestedEnforcer.ROWS_AND_COLLECTIONS)
                    .setupConfig(TABLE_EXEC_SINK_NOT_NULL_ENFORCER, NotNullEnforcer.ERROR)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(SCHEMA_ARRAYS_NOT_NULL)
                                    .producedValues(
                                            // should be dropped because a value in a nested ARRAY
                                            // is null
                                            Row.of(
                                                    Row.of( // a
                                                            1L, // id
                                                            new Long[][] {new Long[] {null}} // ab
                                                            )),
                                            // should be dropped because the entire nested ARRAY is
                                            // null
                                            Row.of(
                                                    Row.of( // a
                                                            2L, // id
                                                            new Long[][] {null})) // ab
                                            )
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SCHEMA_ARRAYS_NOT_NULL)
                                    .consumedValues(new Row[0])
                                    .build())
                    .runFailingSql(
                            "INSERT INTO sink_t SELECT * FROM source_t WHERE `a`.`id` = 1",
                            TableRuntimeException.class,
                            "Column 'a.ab[0][0]' is NOT NULL, however, a null"
                                    + " value is being written into it. You can set job"
                                    + " configuration 'table.exec.sink.not-null-enforcer'='DROP'"
                                    + " to suppress this exception and drop such records silently.")
                    .runFailingSql(
                            "INSERT INTO sink_t SELECT * FROM source_t WHERE `a`.`id` = 2",
                            TableRuntimeException.class,
                            "Column 'a.ab[0]' is NOT NULL, however, a null"
                                    + " value is being written into it. You can set job"
                                    + " configuration 'table.exec.sink.not-null-enforcer'='DROP'"
                                    + " to suppress this exception and drop such records silently.")
                    .build();

    public static final String SCHEMA_MAPS_NOT_NULL =
            "a ROW<"
                    + "id BIGINT NOT NULL, "
                    + "ab MAP<BIGINT NOT NULL, BIGINT NOT NULL> NOT NULL"
                    + "> NOT NULL";
    static final TableTestProgram NOT_NULL_DROP_NESTED_MAPS =
            TableTestProgram.of(
                            "constraint-enforcer-drop-not-null-nested-maps",
                            "validates"
                                    + " constraint enforcer drops records with nulls for NOT NULL"
                                    + " columns in nested maps")
                    .setupConfig(
                            TABLE_EXEC_SINK_NESTED_CONSTRAINT_ENFORCER,
                            NestedEnforcer.ROWS_AND_COLLECTIONS)
                    .setupConfig(TABLE_EXEC_SINK_NOT_NULL_ENFORCER, NotNullEnforcer.DROP)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(SCHEMA_MAPS_NOT_NULL)
                                    .producedValues(
                                            // should be dropped because the key in a nested MAP
                                            // is null
                                            Row.of(
                                                    Row.of( // a
                                                            1L, // id
                                                            mapOfNullable(null, 1L) // ab
                                                            )),
                                            // should be dropped because the value in a nested MAP
                                            // is null
                                            Row.of(
                                                    Row.of( // a
                                                            2L, // id
                                                            mapOfNullable(1L, null) // ab
                                                            )),
                                            // a valid record, which should be kept
                                            Row.of(
                                                    Row.of( // a
                                                            3L, // id
                                                            mapOfNullable(1L, 1L) // ab
                                                            )))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SCHEMA_MAPS_NOT_NULL)
                                    .consumedValues(
                                            Row.of(
                                                    Row.of( // a
                                                            3L, // id
                                                            mapOfNullable(1L, 1L) // ab
                                                            )))
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT * FROM source_t")
                    .build();

    static final TableTestProgram NOT_NULL_ERROR_NESTED_MAPS =
            TableTestProgram.of(
                            "constraint-enforcer-error-not-null-nested-maps",
                            "validates"
                                    + " constraint enforcer throws an exception when writing nulls"
                                    + " into in nested maps which require NOT NULL values and/or"
                                    + " keys")
                    .setupConfig(
                            TABLE_EXEC_SINK_NESTED_CONSTRAINT_ENFORCER,
                            NestedEnforcer.ROWS_AND_COLLECTIONS)
                    .setupConfig(TABLE_EXEC_SINK_NOT_NULL_ENFORCER, NotNullEnforcer.ERROR)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(SCHEMA_MAPS_NOT_NULL)
                                    .producedValues(
                                            // should be dropped because the key in a nested MAP
                                            // is null
                                            Row.of(
                                                    Row.of( // a
                                                            1L, // id
                                                            mapOfNullable(null, 1L) // ab
                                                            )),
                                            // should be dropped because the value in a nested MAP
                                            // is null
                                            Row.of(
                                                    Row.of( // a
                                                            2L, // id
                                                            mapOfNullable(1L, null) // ab
                                                            )))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SCHEMA_MAPS_NOT_NULL)
                                    .consumedValues(new Row[0])
                                    .build())
                    .runFailingSql(
                            "INSERT INTO sink_t SELECT * FROM source_t WHERE `a`.`id` = 1",
                            TableRuntimeException.class,
                            "Column 'a.ab[key]' is NOT NULL, however, a null"
                                    + " value is being written into it. You can set job"
                                    + " configuration 'table.exec.sink.not-null-enforcer'='DROP'"
                                    + " to suppress this exception and drop such records silently.")
                    .runFailingSql(
                            "INSERT INTO sink_t SELECT * FROM source_t WHERE `a`.`id` = 2",
                            TableRuntimeException.class,
                            "Column 'a.ab[value]' is NOT NULL, however, a null"
                                    + " value is being written into it. You can set job"
                                    + " configuration 'table.exec.sink.not-null-enforcer'='DROP'"
                                    + " to suppress this exception and drop such records silently.")
                    .build();

    public static final String SCHEMA_ARRAYS_LENGTH =
            "a ROW<"
                    + "id BIGINT NOT NULL, "
                    + "ab ARRAY<"
                    + " ROW<"
                    + "     c CHAR(2) NOT NULL,"
                    + "     vc VARCHAR(2) NOT NULL,"
                    + "     b BINARY(2) NOT NULL,"
                    + "     vb VARBINARY(2) NOT NULL"
                    + " > NOT NULL"
                    + "> NOT NULL> NOT NULL";
    static final TableTestProgram LENGTH_TRIM_PAD_WITH_NESTED_COLLECTIONS =
            TableTestProgram.of(
                            "constraint-enforcer-trim-pad-nested-collections",
                            "validates"
                                    + " constraint enforcer does not work with trim padding if"
                                    + " checks for nested collections is enabled")
                    .setupConfig(
                            TABLE_EXEC_SINK_NESTED_CONSTRAINT_ENFORCER,
                            NestedEnforcer.ROWS_AND_COLLECTIONS)
                    .setupConfig(TABLE_EXEC_SINK_TYPE_LENGTH_ENFORCER, TypeLengthEnforcer.TRIM_PAD)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(SCHEMA_ARRAYS_LENGTH)
                                    .producedValues(
                                            Row.of(
                                                    Row.of( // a
                                                            1L, // id
                                                            new Row[] {} // ab
                                                            )))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SCHEMA_ARRAYS_LENGTH)
                                    .consumedValues(new Row[0])
                                    .build())
                    .runFailingSql(
                            "INSERT INTO sink_t SELECT * FROM source_t",
                            ValidationException.class,
                            "Trimming and/or padding is not supported if"
                                    + " constraint checking is enabled on types nested"
                                    + " in collections.")
                    .build();

    static final TableTestProgram LENGTH_ERROR_WITH_NESTED_ARRAYS =
            TableTestProgram.of(
                            "constraint-enforcer-error-nested-arrays",
                            "validates"
                                    + " constraint enforcer throws an exception if character or"
                                    + " binary string of invalid lengths are written into elements"
                                    + " of an array")
                    .setupConfig(
                            TABLE_EXEC_SINK_NESTED_CONSTRAINT_ENFORCER,
                            NestedEnforcer.ROWS_AND_COLLECTIONS)
                    .setupConfig(TABLE_EXEC_SINK_TYPE_LENGTH_ENFORCER, TypeLengthEnforcer.ERROR)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(SCHEMA_ARRAYS_LENGTH)
                                    .producedValues(
                                            Row.of(
                                                    Row.of( // a
                                                            1L, // id
                                                            new Row[] {
                                                                Row.of(
                                                                        "a", // c
                                                                        "a", // vc
                                                                        new byte[] {1, 2}, // b
                                                                        new byte[] {1} // vb
                                                                        )
                                                            } // ab
                                                            )),
                                            Row.of(
                                                    Row.of( // a
                                                            2L, // id
                                                            new Row[] {
                                                                Row.of(
                                                                        "ab", // c
                                                                        "abc", // vc
                                                                        new byte[] {1, 2}, // b
                                                                        new byte[] {1} // vb
                                                                        )
                                                            } // ab
                                                            )),
                                            Row.of(
                                                    Row.of( // a
                                                            3L, // id
                                                            new Row[] {
                                                                Row.of(
                                                                        "ab", // c
                                                                        "a", // vc
                                                                        new byte[] {1}, // b
                                                                        new byte[] {1} // vb
                                                                        )
                                                            } // ab
                                                            )),
                                            Row.of(
                                                    Row.of( // a
                                                            4L, // id
                                                            new Row[] {
                                                                Row.of(
                                                                        "ab", // c
                                                                        "a", // vc
                                                                        new byte[] {1, 2}, // b
                                                                        new byte[] {1, 2, 3} // vb
                                                                        )
                                                            } // ab
                                                            )))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SCHEMA_ARRAYS_LENGTH)
                                    .consumedValues(new Row[0])
                                    .build())
                    .runFailingSql(
                            "INSERT INTO sink_t SELECT * FROM source_t WHERE id = 1",
                            TableRuntimeException.class,
                            "Column 'a.ab[0].c' is CHAR(2), however, a string"
                                    + " of length 1 is being written into it. You can set job"
                                    + " configuration 'table.exec.sink.type-length-enforcer' to"
                                    + " control this behaviour")
                    .runFailingSql(
                            "INSERT INTO sink_t SELECT * FROM source_t WHERE id = 2",
                            TableRuntimeException.class,
                            "Column 'a.ab[0].vc' is VARCHAR(2), however, a string"
                                    + " of length 3 is being written into it. You can set job"
                                    + " configuration 'table.exec.sink.type-length-enforcer' to"
                                    + " control this behaviour")
                    .runFailingSql(
                            "INSERT INTO sink_t SELECT * FROM source_t WHERE id = 3",
                            TableRuntimeException.class,
                            "Column 'a.ab[0].b' is BINARY(2), however, a string"
                                    + " of length 1 is being written into it. You can set job"
                                    + " configuration 'table.exec.sink.type-length-enforcer' to"
                                    + " control this behaviour")
                    .runFailingSql(
                            "INSERT INTO sink_t SELECT * FROM source_t WHERE id = 4",
                            TableRuntimeException.class,
                            "Column 'a.ab[0].vb' is VARBINARY(2), however, a string"
                                    + " of length 3 is being written into it. You can set job"
                                    + " configuration 'table.exec.sink.type-length-enforcer' to"
                                    + " control this behaviour")
                    .build();

    public static final String SCHEMA_MAPS_LENGTH =
            "a ROW<"
                    + "id BIGINT NOT NULL, "
                    + "ac MAP<"
                    + "     CHAR(2) NOT NULL,"
                    + "     VARCHAR(2) NOT NULL"
                    + " > NOT NULL, "
                    + "ab MAP<"
                    + "     BINARY(2) NOT NULL,"
                    + "     VARBINARY(2) NOT NULL"
                    + " > NOT NULL "
                    + "> NOT NULL";
    static final TableTestProgram LENGTH_ERROR_WITH_NESTED_MAPS =
            TableTestProgram.of(
                            "constraint-enforcer-error-nested-maps",
                            "validates"
                                    + " constraint enforcer throws an exception if character or"
                                    + " binary string of invalid lengths are written into elements"
                                    + " of a map")
                    .setupConfig(
                            TABLE_EXEC_SINK_NESTED_CONSTRAINT_ENFORCER,
                            NestedEnforcer.ROWS_AND_COLLECTIONS)
                    .setupConfig(TABLE_EXEC_SINK_TYPE_LENGTH_ENFORCER, TypeLengthEnforcer.ERROR)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(SCHEMA_MAPS_LENGTH)
                                    .producedValues(
                                            // key which is BINARY is too short
                                            Row.of(
                                                    Row.of( // a
                                                            1L, // id
                                                            Map.of("a", "a"), // ac
                                                            Map.of(
                                                                    new byte[] {1, 2},
                                                                    new byte[] {1}) // ab
                                                            )),
                                            // value which is VARCHAR is too long
                                            Row.of(
                                                    Row.of( // a
                                                            2L, // id
                                                            Map.of("ab", "abc"), // ac
                                                            Map.of(
                                                                    new byte[] {1, 2},
                                                                    new byte[] {1}) // ab
                                                            )),
                                            // key which is BINARY is too short
                                            Row.of(
                                                    Row.of( // a
                                                            3L, // id
                                                            Map.of("ab", "a"), // ac
                                                            Map.of(
                                                                    new byte[] {1},
                                                                    new byte[] {1}) // ab
                                                            )),
                                            // value which is VARBINARY is too long
                                            Row.of(
                                                    Row.of( // a
                                                            4L, // id
                                                            Map.of("ab", "a"), // ac
                                                            Map.of(
                                                                    new byte[] {1, 2},
                                                                    new byte[] {1, 2, 3}) // ab
                                                            )))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SCHEMA_MAPS_LENGTH)
                                    .consumedValues(new Row[0])
                                    .build())
                    .runFailingSql(
                            "INSERT INTO sink_t SELECT * FROM source_t WHERE id = 1",
                            TableRuntimeException.class,
                            "Column 'a.ac[key]' is CHAR(2), however, a string"
                                    + " of length 1 is being written into it. You can set job"
                                    + " configuration 'table.exec.sink.type-length-enforcer' to"
                                    + " control this behaviour")
                    .runFailingSql(
                            "INSERT INTO sink_t SELECT * FROM source_t WHERE id = 2",
                            TableRuntimeException.class,
                            "Column 'a.ac[value]' is VARCHAR(2), however, a string"
                                    + " of length 3 is being written into it. You can set job"
                                    + " configuration 'table.exec.sink.type-length-enforcer' to"
                                    + " control this behaviour")
                    .runFailingSql(
                            "INSERT INTO sink_t SELECT * FROM source_t WHERE id = 3",
                            TableRuntimeException.class,
                            "Column 'a.ab[key]' is BINARY(2), however, a string"
                                    + " of length 1 is being written into it. You can set job"
                                    + " configuration 'table.exec.sink.type-length-enforcer' to"
                                    + " control this behaviour")
                    .runFailingSql(
                            "INSERT INTO sink_t SELECT * FROM source_t WHERE id = 4",
                            TableRuntimeException.class,
                            "Column 'a.ab[value]' is VARBINARY(2), however, a string"
                                    + " of length 3 is being written into it. You can set job"
                                    + " configuration 'table.exec.sink.type-length-enforcer' to"
                                    + " control this behaviour")
                    .build();

    private static Map<Long, Long> mapOfNullable(@Nullable Long key, @Nullable Long value) {
        final Map<Long, Long> map = new HashMap<>();
        map.put(key, value);
        return map;
    }
}
