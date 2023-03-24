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

package org.apache.flink.table.planner.codegen;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.testutils.FlinkMatchers;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.codegen.sort.ComparatorCodeGenerator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.plan.utils.JoinUtil;
import org.apache.flink.table.runtime.generated.HashFunction;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.HamcrestCondition.matching;

/** Tests for code generations with code splitting. */
public class CodeSplitTest {

    private final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

    @Test
    public void testJoinCondition() {
        int numFields = 200;

        FlinkTypeFactory typeFactory = new FlinkTypeFactory(classLoader, FlinkTypeSystem.INSTANCE);
        RexBuilder builder = new RexBuilder(typeFactory);
        RelDataType intType = typeFactory.createFieldTypeFromLogicalType(new IntType());
        RexNode[] conditions = new RexNode[numFields];
        for (int i = 0; i < numFields; i++) {
            conditions[i] =
                    builder.makeCall(
                            SqlStdOperatorTable.LESS_THAN,
                            new RexInputRef(i, intType),
                            new RexInputRef(numFields + i, intType));
        }
        RexNode joinCondition = builder.makeCall(SqlStdOperatorTable.AND, conditions);
        RowType rowType = getIntRowType(numFields);

        GenericRowData rowData1 = new GenericRowData(numFields);
        GenericRowData rowData2 = new GenericRowData(numFields);
        Random random = new Random();
        for (int i = 0; i < numFields; i++) {
            rowData1.setField(i, 0);
            rowData2.setField(i, 1);
        }
        boolean result = random.nextBoolean();
        if (!result) {
            rowData1.setField(random.nextInt(numFields), 1);
        }

        Consumer<ReadableConfig> consumer =
                tableConfig -> {
                    JoinCondition instance =
                            JoinUtil.generateConditionFunction(
                                            tableConfig,
                                            Thread.currentThread().getContextClassLoader(),
                                            joinCondition,
                                            rowType,
                                            rowType)
                                    .newInstance(classLoader);
                    for (int i = 0; i < 100; i++) {
                        assertThat(instance.apply(rowData1, rowData2)).isEqualTo(result);
                    }
                };
        runTest(consumer);
    }

    @Test
    public void testHashFunction() {
        int numFields = 1000;

        RowType rowType = getIntRowType(numFields);
        int[] hashFields = new int[numFields];
        for (int i = 0; i < numFields; i++) {
            hashFields[i] = i;
        }

        GenericRowData rowData = new GenericRowData(numFields);
        for (int i = 0; i < numFields; i++) {
            rowData.setField(i, i);
        }

        Consumer<ReadableConfig> consumer =
                tableConfig -> {
                    HashFunction instance =
                            HashCodeGenerator.generateRowHash(
                                            new CodeGeneratorContext(
                                                    tableConfig,
                                                    Thread.currentThread().getContextClassLoader()),
                                            rowType,
                                            "",
                                            hashFields)
                                    .newInstance(classLoader);
                    for (int i = 0; i < 100; i++) {
                        assertThat(instance.hashCode(rowData)).isEqualTo(-1433414860);
                    }
                };
        runTest(consumer);
    }

    @Test
    public void testRecordComparator() {
        int numFields = 600;

        RowType rowType = getIntRowType(numFields);
        SortSpec.SortSpecBuilder builder = SortSpec.builder();
        for (int i = 0; i < numFields; i++) {
            builder.addField(i, true, true);
        }
        SortSpec sortSpec = builder.build();

        GenericRowData rowData1 = new GenericRowData(numFields);
        GenericRowData rowData2 = new GenericRowData(numFields);
        Random random = new Random();
        for (int i = 0; i < numFields; i++) {
            int x = random.nextInt(100);
            rowData1.setField(i, x);
            rowData2.setField(i, x);
        }
        int result = random.nextInt(3) - 1;
        if (result == -1) {
            rowData1.setField(random.nextInt(numFields), -1);
        } else if (result == 1) {
            rowData1.setField(random.nextInt(numFields), 100);
        }

        Consumer<ReadableConfig> consumer =
                tableConfig -> {
                    RecordComparator instance =
                            ComparatorCodeGenerator.gen(
                                            tableConfig,
                                            Thread.currentThread().getContextClassLoader(),
                                            "",
                                            rowType,
                                            sortSpec)
                                    .newInstance(classLoader);
                    for (int i = 0; i < 100; i++) {
                        assertThat(instance.compare(rowData1, rowData2)).isEqualTo(result);
                    }
                };
        runTest(consumer);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testProjection() {
        int numFields = 1000;

        RowType rowType = getIntRowType(numFields);
        List<Integer> order = new ArrayList<>();
        for (int i = 0; i < numFields; i++) {
            order.add(i);
        }
        Collections.shuffle(order);

        GenericRowData input = new GenericRowData(numFields);
        for (int i = 0; i < numFields; i++) {
            input.setField(i, i);
        }
        BinaryRowData output = new BinaryRowData(numFields);
        BinaryRowWriter outputWriter = new BinaryRowWriter(output);
        for (int i = 0; i < numFields; i++) {
            outputWriter.writeInt(i, order.get(i));
        }
        outputWriter.complete();

        Consumer<ReadableConfig> consumer =
                tableConfig -> {
                    Projection instance =
                            ProjectionCodeGenerator.generateProjection(
                                            new CodeGeneratorContext(
                                                    tableConfig,
                                                    Thread.currentThread().getContextClassLoader()),
                                            "",
                                            rowType,
                                            rowType,
                                            order.stream().mapToInt(i -> i).toArray())
                                    .newInstance(classLoader);
                    for (int i = 0; i < 100; i++) {
                        assertThat(instance.apply(input)).isEqualTo(output);
                    }
                };
        runTest(consumer);
    }

    private RowType getIntRowType(int numFields) {
        LogicalType[] fieldTypes = new LogicalType[numFields];
        Arrays.fill(fieldTypes, new IntType());
        return RowType.of(fieldTypes);
    }

    private void runTest(Consumer<ReadableConfig> consumer) {
        Configuration splitTableConfig = new Configuration();
        splitTableConfig.set(TableConfigOptions.MAX_LENGTH_GENERATED_CODE, 4000);
        splitTableConfig.set(TableConfigOptions.MAX_MEMBERS_GENERATED_CODE, 10000);
        consumer.accept(splitTableConfig);

        Configuration noSplitTableConfig = new Configuration();
        noSplitTableConfig.set(TableConfigOptions.MAX_LENGTH_GENERATED_CODE, Integer.MAX_VALUE);
        noSplitTableConfig.set(TableConfigOptions.MAX_MEMBERS_GENERATED_CODE, Integer.MAX_VALUE);
        PrintStream originalStdOut = System.out;
        try {
            // redirect stdout to a null output stream to silence compile error in CompileUtils
            System.setOut(
                    new PrintStream(
                            new OutputStream() {
                                @Override
                                public void write(int b) throws IOException {}
                            }));
            consumer.accept(noSplitTableConfig);
            fail("Expecting compiler exception");
        } catch (Exception e) {
            assertThat(e).satisfies(matching(FlinkMatchers.containsMessage("grows beyond 64 KB")));
        } finally {
            // set stdout back
            System.setOut(originalStdOut);
        }
    }
}
