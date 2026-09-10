/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.python.scalar.arrow;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.util.ProtoUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.functions.python.PythonFunctionKind;
import org.apache.flink.table.functions.python.PythonScalarFunction;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ProjectionCodeGenerator;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.operators.python.scalar.PythonScalarFunctionOperatorTestBase;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.runtime.utils.PassThroughPythonScalarFunctionRunner;
import org.apache.flink.table.runtime.utils.PythonTestUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.Collection;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ArrowPythonScalarFunctionOperator}. */
public class ArrowPythonScalarFunctionOperatorTest
        extends PythonScalarFunctionOperatorTestBase<RowData, RowData, RowData> {

    private final RowDataHarnessAssertor assertor =
            new RowDataHarnessAssertor(
                    new LogicalType[] {
                        DataTypes.STRING().getLogicalType(),
                        DataTypes.STRING().getLogicalType(),
                        DataTypes.BIGINT().getLogicalType()
                    });

    @ParameterizedTest
    @EnumSource(
            value = PythonFunctionKind.class,
            names = {"PANDAS", "ARROW"})
    void testScalarBatchFormat(PythonFunctionKind kind) {
        final RowType rowType = RowType.of(DataTypes.STRING().getLogicalType());
        final PythonFunctionInfo function =
                new PythonFunctionInfo(
                        new PythonScalarFunction(
                                "identity",
                                new byte[0],
                                kind,
                                true,
                                false,
                                new PythonEnv(PythonEnv.ExecType.PROCESS)),
                        new Object[] {0},
                        kind == PythonFunctionKind.ARROW
                                ? DataTypes.STRING().notNull().getLogicalType()
                                : null);
        final ArrowPythonScalarFunctionOperator operator =
                getTestOperator(
                        new Configuration(),
                        new PythonFunctionInfo[] {function},
                        rowType,
                        rowType,
                        new int[] {0},
                        new int[0]);
        final FlinkFnApi.CoderInfoDescriptor.ArrowType.BatchFormat expected =
                FlinkFnApi.CoderInfoDescriptor.ArrowType.BatchFormat.valueOf(kind.name());
        assertThat(operator.createInputCoderInfoDescriptor(rowType).getArrowType().getBatchFormat())
                .isEqualTo(expected);
        assertThat(
                        operator.createOutputCoderInfoDescriptor(rowType)
                                .getArrowType()
                                .getBatchFormat())
                .isEqualTo(expected);
        assertThat(ProtoUtils.createUserDefinedFunctionProto(function).getIsArrowUdf())
                .isEqualTo(kind == PythonFunctionKind.ARROW);
        final FlinkFnApi.UserDefinedFunction functionProto =
                ProtoUtils.createUserDefinedFunctionProto(function);
        assertThat(functionProto.hasOutputType()).isEqualTo(kind == PythonFunctionKind.ARROW);
        if (kind == PythonFunctionKind.ARROW) {
            assertThat(functionProto.getOutputType().getTypeName())
                    .isEqualTo(FlinkFnApi.Schema.TypeName.VARCHAR);
            assertThat(functionProto.getOutputType().getNullable()).isFalse();
        }
        assertThat(
                        ProtoUtils.createArrowTypeCoderInfoDescriptorProto(
                                        rowType, FlinkFnApi.CoderInfoDescriptor.Mode.SINGLE, false)
                                .getArrowType()
                                .getBatchFormat())
                .isEqualTo(FlinkFnApi.CoderInfoDescriptor.ArrowType.BatchFormat.PANDAS);
    }

    @Override
    public ArrowPythonScalarFunctionOperator getTestOperator(
            Configuration config,
            PythonFunctionInfo[] scalarFunctions,
            RowType inputType,
            RowType outputType,
            int[] udfInputOffsets,
            int[] forwardedFields) {
        final RowType udfInputType = (RowType) Projection.of(udfInputOffsets).project(inputType);
        final RowType forwardedFieldType =
                (RowType) Projection.of(forwardedFields).project(inputType);
        final RowType udfOutputType =
                (RowType)
                        Projection.range(forwardedFields.length, outputType.getFieldCount())
                                .project(outputType);

        return new PassThroughRowDataArrowPythonScalarFunctionOperator(
                config,
                scalarFunctions,
                inputType,
                udfInputType,
                udfOutputType,
                ProjectionCodeGenerator.generateProjection(
                        new CodeGeneratorContext(
                                new Configuration(),
                                Thread.currentThread().getContextClassLoader()),
                        "UdfInputProjection",
                        inputType,
                        udfInputType,
                        udfInputOffsets),
                ProjectionCodeGenerator.generateProjection(
                        new CodeGeneratorContext(
                                new Configuration(),
                                Thread.currentThread().getContextClassLoader()),
                        "ForwardedFieldProjection",
                        inputType,
                        forwardedFieldType,
                        forwardedFields));
    }

    @Override
    public RowData newRow(boolean accumulateMsg, Object... fields) {
        if (accumulateMsg) {
            return row(fields);
        } else {
            RowData row = row(fields);
            row.setRowKind(RowKind.DELETE);
            return row;
        }
    }

    @Override
    public void assertOutputEquals(
            String message, Collection<Object> expected, Collection<Object> actual) {
        assertor.assertOutputEquals(message, expected, actual);
    }

    @Override
    public StreamTableEnvironment createTableEnvironment(StreamExecutionEnvironment env) {
        return StreamTableEnvironment.create(env);
    }

    @Override
    public TypeSerializer<RowData> getOutputTypeSerializer(RowType rowType) {
        return new RowDataSerializer(rowType);
    }

    private static class PassThroughRowDataArrowPythonScalarFunctionOperator
            extends ArrowPythonScalarFunctionOperator {

        PassThroughRowDataArrowPythonScalarFunctionOperator(
                Configuration config,
                PythonFunctionInfo[] scalarFunctions,
                RowType inputType,
                RowType udfInputType,
                RowType udfOutputType,
                GeneratedProjection udfInputGeneratedProjection,
                GeneratedProjection forwardedFieldGeneratedProjection) {
            super(
                    config,
                    scalarFunctions,
                    inputType,
                    udfInputType,
                    udfOutputType,
                    udfInputGeneratedProjection,
                    forwardedFieldGeneratedProjection);
        }

        @Override
        public PythonFunctionRunner createPythonFunctionRunner() throws IOException {
            return new PassThroughPythonScalarFunctionRunner(
                    getContainingTask().getEnvironment(),
                    getRuntimeContext().getTaskInfo().getTaskName(),
                    PythonTestUtils.createTestProcessEnvironmentManager(),
                    udfInputType,
                    udfOutputType,
                    getFunctionUrn(),
                    createUserDefinedFunctionsProto(),
                    PythonTestUtils.createMockFlinkMetricContainer());
        }
    }
}
