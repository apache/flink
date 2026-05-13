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

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.State;
import org.apache.flink.runtime.event.WatermarkEvent;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalProcessTableFunction;
import org.apache.flink.table.planner.utils.PlannerMocks;
import org.apache.flink.table.runtime.generated.GeneratedProcessTableRunner;
import org.apache.flink.table.runtime.generated.HashFunction;
import org.apache.flink.table.runtime.generated.ProcessTableRunner;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.operators.process.PassAllCollector;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ProcessTableRunnerGenerator}. */
public class ProcessTableRunnerGeneratorTest {

    private PlannerMocks plannerMocks;

    @BeforeEach
    public void before() {
        plannerMocks = PlannerMocks.create();
        plannerMocks.registerTemporaryTable(
                "Names", Schema.newBuilder().column("name", DataTypes.STRING()).build());
        plannerMocks
                .getFunctionCatalog()
                .registerTemporarySystemFunction("ptfLong", new LongScalarFunction(), false);
        plannerMocks
                .getFunctionCatalog()
                .registerTemporarySystemFunction("ptfInt", new IntScalarFunction(), false);
        plannerMocks
                .getFunctionCatalog()
                .registerTemporarySystemFunction("ptfDouble", new DoubleScalarFunction(), false);
        plannerMocks
                .getFunctionCatalog()
                .registerTemporarySystemFunction("ptfDecimal", new DecimalScalarFunction(), false);
        plannerMocks
                .getFunctionCatalog()
                .registerTemporarySystemFunction("ptfMixed", new MixedScalarFunction(), false);
        plannerMocks
                .getFunctionCatalog()
                .registerTemporarySystemFunction(
                        "ptfRowLong", new RowSemanticLongFunction(), false);
    }

    @Test
    public void testIntLiteralToLongArg() {
        // An INT literal (100) flowing into a BIGINT parameter requires the cast to be inserted.
        final RowData out =
                runAndCollectSingle(
                        "SELECT * FROM ptfLong(TABLE Names PARTITION BY name, 100)", rowOf("Bob"));
        assertThat(out.getString(0)).hasToString("Bob");
        assertThat(out.getString(1)).hasToString("long=100");
    }

    @Test
    public void testSmallIntLiteralToIntArg() {
        final RowData out =
                runAndCollectSingle(
                        "SELECT * FROM ptfInt(TABLE Names PARTITION BY name, CAST(7 AS SMALLINT))",
                        rowOf("Alice"));
        assertThat(out.getString(0)).hasToString("Alice");
        assertThat(out.getString(1)).hasToString("int=7");
    }

    @Test
    public void testTinyIntLiteralToLongArg() {
        final RowData out =
                runAndCollectSingle(
                        "SELECT * FROM ptfLong(TABLE Names PARTITION BY name, CAST(3 AS TINYINT))",
                        rowOf("Bob"));
        assertThat(out.getString(0)).hasToString("Bob");
        assertThat(out.getString(1)).hasToString("long=3");
    }

    @Test
    public void testIntLiteralToDoubleArg() {
        final RowData out =
                runAndCollectSingle(
                        "SELECT * FROM ptfDouble(TABLE Names PARTITION BY name, 42)", rowOf("Bob"));
        assertThat(out.getString(0)).hasToString("Bob");
        assertThat(out.getString(1)).hasToString("double=42.0");
    }

    @Test
    public void testIntLiteralToDecimalArg() {
        final RowData out =
                runAndCollectSingle(
                        "SELECT * FROM ptfDecimal(TABLE Names PARTITION BY name, 5)",
                        rowOf("Alice"));
        assertThat(out.getString(0)).hasToString("Alice");
        assertThat(out.getString(1)).hasToString("decimal=5.00");
    }

    @Test
    public void testFloatLiteralToDoubleArg() {
        final RowData out =
                runAndCollectSingle(
                        "SELECT * FROM ptfDouble(TABLE Names PARTITION BY name, CAST(1.5 AS FLOAT))",
                        rowOf("Bob"));
        assertThat(out.getString(0)).hasToString("Bob");
        assertThat(out.getString(1)).hasToString("double=1.5");
    }

    @Test
    public void testMultipleMixedScalarArgs() {
        final RowData out =
                runAndCollectSingle(
                        "SELECT * FROM ptfMixed(TABLE Names PARTITION BY name, 10, 2.5, 'x')",
                        rowOf("Bob"));
        assertThat(out.getString(0)).hasToString("Bob");
        assertThat(out.getString(1)).hasToString("10/2.5/x");
    }

    @Test
    public void testMatchingTypeNeedsNoCast() {
        // Sanity check: when the literal type already matches the parameter type, the generator
        // still produces a runner that emits the correct value (no-op cast path).
        final RowData out =
                runAndCollectSingle(
                        "SELECT * FROM ptfLong(TABLE Names PARTITION BY name, CAST(99 AS BIGINT))",
                        rowOf("Alice"));
        assertThat(out.getString(0)).hasToString("Alice");
        assertThat(out.getString(1)).hasToString("long=99");
    }

    @Test
    public void testNullableLiteralPassedAsNull() {
        // Cover the null-handling path inside the generated cast.
        final RowData out =
                runAndCollectSingle(
                        "SELECT * FROM ptfLong(TABLE Names PARTITION BY name, CAST(NULL AS INT))",
                        rowOf("Bob"));
        assertThat(out.getString(0)).hasToString("Bob");
        assertThat(out.getString(1)).hasToString("long=null");
    }

    @Test
    public void testRowSemanticPtfIntLiteralToLongArg() {
        final RowData out =
                runAndCollectSingle(
                        "SELECT * FROM ptfRowLong(r => TABLE Names, arg => 100)", rowOf("Bob"));
        assertThat(out.getString(0)).hasToString("Bob");
        assertThat(out.getString(1)).hasToString("row-long=100");
    }

    /**
     * Plans the PTF query, generates the runner, drives a single input row through it, and returns
     * the single emitted output row. The generated code is compiled (Janino) when the runner is
     * instantiated, then exercised end-to-end through {@link ProcessTableRunner#processEval()} so
     * we observe the value the cast actually delivered to the user's {@code eval} method.
     */
    private RowData runAndCollectSingle(String sql, RowData input) {
        final List<RowData> outputs = runAndCollect(sql, input);
        assertThat(outputs).hasSize(1);
        return outputs.get(0);
    }

    private List<RowData> runAndCollect(String sql, RowData input) {
        final RexCall udfCall = planAndExtractUdfCall(sql);

        final CodeGeneratorContext ctx =
                new CodeGeneratorContext(
                        plannerMocks.getTableConfig(),
                        Thread.currentThread().getContextClassLoader());

        final ProcessTableRunnerGenerator.GeneratedRunnerResult result =
                ProcessTableRunnerGenerator.generate(
                        ctx,
                        udfCall,
                        List.of(-1),
                        List.of(ChangelogMode.insertOnly()),
                        ChangelogMode.insertOnly());

        final GeneratedProcessTableRunner generated = result.runner();
        final ProcessTableRunner runner =
                generated.newInstance(Thread.currentThread().getContextClassLoader());

        final List<RowData> outputs = new ArrayList<>();
        final Output<StreamRecord<RowData>> output = new CollectingOutput(outputs);
        // prefixRepetition = 1 because we drive a single (set-semantic) table argument.
        final PassAllCollector evalCollector =
                new PassAllCollector(output, ChangelogMode.insertOnly(), 1);
        final PassAllCollector onTimerCollector =
                new PassAllCollector(output, ChangelogMode.insertOnly(), 1);

        try {
            // The test PTFs don't take a Context argument and have no state, so we can pass null
            // contexts and empty state arrays - the generated code references none of these.
            runner.setRuntimeContext(new MockStreamingRuntimeContext(1, 0));
            runner.initialize(
                    new State[0],
                    new HashFunction[0],
                    new RecordEqualiser[0],
                    false,
                    null,
                    null,
                    evalCollector,
                    onTimerCollector);
            runner.open(new OpenContext() {});
            runner.ingestTableEvent(0, input, -1, 0L);
            runner.processEval();
        } catch (Exception e) {
            throw new RuntimeException("Failed to run generated runner", e);
        }
        return outputs;
    }

    private RexCall planAndExtractUdfCall(String sql) {
        final FlinkPlannerImpl planner = plannerMocks.getPlanner();
        final SqlNode parsed = planner.parser().parse(sql);
        final SqlNode validated = planner.validate(parsed);
        final RelRoot root = planner.rel(validated);
        final LogicalTableFunctionScan scan = findTableFunctionScan(root.rel);
        assertThat(scan).as("LogicalTableFunctionScan in plan").isNotNull();

        final RexCall fullCall = (RexCall) scan.getCall();
        // Apply the same call-trait resolution that StreamExecProcessTableFunction's JsonCreator
        // applies during compiled-plan restore - keeps the udfCall identical to the runtime path.
        final RexCall resolved = BridgingSqlFunction.resolveCallTraits(fullCall);
        return StreamPhysicalProcessTableFunction.toUdfCall(resolved);
    }

    private static LogicalTableFunctionScan findTableFunctionScan(RelNode node) {
        if (node instanceof LogicalTableFunctionScan) {
            return (LogicalTableFunctionScan) node;
        }
        for (RelNode child : node.getInputs()) {
            final LogicalTableFunctionScan found = findTableFunctionScan(child);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private static GenericRowData rowOf(String name) {
        return GenericRowData.of(StringData.fromString(name));
    }

    // --------------------------------------------------------------------------------------------
    // Minimal Output that captures emitted rows for assertions
    // --------------------------------------------------------------------------------------------

    private static final class CollectingOutput implements Output<StreamRecord<RowData>> {
        private final List<RowData> sink;

        CollectingOutput(List<RowData> sink) {
            this.sink = sink;
        }

        @Override
        public void collect(StreamRecord<RowData> record) {
            // The PassAllCollector reuses internal JoinedRowData / RepeatedRowData buffers, so we
            // snapshot the row's fields into a fresh GenericRowData. The set-up here always emits
            // [name STRING, function_result STRING] (rowtime is appended as an empty 0-arity row).
            final RowData row = record.getValue();
            final GenericRowData copy = new GenericRowData(row.getRowKind(), row.getArity());
            for (int i = 0; i < row.getArity(); i++) {
                copy.setField(i, row.isNullAt(i) ? null : row.getString(i));
            }
            sink.add(copy);
        }

        @Override
        public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void emitWatermark(Watermark mark) {}

        @Override
        public void emitWatermark(WatermarkEvent watermark) {}

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {}

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) {}

        @Override
        public void emitRecordAttributes(RecordAttributes recordAttributes) {}

        @Override
        public void close() {}
    }

    // --------------------------------------------------------------------------------------------
    // Test PTFs
    // --------------------------------------------------------------------------------------------

    /** PTF that takes a {@code long} scalar argument. */
    public static class LongScalarFunction extends ProcessTableFunction<String> {
        public void eval(@ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input, Long arg) {
            collect("long=" + arg);
        }
    }

    /** PTF that takes an {@code int} scalar argument. */
    public static class IntScalarFunction extends ProcessTableFunction<String> {
        public void eval(@ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input, Integer arg) {
            collect("int=" + arg);
        }
    }

    /** PTF that takes a {@code double} scalar argument. */
    public static class DoubleScalarFunction extends ProcessTableFunction<String> {
        public void eval(@ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input, Double arg) {
            collect("double=" + arg);
        }
    }

    /** PTF that takes a {@code DECIMAL(10, 2)} scalar argument. */
    public static class DecimalScalarFunction extends ProcessTableFunction<String> {
        public void eval(
                @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input,
                @DataTypeHint("DECIMAL(10, 2)") BigDecimal arg) {
            collect("decimal=" + arg);
        }
    }

    /** PTF that takes scalar args of several different types in one call. */
    public static class MixedScalarFunction extends ProcessTableFunction<String> {
        public void eval(
                @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input,
                Long longArg,
                Double doubleArg,
                String stringArg) {
            collect(longArg + "/" + doubleArg + "/" + stringArg);
        }
    }

    /** Row-semantic PTF that takes a {@code long} scalar argument. */
    public static class RowSemanticLongFunction extends ProcessTableFunction<String> {
        public void eval(@ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) Row r, Long arg) {
            collect("row-long=" + arg);
        }
    }
}
