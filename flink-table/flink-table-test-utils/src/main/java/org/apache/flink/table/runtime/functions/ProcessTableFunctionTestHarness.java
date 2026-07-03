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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.extraction.ExtractionUtils;
import org.apache.flink.table.types.inference.StateTypeStrategy;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.table.types.inference.SystemTypeInference;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.utils.DateTimeUtils;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import org.apache.commons.lang3.ClassUtils;

import javax.annotation.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Test harness for {@link ProcessTableFunction}.
 *
 * <p>Provides a fluent builder API for configuring and testing ProcessTableFunctions (PTFs) with
 * table and scalar arguments, lifecycle management, and output collection.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * ProcessTableFunctionTestHarness<Row> harness =
 *     ProcessTableFunctionTestHarness.ofClass(MyPTF.class)
 *         .withTableArgument("input", DataTypes.of("ROW<id INT, name STRING>"))
 *         .withScalarArgument("threshold", 100)
 *         .build();
 *
 * harness.processElement(Row.of(1, "Alice"));
 * harness.processElement(Row.of(2, "Bob"));
 *
 * List<Row> output = harness.getOutput();
 * List<Row> functionOutput = harness.getFunctionOutput();
 * }</pre>
 */
@PublicEvolving
public class ProcessTableFunctionTestHarness<OUT> implements AutoCloseable {

    /** Holds converters for transforming table argument input rows. */
    private static class TableArgumentConverters {
        final DataStructureConverter<Object, Object> toNamedRow;
        final DataStructureConverter<Object, Object> toEvalArgument;

        TableArgumentConverters(
                DataStructureConverter<Object, Object> toNamedRow,
                DataStructureConverter<Object, Object> toEvalArgument) {
            this.toNamedRow = toNamedRow;
            this.toEvalArgument = toEvalArgument;
        }
    }

    private final ProcessTableFunction<OUT> function;
    private final FunctionContext functionContext;
    private final List<OUT> functionOutput;
    private final List<Row> output;
    private boolean isOpen;
    private final HarnessCollector collector;
    private final TestHarnessStateManager stateManager;

    private final String defaultTableArgument;
    private final ResolvedMethod<ProcessTableFunction.Context> eval;
    @Nullable private final ResolvedMethod<ProcessTableFunction.OnTimerContext> onTimer;
    private final List<ArgumentInfo> arguments;

    private final Map<String, ArgumentInfo> argumentsByName;
    private final boolean isSingleTableFunction;

    private boolean hasTableArguments = false;
    private final Map<String, TableArgumentConverters> argumentConverters;
    private final DataStructureConverter<Object, Object> harnessOutputConverter;

    private final OutputKind outputKind;
    @Nullable private final DataStructureConverter<Object, Object> compositeOutputToInternal;
    @Nullable private final DataStructureConverter<Object, Object> compositeOutputToRow;

    private final TestHarnessTimerManager timerManager;
    @Nullable private final String onTimeColumnName;

    @Nullable private final Class<?> rowtimeConversionClass;

    @Nullable private InvocationContext currentInvocation;

    private ProcessTableFunctionTestHarness(
            ProcessTableFunction<OUT> function,
            FunctionContext functionContext,
            ResolvedMethod<ProcessTableFunction.Context> eval,
            @Nullable ResolvedMethod<ProcessTableFunction.OnTimerContext> onTimer,
            List<ArgumentInfo> arguments,
            Map<String, TableArgumentConverters> argumentConverters,
            DataStructureConverter<Object, Object> harnessOutputConverter,
            DataType ptfOutputType,
            TestHarnessStateManager stateManager,
            TestHarnessTimerManager timerManager,
            @Nullable String onTimeColumnName)
            throws Exception {
        this.function = function;
        this.functionContext = functionContext;
        this.eval = eval;
        this.onTimer = onTimer;
        this.arguments = arguments;
        this.argumentConverters = argumentConverters;
        this.harnessOutputConverter = harnessOutputConverter;
        if (!LogicalTypeChecks.isCompositeType(ptfOutputType.getLogicalType())) {
            this.outputKind = OutputKind.ATOMIC;
        } else if (ptfOutputType.getLogicalType() instanceof RowType) {
            this.outputKind = OutputKind.ROW;
        } else {
            this.outputKind = OutputKind.STRUCTURED;
        }
        if (this.outputKind != OutputKind.ATOMIC) {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            DataType rowOutputType =
                    ptfOutputType.getLogicalType() instanceof StructuredType
                            ? toRowDataType((StructuredType) ptfOutputType.getLogicalType())
                            : ptfOutputType;
            this.compositeOutputToInternal = DataStructureConverters.getConverter(ptfOutputType);
            this.compositeOutputToRow = DataStructureConverters.getConverter(rowOutputType);

            this.compositeOutputToInternal.open(classLoader);
            this.compositeOutputToRow.open(classLoader);
        } else {
            this.compositeOutputToInternal = null;
            this.compositeOutputToRow = null;
        }
        this.stateManager = stateManager;
        this.timerManager = timerManager;
        this.onTimeColumnName = onTimeColumnName;
        this.functionOutput = new ArrayList<>();
        this.output = new ArrayList<>();
        this.collector = new HarnessCollector();
        this.isOpen = false;

        this.argumentsByName = new HashMap<>();
        for (ArgumentInfo arg : arguments) {
            if (arg.name != null) {
                argumentsByName.put(arg.name, arg);
            }
        }

        final List<TableArgumentInfo> tableArguments = ArgumentInfo.filterTableArguments(arguments);
        this.hasTableArguments = !tableArguments.isEmpty();

        if (tableArguments.size() == 1) {
            this.defaultTableArgument = tableArguments.get(0).name;
            this.isSingleTableFunction = true;
        } else {
            this.defaultTableArgument = null;
            this.isSingleTableFunction = false;
        }

        this.rowtimeConversionClass = resolveRowtimeConversionClass(tableArguments);

        openFunction();
    }

    /** Creates a new harness builder for the given ProcessTableFunction class. */
    public static <OUT> Builder<OUT> ofClass(
            Class<? extends ProcessTableFunction<OUT>> functionClass) {
        return new Builder<>(functionClass);
    }

    private void openFunction() throws Exception {
        function.open(functionContext);
        function.setCollector(collector);
        isOpen = true;
    }

    @Override
    public void close() throws Exception {
        if (isOpen) {
            function.close();
            isOpen = false;
        }
    }

    /**
     * Process a single element for the default table argument.
     *
     * <p>For PTFs with a single table argument, this processes one row. For multiple table
     * arguments, use {@link #processElementForTable(String, Row)}.
     */
    public void processElement(Row row) throws Exception {
        if (!isSingleTableFunction) {
            throw new IllegalStateException(
                    "PTF has multiple table arguments. Use processElementForTable(argumentName, row) "
                            + "to specify which table argument should receive the row.");
        }

        processElementForTable(defaultTableArgument, row);
    }

    /** Process a single element constructed from values. */
    public void processElement(Object... values) throws Exception {
        processElement(Row.of(values));
    }

    /** Process a single element with a specific RowKind. */
    public void processElement(RowKind rowKind, Object... values) throws Exception {
        processElement(Row.ofKind(rowKind, values));
    }

    /** Process a single element for a specific table argument. */
    public void processElementForTable(String tableArgument, Row row) throws Exception {
        checkState(isOpen, "Harness not open");
        checkNotNull(tableArgument, "tableArgument must not be null");

        ArgumentInfo arg = argumentsByName.get(tableArgument);
        if (arg == null) {
            throw new IllegalArgumentException("Unknown table argument: " + tableArgument);
        }
        if (!(arg instanceof TableArgumentInfo)) {
            throw new IllegalArgumentException("'" + tableArgument + "' is not a table argument");
        }
        invokeEval((TableArgumentInfo) arg, row);
    }

    /** Process a single element for a specific table argument. */
    public void processElementForTable(String tableArgument, Object... values) throws Exception {
        processElementForTable(tableArgument, Row.of(values));
    }

    /** Process a single element for a specific table argument with RowKind. */
    public void processElementForTable(String tableArgument, RowKind rowKind, Object... values)
            throws Exception {
        processElementForTable(tableArgument, Row.ofKind(rowKind, values));
    }

    /**
     * Processes the PTF's eval() method with scalar arguments only.
     *
     * <p>This method is specifically for scalar-only PTFs (PTFs with only scalar arguments and no
     * table arguments). For PTFs that accept table arguments, use {@link #processElement(Row)} or
     * {@link #processElementForTable(String, Row)} instead.
     *
     * @throws IllegalStateException if the PTF has any table arguments
     * @throws Exception if the eval() invocation fails
     */
    public void process() throws Exception {
        checkState(isOpen, "Harness not open");

        if (hasTableArguments) {
            throw new IllegalStateException(
                    "process() is only for scalar-only PTFs. This PTF has table arguments. "
                            + "Use processElement() or processElementForTable() instead.");
        }

        Object[] args = arguments.stream().map(arg -> ((ScalarArgumentInfo) arg).value).toArray();

        try {
            currentInvocation = InvocationContext.forScalarOnlyEval();
            eval.invoke(function, new TestContext(new HashMap<>()), args);
        } catch (InvocationTargetException e) {
            handleEvalInvocationException(
                    "Exception occurred during scalar-only PTF eval() invocation.\n", args, e);
        } finally {
            currentInvocation = null;
        }
    }

    /**
     * Returns the collected output as full rows: atomic output wrapped into an {@code EXPR$0}
     * column or structured output flattened into its attributes, with additional columns (partition
     * keys, rowtime, etc.) prepended or appended as configured.
     */
    public List<Row> getOutput() {
        return List.copyOf(output);
    }

    /**
     * Returns the collected output as values collected by the PTF, typed as its declared output.
     * See {@link #getOutput()} for the full row the runtime would emit, including those additional
     * columns.
     */
    public List<OUT> getFunctionOutput() {
        return List.copyOf(functionOutput);
    }

    /** Clears all collected output. */
    public void clearOutput() {
        functionOutput.clear();
        output.clear();
    }

    /** Get state for a specific partition key. */
    public <T> T getStateForKey(String stateName, Row partitionKey) {
        return stateManager.getStateForKey(stateName, partitionKey);
    }

    /** Set state for a specific partition key. */
    public void setStateForKey(String stateName, Row partitionKey, Object state) throws Exception {
        stateManager.setStateForKey(stateName, partitionKey, state);
    }

    /** Get all partition keys that have a specific state entry. */
    public Set<Row> getKeysForState(String stateName) {
        return stateManager.getKeysForState(stateName);
    }

    /** Get all state values for a state name across all partition keys. */
    public <T> Map<Row, T> getStateForAllKeys(String stateName) {
        return stateManager.getStateForAllKeys(stateName);
    }

    /** Clear all state for a given partition key. */
    public void clearAllStatesForKey(Row partitionKey) {
        stateManager.clearAllStatesForKey(partitionKey);
    }

    /** Clear specific state entry for a given partition key. */
    public void clearStateForKey(String stateName, Row partitionKey) {
        stateManager.clearStateForKey(stateName, partitionKey);
    }

    // -------------------------------------------------------------------------
    // Watermark & Timer API
    // -------------------------------------------------------------------------

    /**
     * Sets the watermark for all tables to the given {@link LocalDateTime} and fires eligible
     * timers.
     */
    public void setWatermark(LocalDateTime watermark) throws Exception {
        checkNotNull(watermark, "watermark must not be null");
        setWatermarkMillis(DateTimeUtils.toTimestampMillis(watermark));
    }

    /** Sets the watermark for all tables to the given {@link Instant} and fires eligible timers. */
    public void setWatermark(Instant watermark) throws Exception {
        checkNotNull(watermark, "watermark must not be null");
        setWatermarkMillis(watermark.toEpochMilli());
    }

    /**
     * Sets the watermark for a specific table. Fires eligible timers if this advances the global
     * watermark (the minimum across all tables).
     */
    public void setWatermarkForTable(String tableArgument, LocalDateTime watermark)
            throws Exception {
        checkNotNull(watermark, "watermark must not be null");
        setWatermarkForTableMillis(tableArgument, DateTimeUtils.toTimestampMillis(watermark));
    }

    /**
     * Sets the watermark for a specific table. Fires eligible timers if this advances the global
     * watermark (the minimum across all tables).
     */
    public void setWatermarkForTable(String tableArgument, Instant watermark) throws Exception {
        checkNotNull(watermark, "watermark must not be null");
        setWatermarkForTableMillis(tableArgument, watermark.toEpochMilli());
    }

    /** Returns all timers (both pending and fired), sorted by timestamp then name. */
    public List<Timer> getAllTimers() {
        return Stream.concat(
                        timerManager.getPendingTimers().stream(),
                        timerManager.getFiredTimers().stream())
                .sorted()
                .collect(Collectors.toList());
    }

    /** Returns all timers (both pending and fired) for the given partition key. */
    public List<Timer> getAllTimers(Row partitionKey) {
        return getAllTimers().stream()
                .filter(t -> partitionKey.equals(t.getKey()))
                .collect(Collectors.toList());
    }

    /** Returns all pending (not yet fired) timers, sorted by timestamp then name. */
    public List<Timer> getPendingTimers() {
        return timerManager.getPendingTimers();
    }

    /** Returns all pending timers for the given partition key. */
    public List<Timer> getPendingTimers(Row partitionKey) {
        return timerManager.getPendingTimers().stream()
                .filter(t -> partitionKey.equals(t.getKey()))
                .collect(Collectors.toList());
    }

    /** Returns all pending timers with the given name. */
    public List<Timer> getPendingTimers(String timerName) {
        return timerManager.getPendingTimers().stream()
                .filter(t -> timerName.equals(t.getName()))
                .collect(Collectors.toList());
    }

    /** Returns all timers that have fired, in the order they fired. */
    public List<Timer> getFiredTimers() {
        return timerManager.getFiredTimers();
    }

    /** Returns all fired timers for the given partition key. */
    public List<Timer> getFiredTimers(Row partitionKey) {
        return timerManager.getFiredTimers().stream()
                .filter(t -> partitionKey.equals(t.getKey()))
                .collect(Collectors.toList());
    }

    /** Returns all fired timers with the given name. */
    public List<Timer> getFiredTimers(String timerName) {
        return timerManager.getFiredTimers().stream()
                .filter(t -> timerName.equals(t.getName()))
                .collect(Collectors.toList());
    }

    /** Clears the fired timer history. */
    public void clearFiredTimers() {
        timerManager.clearFiredTimers();
    }

    private void setWatermarkMillis(long millis) throws Exception {
        checkState(isOpen, "Harness is not open");
        for (TableArgumentInfo tableArg : ArgumentInfo.filterTableArguments(arguments)) {
            timerManager.setTableWatermark(tableArg.name, millis);
        }
        timerManager.updateGlobalWatermarkAndFireTimers(this::fireTimer);
    }

    private void setWatermarkForTableMillis(String tableArgument, long millis) throws Exception {
        checkState(isOpen, "Harness is not open");
        checkNotNull(tableArgument, "tableArgument must not be null");
        checkArgument(
                argumentsByName.get(tableArgument) instanceof TableArgumentInfo,
                "Unknown or non-table argument: %s",
                tableArgument);
        timerManager.setTableWatermark(tableArgument, millis);
        timerManager.updateGlobalWatermarkAndFireTimers(this::fireTimer);
    }

    private void fireTimer(Timer timer) throws Exception {
        if (onTimer == null) {
            throw new IllegalStateException(
                    "Timer fired but no valid onTimer() method is defined in "
                            + function.getClass().getSimpleName());
        }

        currentInvocation = InvocationContext.forTimer(timer);

        Map<String, Object> stateMap = stateManager.loadStateForKey(timer.partitionKey);

        List<StateArgumentInfo> stateArgs = ArgumentInfo.filterStateArguments(arguments);
        Object[] methodArgs = new Object[stateArgs.size()];
        for (int i = 0; i < stateArgs.size(); i++) {
            methodArgs[i] = stateMap.get(stateArgs.get(i).name);
        }

        try {
            onTimer.invoke(function, new TestOnTimerContext(stateMap), methodArgs);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw new RuntimeException("onTimer() invocation failed", e);
        } finally {
            currentInvocation = null;
        }

        stateManager.updateStateForKey(timer.partitionKey, stateMap);
    }

    // -------------------------------------------------------------------------
    // Context implementations
    // -------------------------------------------------------------------------

    private class TestContext implements ProcessTableFunction.Context {
        final Map<String, Object> stateMap;

        TestContext(Map<String, Object> stateMap) {
            this.stateMap = stateMap;
        }

        @Override
        public <TimeType> ProcessTableFunction.TimeContext<TimeType> timeContext(
                Class<TimeType> conversionClass) {
            return new TestTimeContext<>(conversionClass);
        }

        @Override
        public TableSemantics tableSemanticsFor(String argName) {
            List<String> tableArgNames =
                    ArgumentInfo.filterTableArguments(arguments).stream()
                            .map(t -> t.name)
                            .collect(Collectors.toList());
            ArgumentInfo argInfo = argumentsByName.get(argName);
            if (argInfo == null || !(argInfo instanceof TableArgumentInfo)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Argument '%s' is not a table argument. Available table arguments: %s",
                                argName, tableArgNames));
            }
            TableArgumentInfo tableArg = (TableArgumentInfo) argInfo;
            int[] partitionIndices = getPartitionColumnIndices(tableArg);
            int timeColumnIndex =
                    onTimeColumnName != null
                            ? getFieldNames(tableArg.dataType).indexOf(onTimeColumnName)
                            : -1;
            return new TestHarnessTableSemantics(
                    tableArg.dataType, partitionIndices, timeColumnIndex);
        }

        @Override
        public void clearState(String stateName) {
            stateMap.remove(stateName);
        }

        @Override
        public void clearAllState() {
            stateMap.clear();
        }

        @Override
        public void clearAllTimers() {
            timerManager.clearAll(currentInvocation.partitionKey);
        }

        @Override
        public void clearAll() {
            stateMap.clear();
            timerManager.clearAll(currentInvocation.partitionKey);
        }

        @Override
        public ChangelogMode getChangelogMode() {
            return ChangelogMode.insertOnly();
        }
    }

    private class TestTimeContext<TimeType> implements ProcessTableFunction.TimeContext<TimeType> {
        private final Class<TimeType> conversionClass;

        TestTimeContext(Class<TimeType> conversionClass) {
            if (!Set.of(
                            Long.class,
                            long.class,
                            Instant.class,
                            LocalDateTime.class,
                            java.sql.Timestamp.class)
                    .contains(conversionClass)) {
                throw new IllegalArgumentException(
                        "Unsupported time type: " + conversionClass.getName());
            }
            this.conversionClass = conversionClass;
        }

        @Override
        public TimeType time() {
            InvocationContext ctx = currentInvocation;
            if (ctx.isTimerInvocation()) {
                return fromMillis(ctx.firingTimer.timestamp);
            }
            if (ctx.isEvalInvocation() && onTimeColumnName != null) {
                ArgumentInfo argInfo = argumentsByName.get(ctx.tableArgumentName);
                if (argInfo instanceof TableArgumentInfo) {
                    TableArgumentInfo tableArg = (TableArgumentInfo) argInfo;
                    if (!getFieldNames(tableArg.dataType).contains(onTimeColumnName)) {
                        return null;
                    }
                }
                Object timeValue = ctx.row.getField(onTimeColumnName);
                if (timeValue == null) {
                    return null;
                }
                return fromMillis(toMillis(timeValue));
            }
            return null;
        }

        @Override
        public TimeType tableWatermark() {
            InvocationContext ctx = currentInvocation;
            if (!ctx.isEvalInvocation()) {
                return null;
            }
            Long wm = timerManager.getWatermarkForTable(ctx.tableArgumentName);
            return wm != null ? fromMillis(wm) : null;
        }

        @Override
        public TimeType currentWatermark() {
            Long wm = timerManager.getGlobalWatermark();
            return wm != null ? fromMillis(wm) : null;
        }

        @Override
        public void registerOnTime(String name, TimeType time) {
            checkTimersEnabled();
            checkNotNull(name, "Timer name must not be null");
            checkNotNull(time, "Timer timestamp must not be null");
            timerManager.register(currentInvocation.partitionKey, toMillis(time), name);
        }

        @Override
        public void registerOnTime(TimeType time) {
            checkTimersEnabled();
            checkNotNull(time, "Timer timestamp must not be null");
            timerManager.register(currentInvocation.partitionKey, toMillis(time), null);
        }

        @Override
        public void clearTimer(String name) {
            checkNotNull(name, "Timer name must not be null");
            timerManager.clearByName(currentInvocation.partitionKey, name);
        }

        @Override
        public void clearTimer(TimeType time) {
            checkNotNull(time, "Timer timestamp must not be null");
            timerManager.clearByTimestamp(currentInvocation.partitionKey, toMillis(time));
        }

        @Override
        public void clearAllTimers() {
            timerManager.clearAll(currentInvocation.partitionKey);
        }

        private void checkTimersEnabled() {
            boolean enabled =
                    ArgumentInfo.filterTableArguments(arguments).stream()
                            .anyMatch(
                                    t ->
                                            t.isSetSemantic
                                                    && t.prependStrategy
                                                            != OutputPrependStrategy.ALL_COLUMNS);
            if (!enabled) {
                throw new TableRuntimeException(
                        "Timers are not supported in the current PTF declaration. "
                                + "Note that only PTFs that take set semantic tables support timers. "
                                + "Also timers are not available for advanced traits such as "
                                + "supporting pass-through columns or updates.");
            }
        }

        private TimeType fromMillis(long millis) {
            return convertFromMillis(millis, conversionClass);
        }

        private long toMillis(Object time) {
            if (time instanceof Long) {
                return (Long) time;
            } else if (time instanceof Instant) {
                return ((Instant) time).toEpochMilli();
            } else if (time instanceof LocalDateTime) {
                return DateTimeUtils.toTimestampMillis((LocalDateTime) time);
            } else if (time instanceof java.sql.Timestamp) {
                return DateTimeUtils.toInternal((java.sql.Timestamp) time);
            }
            throw new IllegalArgumentException(
                    "Unsupported time type: " + time.getClass().getSimpleName());
        }
    }

    private class TestOnTimerContext extends TestContext
            implements ProcessTableFunction.OnTimerContext {

        TestOnTimerContext(Map<String, Object> stateMap) {
            super(stateMap);
        }

        @Override
        public String currentTimer() {
            if (currentInvocation.isTimerInvocation()) {
                return currentInvocation.firingTimer.getName();
            }
            return null;
        }
    }

    private static int[] getPartitionColumnIndices(TableArgumentInfo arg) {
        if (arg.partitionColumnNames == null || arg.partitionColumnNames.length == 0) {
            return new int[0];
        }
        List<String> fieldNames = getFieldNames(arg.dataType);
        int[] indices = new int[arg.partitionColumnNames.length];
        for (int i = 0; i < arg.partitionColumnNames.length; i++) {
            String colName = arg.partitionColumnNames[i];
            int index = fieldNames.indexOf(colName);
            if (index < 0) {
                throw new IllegalStateException(
                        "Partition column '"
                                + colName
                                + "' not found in table argument. "
                                + "Available fields: "
                                + fieldNames);
            }
            indices[i] = index;
        }
        return indices;
    }

    @Nullable
    private Class<?> resolveRowtimeConversionClass(List<TableArgumentInfo> tableArguments) {
        if (onTimeColumnName == null) {
            return null;
        }
        for (TableArgumentInfo tableArg : tableArguments) {
            List<String> fieldNames = getFieldNames(tableArg.dataType);
            int idx = fieldNames.indexOf(onTimeColumnName);
            if (idx >= 0) {
                return DataType.getFields(tableArg.dataType)
                        .get(idx)
                        .getDataType()
                        .getConversionClass();
            }
        }
        throw new IllegalStateException(
                "Could not resolve rowtime conversion class for column: " + onTimeColumnName);
    }

    private Object rowtimeFromMillis(long millis) {
        return convertFromMillis(millis, rowtimeConversionClass);
    }

    @SuppressWarnings("unchecked")
    private static <T> T convertFromMillis(long millis, Class<T> targetClass) {
        if (targetClass == Long.class || targetClass == long.class) {
            return (T) Long.valueOf(millis);
        } else if (targetClass == Instant.class) {
            return (T) Instant.ofEpochMilli(millis);
        } else if (targetClass == LocalDateTime.class) {
            return (T) DateTimeUtils.toLocalDateTime(millis);
        } else if (targetClass == java.sql.Timestamp.class) {
            return (T) DateTimeUtils.toSQLTimestamp(millis);
        }
        throw new IllegalArgumentException("Unsupported time type: " + targetClass);
    }

    private void invokeEval(TableArgumentInfo activeTableArg, Row activeRow) throws Exception {
        TableArgumentConverters converters = argumentConverters.get(activeTableArg.name);

        RowData rowData = (RowData) converters.toNamedRow.toInternal(activeRow);
        Row namedRow = (Row) converters.toNamedRow.toExternal(rowData);
        Object evalArgument = converters.toEvalArgument.toExternal(rowData);

        Row partitionKey = extractPartitionKey(activeTableArg, namedRow);
        currentInvocation = InvocationContext.forEval(partitionKey, namedRow, activeTableArg.name);

        Map<String, Object> stateMap = stateManager.loadStateForKey(partitionKey);

        Object[] methodArgs = new Object[arguments.size()];
        int i = 0;
        for (ArgumentInfo arg : arguments) {
            if (arg instanceof StateArgumentInfo) {
                methodArgs[i++] = stateMap.get(arg.name);
            } else if (arg instanceof TableArgumentInfo) {
                TableArgumentInfo tableArg = (TableArgumentInfo) arg;
                if (tableArg.name.equals(activeTableArg.name)) {
                    methodArgs[i++] = evalArgument;
                } else {
                    methodArgs[i++] = null;
                }
            } else if (arg instanceof ScalarArgumentInfo) {
                methodArgs[i++] = ((ScalarArgumentInfo) arg).value;
            }
        }

        try {
            eval.invoke(function, new TestContext(stateMap), methodArgs);
            stateManager.updateStateForKey(partitionKey, stateMap);
        } catch (InvocationTargetException e) {
            String partitionInfo =
                    activeTableArg.partitionColumnNames != null
                                    && activeTableArg.partitionColumnNames.length > 0
                            ? String.format(
                                    " (partition columns: %s)",
                                    Arrays.toString(activeTableArg.partitionColumnNames))
                            : "";
            String contextMessage =
                    String.format(
                            "Exception occurred during PTF eval() while processing table argument '%s'%s.\n",
                            activeTableArg.name, partitionInfo);
            handleEvalInvocationException(contextMessage, methodArgs, e);
        } finally {
            currentInvocation = null;
        }
    }

    private Row extractPartitionKey(TableArgumentInfo tableArg, Row row) {
        if (tableArg.partitionColumnNames == null || tableArg.partitionColumnNames.length == 0) {
            return Row.of();
        }

        Object[] keyValues =
                Arrays.stream(tableArg.partitionColumnNames).map(row::getField).toArray();
        return Row.of(keyValues);
    }

    /** Collector implementation that stores output in the harness. */
    private class HarnessCollector implements Collector<OUT> {

        @Override
        @SuppressWarnings("unchecked")
        public void collect(OUT record) {
            Row ptfRow = toPtfOutputRow(record);

            functionOutput.add(outputKind == OutputKind.ROW ? (OUT) ptfRow : record);

            Row finalRecord;
            OutputPrependStrategy strategy = resolvePrependStrategy();
            switch (strategy) {
                case ALL_COLUMNS:
                    finalRecord = prependAllColumns(ptfRow);
                    break;
                case PARTITION_KEYS:
                    finalRecord = prependPartitionKeys(ptfRow);
                    break;
                case NONE:
                    finalRecord = ptfRow;
                    break;
                default:
                    throw new IllegalStateException("Unknown prepend strategy: " + strategy);
            }

            if (onTimeColumnName != null) {
                finalRecord = appendRowtime(finalRecord);
            }

            // Round-trip through the converter so output carries the field structure of the
            // derived output schema.
            output.add(applyOutputConverter(finalRecord));
        }

        private Row toPtfOutputRow(Object record) {
            if (outputKind == OutputKind.ATOMIC) {
                return Row.of(record);
            }
            Object internal = compositeOutputToInternal.toInternalOrNull(record);
            return (Row) compositeOutputToRow.toExternalOrNull(internal);
        }

        private OutputPrependStrategy resolvePrependStrategy() {
            InvocationContext ctx = currentInvocation;
            if (ctx == null) {
                return OutputPrependStrategy.NONE;
            }
            if (ctx.isTimerInvocation()) {
                return OutputPrependStrategy.PARTITION_KEYS;
            }
            if (ctx.isEvalInvocation()) {
                ArgumentInfo argInfo = argumentsByName.get(ctx.tableArgumentName);
                if (argInfo instanceof TableArgumentInfo) {
                    return ((TableArgumentInfo) argInfo).prependStrategy;
                }
            }
            return OutputPrependStrategy.NONE;
        }

        private Row applyOutputConverter(Row record) {
            Object internal = harnessOutputConverter.toInternalOrNull(record);
            return (Row) harnessOutputConverter.toExternalOrNull(internal);
        }

        private Row appendRowtime(Row ptfOutput) {
            return appendField(ptfOutput, resolveRowtimeValue());
        }

        private Object resolveRowtimeValue() {
            InvocationContext ctx = currentInvocation;
            if (ctx.isTimerInvocation()) {
                return rowtimeFromMillis(ctx.firingTimer.timestamp);
            }
            return ctx.row.getField(onTimeColumnName);
        }

        private Row prependPartitionKeys(Row ptfRow) {
            Row partitionKey = currentInvocation.partitionKey;

            int totalPartitionKeyCount = 0;
            for (ArgumentInfo arg : arguments) {
                if (arg instanceof TableArgumentInfo) {
                    TableArgumentInfo tableArg = (TableArgumentInfo) arg;
                    if (tableArg.isSetSemantic && tableArg.partitionColumnNames != null) {
                        totalPartitionKeyCount += tableArg.partitionColumnNames.length;
                    }
                }
            }

            int ptfOutputArity = ptfRow.getArity();
            int totalArity = totalPartitionKeyCount + ptfOutputArity;

            Row result = new Row(ptfRow.getKind(), totalArity);

            int resultIndex = 0;
            for (ArgumentInfo arg : arguments) {
                if (arg instanceof TableArgumentInfo) {
                    TableArgumentInfo tableArg = (TableArgumentInfo) arg;
                    if (tableArg.isSetSemantic && tableArg.partitionColumnNames != null) {
                        for (int i = 0; i < tableArg.partitionColumnNames.length; i++) {
                            result.setField(resultIndex++, partitionKey.getField(i));
                        }
                    }
                }
            }

            for (int i = 0; i < ptfOutputArity; i++) {
                result.setField(resultIndex++, ptfRow.getField(i));
            }

            return result;
        }

        private Row prependAllColumns(Row ptfRow) {
            Row inputRow = currentInvocation.row;
            int inputArity = inputRow.getArity();
            int ptfOutputArity = ptfRow.getArity();
            int totalArity = inputArity + ptfOutputArity;

            Row result = new Row(ptfRow.getKind(), totalArity);

            for (int i = 0; i < inputArity; i++) {
                result.setField(i, inputRow.getField(i));
            }

            for (int i = 0; i < ptfOutputArity; i++) {
                result.setField(inputArity + i, ptfRow.getField(i));
            }

            return result;
        }

        @Override
        public void close() {}
    }

    private static Row appendField(Row row, Object value) {
        Row result = new Row(row.getKind(), row.getArity() + 1);
        for (int i = 0; i < row.getArity(); i++) {
            result.setField(i, row.getField(i));
        }
        result.setField(row.getArity(), value);
        return result;
    }

    /** Extracts field names from RowType or StructuredType. */
    private static List<String> getFieldNames(DataType dataType) {
        LogicalType logicalType = dataType.getLogicalType();
        if (logicalType instanceof RowType) {
            return ((RowType) logicalType)
                    .getFields().stream()
                            .map(RowType.RowField::getName)
                            .collect(Collectors.toList());
        } else if (logicalType instanceof StructuredType) {
            return ((StructuredType) logicalType)
                    .getAttributes().stream()
                            .map(StructuredType.StructuredAttribute::getName)
                            .collect(Collectors.toList());
        }
        throw new IllegalStateException(
                String.format(
                        "Unsupported data type: %s. "
                                + "Only Row and structured types are supported.",
                        dataType));
    }

    /**
     * Builds a {@link RowType} {@link DataType} whose fields mirror a structured type's attributes.
     */
    private static DataType toRowDataType(StructuredType structuredType) {
        List<RowType.RowField> rowFields =
                structuredType.getAttributes().stream()
                        .map(attr -> new RowType.RowField(attr.getName(), attr.getType()))
                        .collect(Collectors.toList());
        return TypeConversions.fromLogicalToDataType(
                new RowType(structuredType.isNullable(), rowFields));
    }

    /**
     * Builder for {@link ProcessTableFunctionTestHarness}.
     *
     * @param <OUT> The output type of the ProcessTableFunction
     */
    @PublicEvolving
    public static class Builder<OUT> {
        private final Class<? extends ProcessTableFunction<OUT>> functionClass;

        private final Map<String, ScalarArgumentConfiguration> scalarArgs = new HashMap<>();
        private final Map<String, TableArgumentConfiguration> tableArgs = new HashMap<>();
        private final Map<String, PartitionConfiguration> partitionConfigs = new HashMap<>();
        private final Map<String, StateArgumentConfiguration> stateArgs = new HashMap<>();
        @Nullable private String onTimeColumnName = null;

        private Builder(Class<? extends ProcessTableFunction<OUT>> functionClass) {
            this.functionClass = checkNotNull(functionClass, "functionClass must not be null");
        }

        private void validateArgumentNotYetConfigured(String argumentName) {
            if (scalarArgs.containsKey(argumentName) || tableArgs.containsKey(argumentName)) {
                throw new IllegalArgumentException("Argument already configured: " + argumentName);
            }
        }

        // ---------------------------------------------------------------------
        // Table & Scalar Arguments
        // ---------------------------------------------------------------------

        /**
         * Configures a table argument with its schema (named argument).
         *
         * <p>Use this for dynamic tables that receive elements during the test. Elements are
         * provided via {@link #processElement(Row)} or {@link #processElementForTable(String,
         * Row)}.
         *
         * @param argumentName The table argument name
         * @param dataType The schema/structure of the table
         */
        public Builder<OUT> withTableArgument(String argumentName, AbstractDataType<?> dataType) {
            checkNotNull(argumentName, "argumentName must not be null");
            checkNotNull(dataType, "dataType must not be null");

            validateArgumentNotYetConfigured(argumentName);

            tableArgs.put(argumentName, new TableArgumentConfiguration(dataType));
            return this;
        }

        /**
         * Configures a table argument without an explicit schema.
         *
         * <p>Use this for structured type arguments where the type can be inferred from the PTF's
         * eval() signature. For Row arguments, use {@link #withTableArgument(String,
         * AbstractDataType)} with an explicit schema.
         *
         * @param argumentName The table argument name
         */
        public Builder<OUT> withTableArgument(String argumentName) {
            checkNotNull(argumentName, "argumentName must not be null");

            validateArgumentNotYetConfigured(argumentName);

            tableArgs.put(argumentName, new TableArgumentConfiguration(null));
            return this;
        }

        /**
         * Configures a scalar (non-table) argument for the PTF's eval() method.
         *
         * <p>Scalar arguments are constant values passed to every eval() invocation, such as
         * thresholds, multipliers, or configuration parameters.
         *
         * @param argumentName Must match the parameter name in eval() or the @ArgumentHint name
         * @param value The value to pass for this argument in all eval() calls
         */
        public Builder<OUT> withScalarArgument(String argumentName, Object value) {
            checkNotNull(argumentName, "argumentName must not be null");

            validateArgumentNotYetConfigured(argumentName);

            ScalarArgumentConfiguration config = new ScalarArgumentConfiguration(value);
            scalarArgs.put(argumentName, config);
            return this;
        }

        /** Sets initial state for a state parameter. */
        public Builder<OUT> withInitialStateForKey(
                String stateName, Row partitionKey, Object state) {
            checkNotNull(stateName, "stateName must not be null");
            checkNotNull(partitionKey, "partitionKey must not be null");
            checkNotNull(state, "state must not be null");

            stateArgs
                    .computeIfAbsent(stateName, k -> new StateArgumentConfiguration())
                    .initialValues
                    .put(partitionKey, state);
            return this;
        }

        // ---------------------------------------------------------------------
        // Partitioning
        // ---------------------------------------------------------------------

        /**
         * Specifies partition columns for a set semantic table.
         *
         * @param argumentName The table argument name
         * @param columnNames The partition column names
         * @return This builder
         */
        public Builder<OUT> withPartitionBy(String argumentName, String... columnNames) {
            checkNotNull(argumentName, "argumentName must not be null");
            checkNotNull(columnNames, "columnNames must not be null");
            checkArgument(columnNames.length > 0, "Must specify at least one column");

            if (partitionConfigs.containsKey(argumentName)) {
                throw new IllegalArgumentException(
                        "Partition config already exists for: " + argumentName);
            }

            PartitionConfiguration config = new PartitionConfiguration(columnNames);
            partitionConfigs.put(argumentName, config);
            return this;
        }

        // ---------------------------------------------------------------------
        // Timer & Watermark Configuration
        // ---------------------------------------------------------------------

        /**
         * Configures the on-time column name for the function.
         *
         * @param columnName The column that carries event time
         */
        public Builder<OUT> withOnTimeColumn(String columnName) {
            checkNotNull(columnName, "columnName must not be null");
            this.onTimeColumnName = columnName;
            return this;
        }

        // ---------------------------------------------------------------------
        // Build
        // ---------------------------------------------------------------------

        /**
         * Builds the test harness.
         *
         * <p>This instantiates the PTF, validates configuration via type inference, creates the
         * FunctionContext, and opens the function.
         *
         * @return The configured test harness
         * @throws Exception If instantiation or opening fails
         */
        public ProcessTableFunctionTestHarness<OUT> build() throws Exception {
            ProcessTableFunction<OUT> function = instantiateFunction();

            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

            DataTypeFactory dataTypeFactory = createDataTypeFactory();
            TypeInference baseTypeInference = function.getTypeInference(dataTypeFactory);
            TypeInference systemTypeInference =
                    SystemTypeInference.of(FunctionKind.PROCESS_TABLE, baseTypeInference);

            List<ArgumentInfo> arguments =
                    extractAndValidateTypeInference(function, systemTypeInference);

            FunctionContext functionContext = new FunctionContext(null, classLoader, null);

            ResolvedMethod<ProcessTableFunction.Context> eval =
                    ResolvedMethod.of(
                            findEvalMethod(functionClass), ProcessTableFunction.Context.class);
            Method onTimerMethod = findOnTimerMethod(functionClass, arguments);
            ResolvedMethod<ProcessTableFunction.OnTimerContext> onTimer =
                    onTimerMethod != null
                            ? ResolvedMethod.of(
                                    onTimerMethod, ProcessTableFunction.OnTimerContext.class)
                            : null;

            validateEvalMethodSupported(eval, arguments);
            validatePartitionConsistency(arguments);
            validateInitialStateKeys(arguments);

            Map<String, TableArgumentConverters> argumentConverters = new HashMap<>();
            Map<String, StateConverter> stateConverters = new HashMap<>();
            createConverters(arguments, argumentConverters, stateConverters, classLoader);

            // Create state manager
            List<StateArgumentInfo> stateArguments = ArgumentInfo.filterStateArguments(arguments);
            TestHarnessStateManager stateManager =
                    new TestHarnessStateManager(
                            stateArguments, stateConverters, extractPartitionKeyInfo(arguments));

            // Populate initial state
            for (Map.Entry<String, StateArgumentConfiguration> entry : stateArgs.entrySet()) {
                String stateName = entry.getKey();
                for (Map.Entry<Row, Object> stateEntry :
                        entry.getValue().initialValues.entrySet()) {
                    stateManager.setStateForKey(
                            stateName, stateEntry.getKey(), stateEntry.getValue());
                }
            }

            // Extract table arguments for output type derivation
            // SystemTypeInference needs table semantics for pass-through column deduplication
            List<TableArgumentInfo> tableArgInfos = ArgumentInfo.filterTableArguments(arguments);

            // The system inference yields the full operator output row (partition keys,
            // pass-through columns, and rowtime); harnessOutputConverter stamps those field names.
            DataType derivedOutputType =
                    deriveOutputType(
                            function,
                            dataTypeFactory,
                            systemTypeInference,
                            arguments,
                            tableArgInfos);

            DataStructureConverter<Object, Object> harnessOutputConverter =
                    createPTFOutputConverter(derivedOutputType);

            // The base inference yields the PTF's own emit type, which decides how a collected
            // value is materialized into a Row: atomic is wrapped, composite is flattened.
            DataType ptfOutputType =
                    deriveOutputType(
                            function, dataTypeFactory, baseTypeInference, arguments, tableArgInfos);

            // Validate onTimeColumn configuration
            if (onTimeColumnName != null) {
                boolean foundInAnyTable =
                        tableArgInfos.stream()
                                .anyMatch(
                                        t -> getFieldNames(t.dataType).contains(onTimeColumnName));
                checkArgument(
                        foundInAnyTable,
                        "withOnTimeColumn references column '%s' which does not exist in any "
                                + "table argument. Available table arguments and their columns: %s",
                        onTimeColumnName,
                        tableArgInfos.stream()
                                .collect(
                                        Collectors.toMap(
                                                t -> t.name, t -> getFieldNames(t.dataType))));
            }

            TestHarnessTimerManager timerManager = new TestHarnessTimerManager();

            return new ProcessTableFunctionTestHarness<>(
                    function,
                    functionContext,
                    eval,
                    onTimer,
                    arguments,
                    argumentConverters,
                    harnessOutputConverter,
                    ptfOutputType,
                    stateManager,
                    timerManager,
                    onTimeColumnName);
        }

        /**
         * Creates converter that enriches PTF output Rows with field names from the derived schema.
         */
        private DataStructureConverter<Object, Object> createPTFOutputConverter(
                DataType derivedOutputType) {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            DataStructureConverter<Object, Object> converter =
                    DataStructureConverters.getConverter(derivedOutputType);
            converter.open(classLoader);
            return converter;
        }

        /**
         * Creates and initializes converters for all table and state arguments.
         *
         * <p>For table arguments with Row types, both converters are the same (between Row and
         * RowData). For structured types, toNamedRow uses Row type (Row to RowData), and
         * toEvalArgument uses the structured type.
         */
        private void createConverters(
                List<ArgumentInfo> arguments,
                Map<String, TableArgumentConverters> argumentConverters,
                Map<String, StateConverter> stateConverters,
                ClassLoader classLoader)
                throws Exception {

            for (StateArgumentInfo stateArg : ArgumentInfo.filterStateArguments(arguments)) {
                StateConverter converter = createStateConverter(stateArg.dataType, classLoader);
                stateConverters.put(stateArg.name, converter);
            }

            for (TableArgumentInfo tableArg : ArgumentInfo.filterTableArguments(arguments)) {
                String converterKey = tableArg.name;

                LogicalType logicalType = tableArg.dataType.getLogicalType();
                boolean isStructuredType =
                        logicalType instanceof StructuredType
                                && ((StructuredType) logicalType)
                                        .getImplementationClass()
                                        .isPresent();

                if (isStructuredType) {
                    DataType rowDataType = toRowDataType((StructuredType) logicalType);

                    DataStructureConverter<Object, Object> toNamedRow =
                            DataStructureConverters.getConverter(rowDataType);
                    toNamedRow.open(classLoader);

                    DataStructureConverter<Object, Object> toEvalArgument =
                            DataStructureConverters.getConverter(tableArg.dataType);
                    toEvalArgument.open(classLoader);

                    argumentConverters.put(
                            converterKey, new TableArgumentConverters(toNamedRow, toEvalArgument));
                } else {
                    DataStructureConverter<Object, Object> converter =
                            DataStructureConverters.getConverter(tableArg.dataType);
                    converter.open(classLoader);

                    argumentConverters.put(
                            converterKey, new TableArgumentConverters(converter, converter));
                }
            }
        }

        private static Method findEvalMethod(Class<?> functionClass) throws NoSuchMethodException {
            Method[] methods = functionClass.getMethods();
            Method evalMethod = null;
            int evalMethodCount = 0;

            for (Method method : methods) {
                if (method.getName().equals("eval")) {
                    evalMethod = method;
                    evalMethodCount++;
                }
            }

            if (evalMethodCount == 0) {
                throw new NoSuchMethodException(
                        "No eval() method found in " + functionClass.getSimpleName());
            } else if (evalMethodCount > 1) {
                throw new IllegalStateException(
                        "Multiple eval() methods found in "
                                + functionClass.getSimpleName()
                                + ". ProcessTableFunction must have exactly one eval() method.");
            }

            return evalMethod;
        }

        @Nullable
        private static Method findOnTimerMethod(
                Class<?> functionClass, List<ArgumentInfo> arguments) {
            List<Method> candidates = ExtractionUtils.collectMethods(functionClass, "onTimer");
            if (candidates.isEmpty()) {
                return null;
            }

            Class<?>[] stateClasses =
                    ArgumentInfo.filterStateArguments(arguments).stream()
                            .map(s -> s.dataType.getConversionClass())
                            .toArray(Class<?>[]::new);

            // Try with OnTimerContext first, then without — mirrors the code generator
            Class<?>[] withContext = new Class<?>[stateClasses.length + 1];
            withContext[0] = ProcessTableFunction.OnTimerContext.class;
            System.arraycopy(stateClasses, 0, withContext, 1, stateClasses.length);

            for (Class<?>[] signature : new Class<?>[][] {withContext, stateClasses}) {
                Optional<Method> match =
                        candidates.stream()
                                .filter(
                                        m ->
                                                ExtractionUtils.isInvokable(
                                                        ExtractionUtils.Autoboxing.JVM,
                                                        m,
                                                        signature))
                                .findFirst();
                if (match.isPresent()) {
                    return match.get();
                }
            }

            throw new IllegalStateException(
                    String.format(
                            "Found %d onTimer() method(s) in %s but none match the expected "
                                    + "signature: optional OnTimerContext followed by state entries %s.",
                            candidates.size(),
                            functionClass.getSimpleName(),
                            Arrays.toString(stateClasses)));
        }

        private void validateEvalMethodSupported(
                ResolvedMethod<?> eval, List<ArgumentInfo> arguments) {
            Parameter[] parameters = eval.method.getParameters();

            int expectedParamCount = arguments.size() + (eval.takesContext ? 1 : 0);
            if (parameters.length != expectedParamCount) {
                long stateCount = ArgumentInfo.filterStateArguments(arguments).size();
                long nonStateCount = arguments.size() - stateCount;
                throw new IllegalStateException(
                        String.format(
                                "Parameter count mismatch: eval() has %d parameters but expected %d "
                                        + "(%d state + %d table/scalar arguments%s). "
                                        + "eval() signature: %s. "
                                        + "This may indicate missing @ArgumentHint or @StateHint annotations.",
                                parameters.length,
                                expectedParamCount,
                                stateCount,
                                nonStateCount,
                                eval.takesContext ? " + Context" : "",
                                eval.method));
            }

            int argOffset = eval.takesContext ? 1 : 0;
            for (int i = 0; i < arguments.size(); i++) {
                Parameter param = parameters[i + argOffset];
                Class<?> paramType = param.getType();
                ArgumentInfo arg = arguments.get(i);

                if (arg instanceof ScalarArgumentInfo) {
                    Object value = ((ScalarArgumentInfo) arg).value;
                    Class<?> expectedType = ClassUtils.primitiveToWrapper(paramType);
                    if (value != null && !expectedType.isAssignableFrom(value.getClass())) {
                        throw new IllegalStateException(
                                String.format(
                                        "Type mismatch for scalar argument '%s' at position %d: "
                                                + "eval() parameter expects %s but provided value is %s",
                                        arg.name,
                                        i + argOffset,
                                        paramType.getName(),
                                        value.getClass().getName()));
                    }
                }
            }
        }

        /**
         * Validates that all SET_SEMANTIC_TABLE arguments with partitioning use consistent
         * partitioning. All such arguments must have the same number of partition columns with
         * matching data types.
         */
        private void validatePartitionConsistency(List<ArgumentInfo> arguments) {
            final List<TableArgumentInfo> partitionedTables = new ArrayList<>();
            for (ArgumentInfo arg : arguments) {
                if (arg instanceof TableArgumentInfo) {
                    TableArgumentInfo tableArg = (TableArgumentInfo) arg;
                    if (tableArg.isSetSemantic && tableArg.partitionColumnNames != null) {
                        partitionedTables.add(tableArg);
                    }
                }
            }

            if (partitionedTables.size() <= 1) {
                return;
            }

            final TableArgumentInfo first = partitionedTables.get(0);
            final int expectedPartitionColumnCount = first.partitionColumnNames.length;

            for (int i = 1; i < partitionedTables.size(); i++) {
                TableArgumentInfo current = partitionedTables.get(i);

                if (current.partitionColumnNames.length != expectedPartitionColumnCount) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Inconsistent partitioning: Table argument '%s' has %d partition column(s), "
                                            + "but table argument '%s' has %d partition column(s). "
                                            + "All SET_SEMANTIC_TABLE arguments must use consistent partitioning "
                                            + "(same number of columns and matching data types).",
                                    first.name,
                                    expectedPartitionColumnCount,
                                    current.name,
                                    current.partitionColumnNames.length));
                }

                // Check that partition column types match
                for (int colIdx = 0; colIdx < expectedPartitionColumnCount; colIdx++) {
                    String firstColName = first.partitionColumnNames[colIdx];
                    String currentColName = current.partitionColumnNames[colIdx];
                    DataType firstColType = extractPartitionColumnType(first, firstColName);
                    DataType currentColType = extractPartitionColumnType(current, currentColName);

                    if (!firstColType.getLogicalType().equals(currentColType.getLogicalType())) {
                        throw new IllegalArgumentException(
                                String.format(
                                        "Inconsistent partitioning: Partition column '%s' of table argument '%s' "
                                                + "has type %s, but partition column '%s' of table argument '%s' "
                                                + "has type %s. All SET_SEMANTIC_TABLE arguments must use "
                                                + "consistent partitioning (same number of columns and matching data types).",
                                        firstColName,
                                        first.name,
                                        firstColType,
                                        currentColName,
                                        current.name,
                                        currentColType));
                    }
                }
            }
        }

        private void validateInitialStateKeys(List<ArgumentInfo> arguments) {
            if (stateArgs.isEmpty()) {
                return;
            }

            // All partitioned tables share the same partition key shape (ensured by
            // validatePartitionConsistency()), so any one suffices for validation.
            Optional<TableArgumentInfo> partitionedTable =
                    arguments.stream()
                            .filter(arg -> arg instanceof TableArgumentInfo)
                            .map(arg -> (TableArgumentInfo) arg)
                            .filter(t -> t.isSetSemantic && t.partitionColumnNames != null)
                            .findFirst();

            if (partitionedTable.isEmpty()) {
                // In cases of PTFs with OPTIONAL_PARTITION_BY and harness setups with no partition
                // setup, all data shares the same Row.of() key, so there is no schema to validate
                return;
            }

            TableArgumentInfo table = partitionedTable.get();
            int expectedArity = table.partitionColumnNames.length;
            LogicalType[] expectedTypes =
                    Arrays.stream(table.partitionColumnNames)
                            .map(col -> extractPartitionColumnType(table, col).getLogicalType())
                            .toArray(LogicalType[]::new);

            for (Map.Entry<String, StateArgumentConfiguration> entry : stateArgs.entrySet()) {
                for (Row key : entry.getValue().initialValues.keySet()) {
                    if (key.getArity() != expectedArity) {
                        throw new IllegalArgumentException(
                                String.format(
                                        "Initial state key '%s' for state '%s' has arity %d, "
                                                + "but partition key has arity %d.",
                                        key, entry.getKey(), key.getArity(), expectedArity));
                    }

                    for (int i = 0; i < expectedArity; i++) {
                        Object value = key.getField(i);
                        Class<?> expectedClass = expectedTypes[i].getDefaultConversion();
                        if (value != null && !expectedClass.isInstance(value)) {
                            throw new IllegalArgumentException(
                                    String.format(
                                            "Initial state key '%s' for state '%s' has type %s "
                                                    + "at position %d, but partition column '%s' "
                                                    + "expects %s.",
                                            key,
                                            entry.getKey(),
                                            value.getClass().getSimpleName(),
                                            i,
                                            table.partitionColumnNames[i],
                                            expectedClass.getSimpleName()));
                        }
                    }
                }
            }
        }

        private DataType extractPartitionColumnType(TableArgumentInfo tableArg, String columnName) {
            if (!(tableArg.dataType instanceof FieldsDataType)) {
                throw new IllegalStateException(
                        String.format(
                                "Cannot extract data type for partition column '%s' of argument '%s': "
                                        + "argument data type is not a FieldsDataType (actual: %s)",
                                columnName,
                                tableArg.name,
                                tableArg.dataType.getClass().getSimpleName()));
            }

            FieldsDataType fieldsDataType = (FieldsDataType) tableArg.dataType;
            List<String> fieldNames = getFieldNames(tableArg.dataType);
            List<DataType> fieldDataTypes = fieldsDataType.getChildren();

            int fieldIndex = fieldNames.indexOf(columnName);
            if (fieldIndex >= 0) {
                return fieldDataTypes.get(fieldIndex);
            }

            throw new IllegalStateException(
                    String.format(
                            "Partition column '%s' not found in argument '%s'",
                            columnName, tableArg.name));
        }

        private TestHarnessStateManager.PartitionKeyInfo extractPartitionKeyInfo(
                List<ArgumentInfo> arguments) {
            // All partitioned tables share the same partition key shape (ensured by
            // validatePartitionConsistency()), so any one suffices.
            Optional<TableArgumentInfo> partitionedTable =
                    arguments.stream()
                            .filter(arg -> arg instanceof TableArgumentInfo)
                            .map(arg -> (TableArgumentInfo) arg)
                            .filter(t -> t.isSetSemantic && t.partitionColumnNames != null)
                            .findFirst();

            if (partitionedTable.isEmpty()) {
                return new TestHarnessStateManager.PartitionKeyInfo(0, null, null);
            }

            TableArgumentInfo table = partitionedTable.get();
            String[] columnNames = table.partitionColumnNames;
            LogicalType[] columnTypes =
                    Arrays.stream(columnNames)
                            .map(col -> extractPartitionColumnType(table, col).getLogicalType())
                            .toArray(LogicalType[]::new);
            return new TestHarnessStateManager.PartitionKeyInfo(
                    columnNames.length, columnNames, columnTypes);
        }

        // ---------------------------------------------------------------------
        // Type Inference
        // ---------------------------------------------------------------------

        /**
         * Extracts type inference from the PTF and validates builder configuration.
         *
         * <p>Uses SystemTypeInference to validate things like reserved argument names, multiple
         * table argument rules, static argument trait validation, etc.
         */
        private List<ArgumentInfo> extractAndValidateTypeInference(
                ProcessTableFunction<OUT> function, TypeInference systemTypeInference)
                throws Exception {

            Optional<List<StaticArgument>> staticArgsOpt = systemTypeInference.getStaticArguments();
            if (staticArgsOpt.isEmpty()) {
                throw new IllegalStateException(
                        "PTF does not provide static argument information. "
                                + "Ensure @ArgumentHint annotations are present on all eval() parameters.");
            }

            List<StaticArgument> allArgs = staticArgsOpt.get();
            List<StaticArgument> userArgs = new ArrayList<>();
            for (StaticArgument arg : allArgs) {
                if (!isSystemArgument(arg.getName())) {
                    userArgs.add(arg);
                }
            }

            List<ArgumentInfo> tableAndScalarArguments = new ArrayList<>();

            for (StaticArgument staticArg : userArgs) {
                boolean isScalar = staticArg.getTraits().contains(StaticArgumentTrait.SCALAR);
                boolean isTableArg =
                        staticArg.getTraits().contains(StaticArgumentTrait.ROW_SEMANTIC_TABLE)
                                || staticArg
                                        .getTraits()
                                        .contains(StaticArgumentTrait.SET_SEMANTIC_TABLE);

                if (isScalar || isTableArg) {
                    ArgumentInfo argInfo = buildArgumentInfo(staticArg);
                    tableAndScalarArguments.add(argInfo);
                } else {
                    throw new IllegalStateException(
                            "Unknown argument type for StaticArgument. "
                                    + "Expected SCALAR, ROW_SEMANTIC_TABLE, or SET_SEMANTIC_TABLE trait.");
                }
            }

            validateArgumentConfiguration(tableAndScalarArguments);

            // Extract state arguments from TypeInference
            List<StateArgumentInfo> stateArguments = new ArrayList<>();

            Map<String, StateTypeStrategy> stateStrategies =
                    systemTypeInference.getStateTypeStrategies();

            DataTypeFactory dataTypeFactory = createDataTypeFactory();

            List<TableArgumentInfo> tableArgs =
                    ArgumentInfo.filterTableArguments(tableAndScalarArguments);
            List<DataType> argumentDataTypes = new ArrayList<>();
            for (TableArgumentInfo tArg : tableArgs) {
                argumentDataTypes.add(tArg.dataType);
            }
            Map<Integer, TableSemantics> tableSemanticsMap = new HashMap<>();
            for (int i = 0; i < tableArgs.size(); i++) {
                TableArgumentInfo tArg = tableArgs.get(i);
                int[] partitionIndices = getPartitionColumnIndices(tArg);
                tableSemanticsMap.put(
                        i, new TestHarnessTableSemantics(tArg.dataType, partitionIndices));
            }

            TestHarnessCallContext callContext = new TestHarnessCallContext();
            callContext.typeFactory = dataTypeFactory;
            callContext.argumentDataTypes = argumentDataTypes;
            callContext.tableSemantics = tableSemanticsMap;
            callContext.functionDefinition = function;
            callContext.name = function.getClass().getSimpleName();

            for (Map.Entry<String, StateTypeStrategy> entry : stateStrategies.entrySet()) {
                String stateName = entry.getKey();
                StateTypeStrategy strategy = entry.getValue();

                Optional<DataType> dataTypeOpt = strategy.inferType(callContext);
                if (dataTypeOpt.isEmpty()) {
                    throw new IllegalStateException(
                            String.format(
                                    "Could not infer data type for state parameter '%s'",
                                    stateName));
                }
                DataType stateDataType = dataTypeOpt.get();

                Optional<Duration> ttlOpt = strategy.getTimeToLive(callContext);
                stateArguments.add(
                        new StateArgumentInfo(stateName, stateDataType, ttlOpt.orElse(null)));
            }

            List<ArgumentInfo> allArguments = new ArrayList<>();
            allArguments.addAll(stateArguments);
            allArguments.addAll(tableAndScalarArguments);

            return allArguments;
        }

        /** Creates appropriate StateConverter for the given state data type. */
        private StateConverter createStateConverter(
                DataType stateDataType, ClassLoader classLoader) {
            DataType resolvedType =
                    ListView.class.isAssignableFrom(stateDataType.getConversionClass())
                                    || MapView.class.isAssignableFrom(
                                            stateDataType.getConversionClass())
                            ? stateDataType.getChildren().get(0)
                            : stateDataType;

            LogicalType logicalType = resolvedType.getLogicalType();

            if (logicalType instanceof ArrayType) {
                DataType elementType = resolvedType.getChildren().get(0);
                DataStructureConverter<Object, Object> elementConverter =
                        DataStructureConverters.getConverter(elementType);
                elementConverter.open(classLoader);
                return new ListViewStateConverter((ArrayType) logicalType, elementConverter);
            } else if (logicalType instanceof MapType) {
                DataType keyType = resolvedType.getChildren().get(0);
                DataType valueType = resolvedType.getChildren().get(1);
                DataStructureConverter<Object, Object> keyConverter =
                        DataStructureConverters.getConverter(keyType);
                DataStructureConverter<Object, Object> valueConverter =
                        DataStructureConverters.getConverter(valueType);
                keyConverter.open(classLoader);
                valueConverter.open(classLoader);
                return new MapViewStateConverter(
                        (MapType) logicalType, keyConverter, valueConverter);
            } else if (logicalType instanceof RowType) {
                DataStructureConverter<Object, Object> converter =
                        DataStructureConverters.getConverter(resolvedType);
                converter.open(classLoader);
                return new RowStateConverter((RowType) logicalType, converter);
            } else {
                DataStructureConverter<Object, Object> converter =
                        DataStructureConverters.getConverter(resolvedType);
                converter.open(classLoader);
                return new StructuredTypeStateConverter(
                        resolvedType.getConversionClass(), converter);
            }
        }

        /** Checks if an argument name is a system-reserved argument. */
        private boolean isSystemArgument(String argName) {
            return SystemTypeInference.PROCESS_TABLE_FUNCTION_ARG_ON_TIME.equals(argName)
                    || SystemTypeInference.PROCESS_TABLE_FUNCTION_ARG_UID.equals(argName);
        }

        private DataTypeFactory createDataTypeFactory() {
            return new TestHarnessDataTypeFactory();
        }

        private ArgumentInfo buildArgumentInfo(StaticArgument staticArg) {

            String name = staticArg.getName();
            ArgumentTrait primaryTrait = extractPrimaryTrait(staticArg.getTraits());

            DataType dataType;
            if (primaryTrait == ArgumentTrait.SCALAR) {
                Optional<DataType> dataTypeOpt = staticArg.getDataType();
                if (dataTypeOpt.isPresent()) {
                    dataType = dataTypeOpt.get();
                } else {
                    throw new IllegalStateException(
                            String.format(
                                    "Cannot determine data type for scalar argument '%s'", name));
                }
            } else {
                // For table arguments, check both annotation and builder config
                Optional<DataType> annotationTypeOpt = staticArg.getDataType();
                TableArgumentConfiguration config = tableArgs.get(name);

                if (annotationTypeOpt.isPresent()
                        && config != null
                        && config.explicitType != null) {
                    // Both specified - validate they match
                    DataTypeFactory factory = createDataTypeFactory();
                    DataType builderType = factory.createDataType(config.explicitType);
                    DataType annotationType = annotationTypeOpt.get();

                    if (!annotationType.equals(builderType)) {
                        throw new IllegalStateException(
                                String.format(
                                        "Type mismatch for table argument '%s': "
                                                + "annotation declares type %s but builder declares type %s. "
                                                + "If the PTF already explicitly declares type information, "
                                                + "there is no need to specify it in the builder.",
                                        name, annotationType, builderType));
                    }
                    dataType = annotationType;
                } else if (annotationTypeOpt.isPresent()) {
                    dataType = annotationTypeOpt.get();
                } else if (config != null && config.explicitType != null) {
                    DataTypeFactory factory = createDataTypeFactory();
                    dataType = factory.createDataType(config.explicitType);
                } else {
                    // Try to infer from Java parameter class (for structured types)
                    Optional<Class<?>> conversionClassOpt = staticArg.getConversionClass();
                    if (conversionClassOpt.isPresent()) {
                        DataTypeFactory factory = createDataTypeFactory();
                        dataType = factory.createDataType(conversionClassOpt.get());
                    } else {
                        throw new IllegalStateException(
                                String.format(
                                        "Table argument '%s' requires explicit type configuration. "
                                                + "Use .withTableArgument(\"%s\", DataTypes.of(\"ROW<...>\")) "
                                                + "to explicitly declare it.",
                                        name, name));
                    }
                }
            }

            String[] partitionColumnNames = null;
            if (primaryTrait == ArgumentTrait.SET_SEMANTIC_TABLE) {
                boolean hasOptionalPartitionBy =
                        staticArg.getTraits().contains(StaticArgumentTrait.OPTIONAL_PARTITION_BY);
                partitionColumnNames =
                        extractAndValidatePartitionColumns(name, dataType, hasOptionalPartitionBy);
            }

            boolean hasPassColumnsThrough =
                    staticArg.getTraits().contains(StaticArgumentTrait.PASS_COLUMNS_THROUGH);

            if (primaryTrait == ArgumentTrait.SCALAR) {
                ScalarArgumentConfiguration config = scalarArgs.get(name);
                Object value = config != null ? config.value : null;
                return new ScalarArgumentInfo(name, dataType, value);
            } else {
                return new TableArgumentInfo(
                        name, dataType, primaryTrait, partitionColumnNames, hasPassColumnsThrough);
            }
        }

        private ArgumentTrait extractPrimaryTrait(EnumSet<StaticArgumentTrait> staticTraits) {
            if (staticTraits.contains(StaticArgumentTrait.SCALAR)) {
                return ArgumentTrait.SCALAR;
            }
            if (staticTraits.contains(StaticArgumentTrait.ROW_SEMANTIC_TABLE)) {
                return ArgumentTrait.ROW_SEMANTIC_TABLE;
            }
            if (staticTraits.contains(StaticArgumentTrait.SET_SEMANTIC_TABLE)) {
                return ArgumentTrait.SET_SEMANTIC_TABLE;
            }
            return ArgumentTrait.SCALAR;
        }

        private String[] extractAndValidatePartitionColumns(
                String name, DataType dataType, boolean isOptionalPartitionBy) {
            PartitionConfiguration config = partitionConfigs.get(name);
            if (config == null) {
                if (isOptionalPartitionBy) {
                    return null;
                }
                throw new IllegalStateException(
                        String.format(
                                "No partition configuration found for SET_SEMANTIC_TABLE argument '%s'. "
                                        + "Use withPartitionBy(\"%s\", ...) to configure partitioning.",
                                name, name));
            }

            List<String> fieldNames = getFieldNames(dataType);

            for (String columnName : config.columnNames) {
                if (!fieldNames.contains(columnName)) {
                    throw new IllegalArgumentException(
                            "Partition column '"
                                    + columnName
                                    + "' not found. "
                                    + "Available columns: "
                                    + fieldNames);
                }
            }
            return config.columnNames;
        }

        /** Validates scalar argument values are configured and no unknown arguments exist. */
        private void validateArgumentConfiguration(List<ArgumentInfo> arguments) {
            for (ArgumentInfo arg : arguments) {
                if (arg instanceof ScalarArgumentInfo && !scalarArgs.containsKey(arg.name)) {
                    throw new IllegalStateException(
                            String.format(
                                    "Missing required scalar argument '%s'. "
                                            + "Use .withScalarArgument(\"%s\", ...)",
                                    arg.name, arg.name));
                }
            }

            Set<String> validNames = new HashSet<>();
            for (ArgumentInfo arg : arguments) {
                if (arg.name != null) {
                    validNames.add(arg.name);
                }
            }

            for (String configuredScalar : scalarArgs.keySet()) {
                if (!validNames.contains(configuredScalar)) {
                    throw new IllegalStateException(
                            "Unknown scalar argument: '"
                                    + configuredScalar
                                    + "'. Not found in PTF signature.");
                }
            }

            for (String configuredTable : tableArgs.keySet()) {
                if (!validNames.contains(configuredTable)) {
                    throw new IllegalStateException(
                            "Unknown table argument: '"
                                    + configuredTable
                                    + "'. Not found in PTF signature.");
                }
            }
        }

        private ProcessTableFunction<OUT> instantiateFunction() throws IllegalArgumentException {
            try {
                return functionClass.getDeclaredConstructor().newInstance();
            } catch (NoSuchMethodException e) {
                throw new IllegalArgumentException(
                        "PTF class must have a no-arg constructor: " + functionClass.getName(), e);
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        "Failed to instantiate PTF: " + functionClass.getName(), e);
            }
        }

        /**
         * Derives an output schema by invoking the given {@link TypeInference}'s output strategy.
         *
         * <p>Passing the {@link SystemTypeInference} yields the full operator output (prepended
         * partition/pass-through columns, the wrapped or flattened function output, and the rowtime
         * column). Passing the base inference yields the PTF's own declared output type.
         *
         * <p>The staticArgs list may include both user-declared arguments and system-injected
         * arguments (on_time, uid). The CallContext we build mirrors this list positionally — each
         * index maps to a staticArg. User args get their resolved DataType; system args get
         * placeholder types (DESCRIPTOR for on_time, STRING for uid). Table semantics are attached
         * at the positions of table arguments so SystemTypeInference can perform pass-through
         * column deduplication. The base inference has no system args, so those branches are simply
         * unused.
         */
        private DataType deriveOutputType(
                ProcessTableFunction<OUT> function,
                DataTypeFactory dataTypeFactory,
                TypeInference typeInference,
                List<ArgumentInfo> allArguments,
                List<TableArgumentInfo> tableArguments) {

            List<StaticArgument> staticArgs =
                    typeInference
                            .getStaticArguments()
                            .orElseThrow(
                                    () ->
                                            new IllegalStateException(
                                                    "TypeInference has no static arguments"));

            Map<String, TableArgumentInfo> tableArgsByName = new HashMap<>();
            for (TableArgumentInfo tableArg : tableArguments) {
                tableArgsByName.put(tableArg.name, tableArg);
            }
            Map<String, ArgumentInfo> allArgsByName = new HashMap<>();
            for (ArgumentInfo arg : allArguments) {
                if (arg.name != null) {
                    allArgsByName.put(arg.name, arg);
                }
            }

            List<DataType> argumentDataTypes = new ArrayList<>();
            Map<Integer, TableSemantics> tableSemanticsMap = new HashMap<>();
            int onTimePos = -1;

            for (int i = 0; i < staticArgs.size(); i++) {
                StaticArgument staticArg = staticArgs.get(i);
                String argName = staticArg.getName();

                if (SystemTypeInference.PROCESS_TABLE_FUNCTION_ARG_ON_TIME.equals(argName)) {
                    argumentDataTypes.add(DataTypes.DESCRIPTOR());
                    onTimePos = i;
                } else if (SystemTypeInference.PROCESS_TABLE_FUNCTION_ARG_UID.equals(argName)) {
                    argumentDataTypes.add(DataTypes.STRING());
                } else {
                    ArgumentInfo argInfo = allArgsByName.get(argName);
                    if (argInfo != null) {
                        argumentDataTypes.add(argInfo.dataType);
                    } else {
                        argumentDataTypes.add(DataTypes.NULL());
                    }

                    TableArgumentInfo tableArg = tableArgsByName.get(argName);
                    if (tableArg != null) {
                        int[] partitionIndices = getPartitionColumnIndices(tableArg);
                        int timeColumnIndex = -1;
                        if (onTimeColumnName != null) {
                            int idx = getFieldNames(tableArg.dataType).indexOf(onTimeColumnName);
                            if (idx >= 0) {
                                timeColumnIndex = idx;
                            }
                        }
                        tableSemanticsMap.put(
                                i,
                                new TestHarnessTableSemantics(
                                        tableArg.dataType, partitionIndices, timeColumnIndex));
                    }
                }
            }

            TestHarnessCallContext callContext = new TestHarnessCallContext();
            callContext.typeFactory = dataTypeFactory;
            callContext.argumentDataTypes = argumentDataTypes;
            callContext.functionDefinition = function;
            callContext.tableSemantics = tableSemanticsMap;
            callContext.name = function.getClass().getSimpleName();

            if (onTimePos >= 0 && onTimeColumnName != null) {
                callContext.argumentValues.put(
                        onTimePos,
                        org.apache.flink.types.ColumnList.of(
                                Collections.singletonList(onTimeColumnName)));
            }

            TypeStrategy outputStrategy = typeInference.getOutputTypeStrategy();
            Optional<DataType> outputTypeOpt = outputStrategy.inferType(callContext);

            if (outputTypeOpt.isEmpty()) {
                throw new IllegalStateException(
                        "Failed to derive output type for " + function.getClass().getSimpleName());
            }

            return outputTypeOpt.get();
        }
    }

    private enum OutputPrependStrategy {
        NONE,
        PARTITION_KEYS,
        ALL_COLUMNS
    }

    private enum OutputKind {
        ATOMIC,
        ROW,
        STRUCTURED
    }

    private void handleEvalInvocationException(
            String contextMessage, Object[] methodArgs, InvocationTargetException e)
            throws Exception {
        Throwable cause = e.getCause();
        StringBuilder details = new StringBuilder();
        details.append(contextMessage);
        details.append("Expected parameter types: ");
        details.append(Arrays.toString(eval.method.getParameterTypes()));
        details.append("\nActual arguments:\n");
        for (int i = 0; i < arguments.size(); i++) {
            ArgumentInfo arg = arguments.get(i);
            Object value = methodArgs[i];
            details.append(
                    String.format(
                            "  [%d] %s: %s (type: %s)\n",
                            i,
                            arg.name,
                            value,
                            value != null ? value.getClass().getName() : "null"));
        }

        if (cause instanceof Exception) {
            Exception userException = (Exception) cause;
            userException.addSuppressed(new Exception(details.toString()));
            throw userException;
        } else {
            throw new RuntimeException(details.toString(), e);
        }
    }

    /**
     * Base class for PTF eval() arguments.
     *
     * <p>Represents validated argument information combining PTF signature, type inference results,
     * and builder configuration.
     */
    private abstract static class ArgumentInfo {
        final String name;
        final DataType dataType;

        ArgumentInfo(String name, DataType dataType) {
            this.name = name;
            this.dataType = dataType;
        }

        static List<StateArgumentInfo> filterStateArguments(List<ArgumentInfo> arguments) {
            return arguments.stream()
                    .filter(arg -> arg instanceof StateArgumentInfo)
                    .map(arg -> (StateArgumentInfo) arg)
                    .collect(Collectors.toList());
        }

        static List<TableArgumentInfo> filterTableArguments(List<ArgumentInfo> arguments) {
            return arguments.stream()
                    .filter(arg -> arg instanceof TableArgumentInfo)
                    .map(arg -> (TableArgumentInfo) arg)
                    .collect(Collectors.toList());
        }

        static List<ScalarArgumentInfo> filterScalarArguments(List<ArgumentInfo> arguments) {
            return arguments.stream()
                    .filter(arg -> arg instanceof ScalarArgumentInfo)
                    .map(arg -> (ScalarArgumentInfo) arg)
                    .collect(Collectors.toList());
        }
    }

    /** State parameter with TTL configuration. */
    static class StateArgumentInfo extends ArgumentInfo {
        final Duration ttl;

        StateArgumentInfo(String name, DataType dataType, Duration ttl) {
            super(name, dataType);
            this.ttl = ttl;
        }
    }

    /** Table argument with partitioning and output prepending strategy. */
    private static class TableArgumentInfo extends ArgumentInfo {
        final String[] partitionColumnNames;
        final boolean isSetSemantic;
        final OutputPrependStrategy prependStrategy;

        TableArgumentInfo(
                String name,
                DataType dataType,
                ArgumentTrait primaryTrait,
                String[] partitionColumnNames,
                boolean hasPassColumnsThrough) {
            super(name, dataType);
            this.partitionColumnNames = partitionColumnNames;
            this.isSetSemantic = (primaryTrait == ArgumentTrait.SET_SEMANTIC_TABLE);
            this.prependStrategy =
                    hasPassColumnsThrough
                            ? OutputPrependStrategy.ALL_COLUMNS
                            : (this.isSetSemantic && partitionColumnNames != null)
                                    ? OutputPrependStrategy.PARTITION_KEYS
                                    : OutputPrependStrategy.NONE;
        }
    }

    /** Scalar (constant) argument. */
    private static class ScalarArgumentInfo extends ArgumentInfo {
        final Object value;

        ScalarArgumentInfo(String name, DataType dataType, Object value) {
            super(name, dataType);
            this.value = value;
        }
    }

    private static class TableArgumentConfiguration {
        final AbstractDataType<?> explicitType;

        TableArgumentConfiguration(AbstractDataType<?> explicitType) {
            this.explicitType = explicitType;
        }
    }

    private static class ScalarArgumentConfiguration {
        final Object value;

        ScalarArgumentConfiguration(Object value) {
            this.value = value;
        }
    }

    private static class PartitionConfiguration {
        final String[] columnNames;

        PartitionConfiguration(String[] columnNames) {
            this.columnNames = columnNames;
        }
    }

    private static class StateArgumentConfiguration {
        final Map<Row, Object> initialValues;

        StateArgumentConfiguration() {
            this.initialValues = new HashMap<>();
        }
    }
}
