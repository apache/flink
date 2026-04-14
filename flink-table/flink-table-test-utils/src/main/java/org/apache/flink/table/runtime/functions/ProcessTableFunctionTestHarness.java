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
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.table.types.inference.SystemTypeInference;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
 * }</pre>
 */
@PublicEvolving
public class ProcessTableFunctionTestHarness<OUT> implements AutoCloseable {

    private final ProcessTableFunction<OUT> function;
    private final FunctionContext functionContext;
    private final List<OUT> output;
    private boolean isOpen;
    private final HarnessCollector collector;

    private final String defaultTableArgument;
    private final Method evalMethod;
    private final List<ArgumentInfo> arguments;

    private final Map<String, ArgumentInfo> argumentsByName;
    private final boolean isSingleTableFunction;
    private final Map<String, Object> scalarArgumentValues;

    private final boolean hasTableArguments;
    private final Map<String, DataStructureConverter<Object, Object>> inputConverters;
    private final Map<String, DataStructureConverter<Object, Object>> outputConverters;

    private ProcessTableFunctionTestHarness(
            ProcessTableFunction<OUT> function,
            FunctionContext functionContext,
            String defaultTableArgument,
            Method evalMethod,
            List<ArgumentInfo> arguments,
            Map<String, ArgumentInfo> argumentsByName,
            boolean isSingleTableFunction,
            Map<String, Object> scalarArgumentValues,
            Map<String, DataStructureConverter<Object, Object>> inputConverters,
            Map<String, DataStructureConverter<Object, Object>> outputConverters)
            throws Exception {
        this.function = function;
        this.functionContext = functionContext;
        this.defaultTableArgument = defaultTableArgument;
        this.evalMethod = evalMethod;
        this.arguments = arguments;
        this.argumentsByName = argumentsByName;
        this.isSingleTableFunction = isSingleTableFunction;
        this.scalarArgumentValues = scalarArgumentValues;
        this.hasTableArguments = arguments.stream().anyMatch(arg -> arg.isTableArgument);
        this.inputConverters = inputConverters;
        this.outputConverters = outputConverters;
        this.output = new ArrayList<>();
        this.collector = new HarnessCollector();
        this.isOpen = false;

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

        ArgumentInfo tableArg = argumentsByName.get(tableArgument);
        if (tableArg == null) {
            throw new IllegalArgumentException("Unknown table argument: " + tableArgument);
        }
        invokeEval(tableArg, row);
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

        // Clear collector context since there's no active table argument
        collector.setContext(null, null);

        Object[] args = new Object[arguments.size()];
        for (int i = 0; i < arguments.size(); i++) {
            ArgumentInfo arg = arguments.get(i);
            if (arg.isScalar) {
                args[i] = scalarArgumentValues.get(arg.name);
            } else {
                throw new IllegalStateException(
                        "Unexpected non-scalar argument at position " + i + ": " + arg.name);
            }
        }
        try {
            evalMethod.invoke(function, args);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                Exception userException = (Exception) cause;
                userException.addSuppressed(
                        new Exception(
                                String.format(
                                        "Exception occurred during scalar-only PTF eval() invocation. "
                                                + "Scalar arguments: %s",
                                        scalarArgumentValues)));
                throw userException;
            } else {
                throw new RuntimeException("Error invoking PTF eval() method", e);
            }
        }
    }

    /** Returns all collected output rows. */
    public List<OUT> getOutput() {
        return List.copyOf(output);
    }

    /** Clears all collected output. */
    public void clearOutput() {
        output.clear();
    }

    /**
     * Given a target table argument and a row to process, construct the right set of arguments for
     * the PTF's eval function and attempt to invoke it.
     */
    private void invokeEval(ArgumentInfo activeTableArg, Row activeRow) throws Exception {
        // Set collector context so it can prepend columns if needed
        collector.setContext(activeTableArg, activeRow);

        Object[] args = new Object[arguments.size()];

        for (int i = 0; i < arguments.size(); i++) {
            ArgumentInfo arg = arguments.get(i);

            if (arg.isTableArgument && arg.name.equals(activeTableArg.name)) {
                // If the argument is the active table argument, first convert the input row
                // to an internal RowData type, and then convert the RowData to type that the
                // argument expects. For Rows, this will structure the Row based on the table
                // argument structure. Otherwise, for POJOs, it will pass the expected POJO to eval.

                DataStructureConverter<Object, Object> inputConverter =
                        inputConverters.get(arg.name);
                DataStructureConverter<Object, Object> outputConverter =
                        outputConverters.get(arg.name);

                args[i] =
                        outputConverter.toExternalOrNull(
                                inputConverter.toInternalOrNull(activeRow));

            } else if (arg.isScalar) {
                args[i] = scalarArgumentValues.get(arg.name);

            } else if (arg.isTableArgument) {
                // Inactive table arguments receive null
                args[i] = null;
            } else {
                throw new IllegalStateException(
                        "Unexpected argument type at position " + i + ": " + arg.name);
            }
        }

        try {
            evalMethod.invoke(function, args);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                Exception userException = (Exception) cause;
                String partitionInfo =
                        activeTableArg.partitionColumnNames != null
                                        && activeTableArg.partitionColumnNames.length > 0
                                ? String.format(
                                        ", partition columns: %s",
                                        Arrays.toString(activeTableArg.partitionColumnNames))
                                : ", no partitioning";
                userException.addSuppressed(
                        new Exception(
                                String.format(
                                        "Exception occurred during PTF eval() while processing table argument '%s'%s. "
                                                + "Input row: %s",
                                        activeTableArg.name, partitionInfo, activeRow)));
                throw userException;
            } else {
                throw new RuntimeException("Error invoking PTF eval() method", e);
            }
        }
    }

    /**
     * Collector implementation that stores output in the harness.
     *
     * <p>For SET_SEMANTIC_TABLE arguments, automatically prepends partition key columns to the PTF
     * output. If the argument has PASS_COLUMNS_THROUGH trait, prepends all input columns.
     */
    private class HarnessCollector implements Collector<OUT> {
        // Context set before each eval() invocation
        private ArgumentInfo activeTableArg;
        private Row activeRow;

        void setContext(ArgumentInfo tableArg, Row row) {
            this.activeTableArg = tableArg;
            this.activeRow = row;
        }

        @Override
        public void collect(OUT record) {
            if (activeTableArg == null || !activeTableArg.isTableArgument) {
                output.add(record);
                return;
            }

            if (activeTableArg.hasPassColumnsThrough) {
                output.add(prependAllColumns(record));
            } else if (activeTableArg.isSetSemantic
                    && activeTableArg.partitionColumnNames != null) {
                output.add(prependPartitionKeys(record));
            } else {
                output.add(record);
            }
        }

        @SuppressWarnings("unchecked")
        private OUT prependPartitionKeys(OUT ptfOutput) {
            if (!(ptfOutput instanceof Row)) {
                throw new IllegalStateException(
                        "Cannot prepend partition keys to non-Row output type: "
                                + ptfOutput.getClass());
            }

            Row ptfRow = (Row) ptfOutput;

            // For multi-table PTFs, prepend partition keys from ALL SET_SEMANTIC_TABLE arguments
            // Active table contributes actual partition key values, inactive tables contribute
            // nulls
            int totalPartitionKeyCount = 0;
            for (ArgumentInfo arg : arguments) {
                if (arg.isSetSemantic && arg.partitionColumnNames != null) {
                    totalPartitionKeyCount += arg.partitionColumnNames.length;
                }
            }

            int ptfOutputArity = ptfRow.getArity();
            int totalArity = totalPartitionKeyCount + ptfOutputArity;

            Row result = new Row(ptfRow.getKind(), totalArity);

            // Prepend partition key values from all SET_SEMANTIC_TABLE arguments
            int resultIndex = 0;
            for (ArgumentInfo arg : arguments) {
                if (arg.isSetSemantic && arg.partitionColumnNames != null) {
                    boolean isActive = arg.name.equals(activeTableArg.name);

                    for (String columnName : arg.partitionColumnNames) {
                        if (isActive) {
                            int columnIndex = getFieldIndex(arg.dataType, columnName);
                            result.setField(resultIndex++, activeRow.getField(columnIndex));
                        } else {
                            result.setField(resultIndex++, null);
                        }
                    }
                }
            }

            for (int i = 0; i < ptfOutputArity; i++) {
                result.setField(resultIndex++, ptfRow.getField(i));
            }

            return (OUT) result;
        }

        /** Helper to get field index by name from a DataType. */
        private int getFieldIndex(DataType dataType, String fieldName) {
            LogicalType logicalType = dataType.getLogicalType();

            if (logicalType instanceof RowType) {
                RowType rowType = (RowType) logicalType;
                int index = 0;
                for (RowType.RowField field : rowType.getFields()) {
                    if (field.getName().equals(fieldName)) {
                        return index;
                    }
                    index++;
                }
            } else if (logicalType instanceof StructuredType) {
                StructuredType structuredType = (StructuredType) logicalType;
                int index = 0;
                for (StructuredType.StructuredAttribute attr : structuredType.getAttributes()) {
                    if (attr.getName().equals(fieldName)) {
                        return index;
                    }
                    index++;
                }
            }

            throw new IllegalStateException(
                    String.format("Field '%s' not found in type %s", fieldName, dataType));
        }

        @SuppressWarnings("unchecked")
        private OUT prependAllColumns(OUT ptfOutput) {
            if (!(ptfOutput instanceof Row)) {
                throw new IllegalStateException(
                        "Cannot prepend columns to non-Row output type: " + ptfOutput.getClass());
            }

            Row ptfRow = (Row) ptfOutput;
            int inputArity = activeRow.getArity();
            int ptfOutputArity = ptfRow.getArity();
            int totalArity = inputArity + ptfOutputArity;

            Row result = new Row(ptfRow.getKind(), totalArity);

            for (int i = 0; i < inputArity; i++) {
                result.setField(i, activeRow.getField(i));
            }

            for (int i = 0; i < ptfOutputArity; i++) {
                result.setField(inputArity + i, ptfRow.getField(i));
            }

            return (OUT) result;
        }

        @Override
        public void close() {}
    }

    /**
     * Builder for {@link ProcessTableFunctionTestHarness}.
     *
     * @param <OUT> The output type of the ProcessTableFunction
     */
    @PublicEvolving
    public static class Builder<OUT> {
        private final Class<? extends ProcessTableFunction<OUT>> functionClass;

        private final LinkedHashMap<String, ScalarArgumentConfiguration> scalarArgs =
                new LinkedHashMap<>();
        private final LinkedHashMap<String, TableArgumentConfiguration> tableArgs =
                new LinkedHashMap<>();
        private final Map<String, PartitionConfiguration> partitionConfigs = new HashMap<>();

        private Builder(Class<? extends ProcessTableFunction<OUT>> functionClass) {
            this.functionClass = checkNotNull(functionClass, "functionClass must not be null");
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

            if (scalarArgs.containsKey(argumentName)) {
                throw new IllegalArgumentException(
                        "Argument already configured as scalar: " + argumentName);
            }

            if (tableArgs.containsKey(argumentName)) {
                throw new IllegalArgumentException(
                        "Table argument already configured: " + argumentName);
            }

            TableArgumentConfiguration config = new TableArgumentConfiguration();
            config.explicitType = dataType;
            tableArgs.put(argumentName, config);
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

            if (scalarArgs.containsKey(argumentName)) {
                throw new IllegalArgumentException(
                        "Argument already configured as scalar: " + argumentName);
            }

            if (tableArgs.containsKey(argumentName)) {
                throw new IllegalArgumentException(
                        "Table argument already configured: " + argumentName);
            }

            TableArgumentConfiguration config = new TableArgumentConfiguration();
            tableArgs.put(argumentName, config);
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

            if (scalarArgs.containsKey(argumentName) || tableArgs.containsKey(argumentName)) {
                throw new IllegalArgumentException("Argument already configured: " + argumentName);
            }

            ScalarArgumentConfiguration config = new ScalarArgumentConfiguration(value);
            scalarArgs.put(argumentName, config);
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

            List<ArgumentInfo> arguments = extractAndValidateTypeInference(function);

            FunctionContext functionContext =
                    new FunctionContext(null, Thread.currentThread().getContextClassLoader(), null);

            Method evalMethod = findEvalMethod();

            validateEvalMethodSupported(evalMethod, arguments);
            validatePartitionConsistency(arguments);

            // In cases where PTFs have only a single table argument, set it as a default
            // and mark as a single table function so that processElement without specifying
            // a table argument can be used.
            String defaultTableArg = null;
            boolean isSingleTableFunction = false;

            List<ArgumentInfo> tableArguments = new ArrayList<>();
            for (ArgumentInfo arg : arguments) {
                if (arg.isTableArgument) {
                    tableArguments.add(arg);
                }
            }

            if (tableArguments.size() == 1) {
                defaultTableArg = tableArguments.get(0).name;
                isSingleTableFunction = true;
            }

            Map<String, DataStructureConverter<Object, Object>> inputConverters = new HashMap<>();
            Map<String, DataStructureConverter<Object, Object>> outputConverters = new HashMap<>();
            createConverters(arguments, inputConverters, outputConverters);

            Map<String, ArgumentInfo> argumentsByName = new HashMap<>();
            for (ArgumentInfo arg : arguments) {
                if (arg.name != null) {
                    argumentsByName.put(arg.name, arg);
                }
            }

            return new ProcessTableFunctionTestHarness<>(
                    function,
                    functionContext,
                    defaultTableArg,
                    evalMethod,
                    arguments,
                    argumentsByName,
                    isSingleTableFunction,
                    extractScalarValues(arguments),
                    inputConverters,
                    outputConverters);
        }

        /** Extracts scalar values from configs, creating a map keyed by argument name. */
        private Map<String, Object> extractScalarValues(List<ArgumentInfo> arguments) {
            Map<String, Object> values = new HashMap<>();
            for (ArgumentInfo arg : arguments) {
                if (arg.isScalar) {
                    ScalarArgumentConfiguration config = scalarArgs.get(arg.name);
                    if (config != null) {
                        values.put(arg.name, config.value);
                    }
                }
            }
            return values;
        }

        /**
         * Creates and initializes data structure converters for all table arguments.
         *
         * <p>For Row types, both input and output converters are the same (between Row and
         * RowData).
         *
         * <p>For structured types, input converter uses Row types (Row to RowData), and the output
         * converter uses the structured type.
         */
        private void createConverters(
                List<ArgumentInfo> arguments,
                Map<String, DataStructureConverter<Object, Object>> inputConverters,
                Map<String, DataStructureConverter<Object, Object>> outputConverters) {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

            for (ArgumentInfo arg : arguments) {
                if (arg.isTableArgument) {
                    String converterKey = arg.name;

                    LogicalType logicalType = arg.dataType.getLogicalType();
                    boolean isStructuredType =
                            logicalType instanceof StructuredType
                                    && ((StructuredType) logicalType)
                                            .getImplementationClass()
                                            .isPresent();

                    if (isStructuredType) {
                        StructuredType structuredType = (StructuredType) logicalType;
                        List<RowType.RowField> rowFields = new ArrayList<>();
                        for (StructuredType.StructuredAttribute attr :
                                structuredType.getAttributes()) {
                            rowFields.add(new RowType.RowField(attr.getName(), attr.getType()));
                        }
                        RowType rowType = new RowType(logicalType.isNullable(), rowFields);
                        DataType rowDataType = TypeConversions.fromLogicalToDataType(rowType);

                        DataStructureConverter<Object, Object> inputConverter =
                                DataStructureConverters.getConverter(rowDataType);
                        inputConverter.open(classLoader);

                        DataStructureConverter<Object, Object> outputConverter =
                                DataStructureConverters.getConverter(arg.dataType);
                        outputConverter.open(classLoader);

                        inputConverters.put(converterKey, inputConverter);
                        outputConverters.put(converterKey, outputConverter);
                    } else {
                        DataStructureConverter<Object, Object> converter =
                                DataStructureConverters.getConverter(arg.dataType);
                        converter.open(classLoader);

                        inputConverters.put(converterKey, converter);
                        outputConverters.put(converterKey, converter);
                    }
                }
            }
        }

        private Method findEvalMethod() throws NoSuchMethodException {
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
            } else {
                return evalMethod;
            }
        }

        /**
         * Validates that the eval() method doesn't use unsupported features. Temporary, until state
         * and context is supported.
         */
        private void validateEvalMethodSupported(Method evalMethod, List<ArgumentInfo> arguments) {
            Parameter[] parameters = evalMethod.getParameters();

            for (int i = 0; i < parameters.length; i++) {
                Parameter param = parameters[i];
                Class<?> paramType = param.getType();

                if (ProcessTableFunction.Context.class.isAssignableFrom(paramType)) {
                    throw new IllegalStateException(
                            String.format(
                                    "ProcessTableFunctionTestHarness does not yet support Context parameters. "
                                            + "Found Context parameter at position %d in eval() method. ",
                                    i));
                }

                if (param.isAnnotationPresent(StateHint.class)) {
                    throw new IllegalStateException(
                            String.format(
                                    "ProcessTableFunctionTestHarness does not yet support state parameters. "
                                            + "Found @StateHint parameter at position %d in eval() method. ",
                                    i));
                }
            }

            // Parameter count should also match our arguments list
            if (parameters.length != arguments.size()) {
                throw new IllegalStateException(
                        String.format(
                                "Parameter count mismatch: eval() has %d parameters but only %d arguments were extracted. "
                                        + "This may indicate missing @ArgumentHint annotations.",
                                parameters.length, arguments.size()));
            }
        }

        /**
         * Validates that all SET_SEMANTIC_TABLE arguments with partitioning use consistent
         * partitioning. All such arguments must have the same number of partition columns with
         * matching data types.
         */
        private void validatePartitionConsistency(List<ArgumentInfo> arguments) {
            List<ArgumentInfo> partitionedTables = new ArrayList<>();
            for (ArgumentInfo arg : arguments) {
                if (arg.isSetSemantic && arg.partitionColumnNames != null) {
                    partitionedTables.add(arg);
                }
            }

            if (partitionedTables.size() <= 1) {
                return;
            }

            ArgumentInfo first = partitionedTables.get(0);
            int expectedPartitionColumnCount = first.partitionColumnNames.length;

            for (int i = 1; i < partitionedTables.size(); i++) {
                ArgumentInfo current = partitionedTables.get(i);

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

        private DataType extractPartitionColumnType(ArgumentInfo arg, String columnName) {
            if (arg.dataType instanceof FieldsDataType) {
                FieldsDataType fieldsDataType = (FieldsDataType) arg.dataType;
                LogicalType logicalType = fieldsDataType.getLogicalType();
                List<DataType> fieldDataTypes = fieldsDataType.getChildren();

                if (logicalType instanceof RowType) {
                    RowType rowType = (RowType) logicalType;
                    int fieldIndex = 0;
                    for (RowType.RowField field : rowType.getFields()) {
                        if (field.getName().equals(columnName)) {
                            return fieldDataTypes.get(fieldIndex);
                        }
                        fieldIndex++;
                    }
                } else if (logicalType instanceof StructuredType) {
                    StructuredType structuredType = (StructuredType) logicalType;
                    int attrIndex = 0;
                    for (StructuredType.StructuredAttribute attr : structuredType.getAttributes()) {
                        if (attr.getName().equals(columnName)) {
                            return fieldDataTypes.get(attrIndex);
                        }
                        attrIndex++;
                    }
                }
            }

            throw new IllegalStateException(
                    String.format(
                            "Cannot extract data type for partition column '%s' of argument '%s'",
                            columnName, arg.name));
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
                ProcessTableFunction<OUT> function) {

            DataTypeFactory dataTypeFactory = createDataTypeFactory();
            TypeInference baseTypeInference = function.getTypeInference(dataTypeFactory);
            TypeInference systemTypeInference =
                    SystemTypeInference.of(FunctionKind.PROCESS_TABLE, baseTypeInference);

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

            List<ArgumentInfo> arguments = new ArrayList<>();

            for (StaticArgument staticArg : userArgs) {
                boolean isScalar = staticArg.getTraits().contains(StaticArgumentTrait.SCALAR);
                boolean isTableArg =
                        staticArg.getTraits().contains(StaticArgumentTrait.ROW_SEMANTIC_TABLE)
                                || staticArg
                                        .getTraits()
                                        .contains(StaticArgumentTrait.SET_SEMANTIC_TABLE);

                if (isScalar || isTableArg) {
                    ArgumentInfo argInfo = buildArgumentInfo(staticArg);
                    arguments.add(argInfo);
                } else {
                    throw new IllegalStateException(
                            "Unknown argument type for StaticArgument. "
                                    + "Expected SCALAR, ROW_SEMANTIC_TABLE, or SET_SEMANTIC_TABLE trait.");
                }
            }

            validateArgumentConfiguration(arguments);

            return arguments;
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
                                                + "Use either @ArgumentHint(type = ...) OR .withTableArgument(...), not both.",
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
                                                + "Use @ArgumentHint(type = @DataTypeHint(\"ROW<...>\")) or "
                                                + ".withTableArgument(\"%s\", DataTypes.of(\"ROW<...>\"))",
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

            return new ArgumentInfo(
                    name, dataType, primaryTrait, partitionColumnNames, hasPassColumnsThrough);
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
                                "No partition configuration found for table argument '%s'. "
                                        + "Use withPartitionBy(\"%s\", ...) to configure partitioning.",
                                name, name));
            }

            LogicalType logicalType = dataType.getLogicalType();
            List<String> fieldNames = new ArrayList<>();

            if (logicalType instanceof RowType) {
                RowType rowType = (RowType) logicalType;
                for (RowType.RowField field : rowType.getFields()) {
                    fieldNames.add(field.getName());
                }
            } else if (logicalType instanceof StructuredType) {
                StructuredType structuredType = (StructuredType) logicalType;
                for (StructuredType.StructuredAttribute attr : structuredType.getAttributes()) {
                    fieldNames.add(attr.getName());
                }
            } else {
                throw new IllegalStateException(
                        String.format(
                                "Unsupported data type for partitioning: %s. "
                                        + "Only Row and structured types (POJOs) support partitioning.",
                                dataType));
            }

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

        private void validateArgumentConfiguration(List<ArgumentInfo> arguments) {
            for (ArgumentInfo arg : arguments) {
                if (arg.isScalar) {
                    if (!scalarArgs.containsKey(arg.name)) {
                        throw new IllegalStateException(
                                String.format(
                                        "Missing required scalar argument '%s'. "
                                                + "Use .withScalarArgument(\"%s\", ...)",
                                        arg.name, arg.name));
                    }
                } else {
                    // For table arguments: builder config is optional if type comes from annotation
                    boolean hasBuilderConfig = tableArgs.containsKey(arg.name);
                    boolean hasInlineType = arg.dataType != null;

                    if (!hasBuilderConfig && !hasInlineType) {
                        throw new IllegalStateException(
                                String.format(
                                        "Missing required table argument '%s'. "
                                                + "Either specify @ArgumentHint(type = @DataTypeHint(...)) "
                                                + "or use .withTableArgument(\"%s\", ...)",
                                        arg.name, arg.name));
                    }
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
    }

    /**
     * Metadata for a single argument extracted from type inference.
     *
     * <p>Represents validated argument information combining PTF signature, type inference results,
     * and builder configuration.
     *
     * <p>Position in eval() signature is implicit from the list order.
     */
    private static class ArgumentInfo {
        final String name;
        final DataType dataType;
        final String[] partitionColumnNames; // nullable - only for SET_SEMANTIC_TABLE
        final boolean isScalar;
        final boolean isTableArgument;
        final boolean isSetSemantic;
        final boolean hasPassColumnsThrough;

        ArgumentInfo(
                String name,
                DataType dataType,
                ArgumentTrait primaryTrait,
                String[] partitionColumnNames,
                boolean hasPassColumnsThrough) {
            this.name = name;
            this.dataType = dataType;
            this.partitionColumnNames = partitionColumnNames;
            this.isScalar = (primaryTrait == ArgumentTrait.SCALAR);
            this.isTableArgument = (primaryTrait != ArgumentTrait.SCALAR);
            this.isSetSemantic = (primaryTrait == ArgumentTrait.SET_SEMANTIC_TABLE);
            this.hasPassColumnsThrough = hasPassColumnsThrough;
        }
    }

    private static class TableArgumentConfiguration {
        AbstractDataType<?> explicitType; // from withTableArgument
    }

    private static class ScalarArgumentConfiguration {
        Object value;

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
}
