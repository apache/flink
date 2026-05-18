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
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.table.types.inference.SystemTypeInference;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategy;
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

    /** Holds input and output converters for a table argument. */
    private static class ConverterPair {
        final DataStructureConverter<Object, Object> input;
        final DataStructureConverter<Object, Object> output;

        ConverterPair(
                DataStructureConverter<Object, Object> input,
                DataStructureConverter<Object, Object> output) {
            this.input = input;
            this.output = output;
        }
    }

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

    private boolean hasTableArguments = false;
    private final Map<String, ConverterPair> argumentConverters;
    private final DataStructureConverter<Object, Object> harnessOutputConverter;

    private ProcessTableFunctionTestHarness(
            ProcessTableFunction<OUT> function,
            FunctionContext functionContext,
            Method evalMethod,
            List<ArgumentInfo> arguments,
            Map<String, Object> scalarArgumentValues,
            Map<String, ConverterPair> argumentConverters,
            DataStructureConverter<Object, Object> harnessOutputConverter)
            throws Exception {
        this.function = function;
        this.functionContext = functionContext;
        this.evalMethod = evalMethod;
        this.arguments = arguments;
        this.scalarArgumentValues = scalarArgumentValues;
        this.argumentConverters = argumentConverters;
        this.harnessOutputConverter = harnessOutputConverter;
        this.output = new ArrayList<>();
        this.collector = new HarnessCollector();
        this.isOpen = false;

        this.argumentsByName = new HashMap<>();
        for (ArgumentInfo arg : arguments) {
            if (arg.name != null) {
                argumentsByName.put(arg.name, arg);
            }
        }

        final List<ArgumentInfo> tableArguments = new ArrayList<>();
        for (ArgumentInfo arg : arguments) {
            if (arg.isTableArgument) {
                tableArguments.add(arg);
                this.hasTableArguments = true;
            }
        }

        if (tableArguments.size() == 1) {
            this.defaultTableArgument = tableArguments.get(0).name;
            this.isSingleTableFunction = true;
        } else {
            this.defaultTableArgument = null;
            this.isSingleTableFunction = false;
        }

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

        Object[] args = arguments.stream().map(arg -> scalarArgumentValues.get(arg.name)).toArray();

        try {
            evalMethod.invoke(function, args);
        } catch (InvocationTargetException e) {
            handleEvalInvocationException(
                    "Exception occurred during scalar-only PTF eval() invocation.\n", args, e);
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

                ConverterPair pair = argumentConverters.get(arg.name);

                args[i] = pair.output.toExternalOrNull(pair.input.toInternalOrNull(activeRow));

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
            handleEvalInvocationException(contextMessage, args, e);
        }
    }

    /** Collector implementation that stores output in the harness. */
    private class HarnessCollector implements Collector<OUT> {
        private ArgumentInfo activeTableArg;
        private Row activeRow;

        void setContext(ArgumentInfo tableArg, Row row) {
            this.activeTableArg = tableArg;
            this.activeRow = row;
        }

        @Override
        public void collect(OUT record) {
            OUT finalRecord;

            if (activeTableArg == null || !activeTableArg.isTableArgument) {
                finalRecord = record;
            } else {
                switch (activeTableArg.prependStrategy) {
                    case ALL_COLUMNS:
                        finalRecord = prependAllColumns(record);
                        break;
                    case PARTITION_KEYS:
                        finalRecord = prependPartitionKeys(record);
                        break;
                    case NONE:
                        finalRecord = record;
                        break;
                    default:
                        throw new IllegalStateException(
                                "Unknown prepend strategy: " + activeTableArg.prependStrategy);
                }
            }

            // After prepending, round-trip through converter to ensure output has proper
            // field structure from derived output schema
            OUT structuredRecord = applyOutputConverter(finalRecord);
            output.add(structuredRecord);
        }

        @SuppressWarnings("unchecked")
        private OUT applyOutputConverter(OUT record) {
            if (record instanceof Row) {
                Object internal = harnessOutputConverter.toInternalOrNull(record);
                Object external = harnessOutputConverter.toExternalOrNull(internal);
                return (OUT) external;
            }
            return record;
        }

        @SuppressWarnings("unchecked")
        private OUT prependPartitionKeys(OUT ptfOutput) {
            if (!(ptfOutput instanceof Row)) {
                throw new IllegalStateException(
                        "Cannot prepend partition keys to non-Row output type: "
                                + ptfOutput.getClass());
            }

            Row ptfRow = (Row) ptfOutput;

            int totalPartitionKeyCount = 0;
            for (ArgumentInfo arg : arguments) {
                if (arg.isSetSemantic && arg.partitionColumnNames != null) {
                    totalPartitionKeyCount += arg.partitionColumnNames.length;
                }
            }

            int ptfOutputArity = ptfRow.getArity();
            int totalArity = totalPartitionKeyCount + ptfOutputArity;

            Row result = new Row(ptfRow.getKind(), totalArity);

            // Extract partition key values from active row
            Object[] partitionKeyValues = new Object[activeTableArg.partitionColumnNames.length];
            for (int i = 0; i < activeTableArg.partitionColumnNames.length; i++) {
                String columnName = activeTableArg.partitionColumnNames[i];
                int columnIndex = getFieldIndex(activeTableArg.dataType, columnName);
                partitionKeyValues[i] = activeRow.getField(columnIndex);
            }

            int resultIndex = 0;
            for (ArgumentInfo arg : arguments) {
                if (arg.isSetSemantic && arg.partitionColumnNames != null) {
                    for (int i = 0; i < arg.partitionColumnNames.length; i++) {
                        result.setField(resultIndex++, partitionKeyValues[i]);
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
            List<String> fieldNames = getFieldNames(dataType);
            int index = fieldNames.indexOf(fieldName);
            if (index < 0) {
                throw new IllegalStateException(
                        String.format("Field '%s' not found in type %s", fieldName, dataType));
            }
            return index;
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

    /** Extracts field names from RowType or StructuredType. */
    private static List<String> getFieldNames(DataType dataType) {
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
                            "Unsupported data type: %s. "
                                    + "Only Row and structured types are supported.",
                            dataType));
        }

        return fieldNames;
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

            DataTypeFactory dataTypeFactory = createDataTypeFactory();
            TypeInference baseTypeInference = function.getTypeInference(dataTypeFactory);
            TypeInference systemTypeInference =
                    SystemTypeInference.of(FunctionKind.PROCESS_TABLE, baseTypeInference);

            List<ArgumentInfo> arguments =
                    extractAndValidateTypeInference(function, systemTypeInference);

            FunctionContext functionContext =
                    new FunctionContext(null, Thread.currentThread().getContextClassLoader(), null);

            Method evalMethod = findEvalMethod();

            validateEvalMethodSupported(evalMethod, arguments);
            validatePartitionConsistency(arguments);

            Map<String, ConverterPair> argumentConverters = new HashMap<>();
            createConverters(arguments, argumentConverters);

            // Derive output schema using SystemTypeInference (includes deduplication)
            DataType derivedOutputType =
                    deriveOutputTypeFromSystemInference(
                            function, dataTypeFactory, systemTypeInference, arguments);

            // Create output converter for PTF emissions
            DataStructureConverter<Object, Object> harnessOutputConverter =
                    createPTFOutputConverter(derivedOutputType);

            return new ProcessTableFunctionTestHarness<>(
                    function,
                    functionContext,
                    evalMethod,
                    arguments,
                    extractScalarValues(arguments),
                    argumentConverters,
                    harnessOutputConverter);
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
         * Creates and initializes data structure converters for all table arguments.
         *
         * <p>For Row types, both input and output converters are the same (between Row and
         * RowData).
         *
         * <p>For structured types, input converter uses Row types (Row to RowData), and the output
         * converter uses the structured type.
         */
        private void createConverters(
                List<ArgumentInfo> arguments, Map<String, ConverterPair> argumentConverters) {
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

                        argumentConverters.put(
                                converterKey, new ConverterPair(inputConverter, outputConverter));
                    } else {
                        // For Row types, input and output converters are the same
                        DataStructureConverter<Object, Object> converter =
                                DataStructureConverters.getConverter(arg.dataType);
                        converter.open(classLoader);

                        argumentConverters.put(
                                converterKey, new ConverterPair(converter, converter));
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

            if (parameters.length != arguments.size()) {
                throw new IllegalStateException(
                        String.format(
                                "Parameter count mismatch: eval() has %d parameters but only %d arguments were extracted. "
                                        + "This may indicate missing @ArgumentHint annotations.",
                                parameters.length, arguments.size()));
            }

            for (int i = 0; i < parameters.length; i++) {
                Parameter param = parameters[i];
                Class<?> paramType = param.getType();
                ArgumentInfo arg = arguments.get(i);

                if (arg.isScalar) {
                    ScalarArgumentConfiguration config = scalarArgs.get(arg.name);
                    if (config != null
                            && config.value != null
                            && !paramType.isAssignableFrom(config.value.getClass())) {
                        throw new IllegalStateException(
                                String.format(
                                        "Type mismatch for scalar argument '%s' at position %d: "
                                                + "eval() parameter expects %s but provided value is %s",
                                        arg.name,
                                        i,
                                        paramType.getName(),
                                        config.value.getClass().getName()));
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
            final List<ArgumentInfo> partitionedTables = new ArrayList<>();
            for (ArgumentInfo arg : arguments) {
                if (arg.isSetSemantic && arg.partitionColumnNames != null) {
                    partitionedTables.add(arg);
                }
            }

            if (partitionedTables.size() <= 1) {
                return;
            }

            final ArgumentInfo first = partitionedTables.get(0);
            final int expectedPartitionColumnCount = first.partitionColumnNames.length;

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
            if (!(arg.dataType instanceof FieldsDataType)) {
                throw new IllegalStateException(
                        String.format(
                                "Cannot extract data type for partition column '%s' of argument '%s': "
                                        + "argument data type is not a FieldsDataType (actual: %s)",
                                columnName, arg.name, arg.dataType.getClass().getSimpleName()));
            }

            FieldsDataType fieldsDataType = (FieldsDataType) arg.dataType;
            List<String> fieldNames = getFieldNames(arg.dataType);
            List<DataType> fieldDataTypes = fieldsDataType.getChildren();

            int fieldIndex = fieldNames.indexOf(columnName);
            if (fieldIndex >= 0) {
                return fieldDataTypes.get(fieldIndex);
            }

            throw new IllegalStateException(
                    String.format(
                            "Partition column '%s' not found in argument '%s'",
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
                ProcessTableFunction<OUT> function, TypeInference systemTypeInference) {

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
                if (arg.isScalar && !scalarArgs.containsKey(arg.name)) {
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
         * Derives the output schema using SystemTypeInference, including field name deduplication.
         */
        private DataType deriveOutputTypeFromSystemInference(
                ProcessTableFunction<OUT> function,
                DataTypeFactory dataTypeFactory,
                TypeInference systemTypeInference,
                List<ArgumentInfo> arguments) {

            List<DataType> argumentDataTypes = new ArrayList<>();
            for (ArgumentInfo arg : arguments) {
                argumentDataTypes.add(arg.dataType);
            }

            Map<Integer, TableSemantics> tableSemanticsMap = new HashMap<>();
            for (int i = 0; i < arguments.size(); i++) {
                ArgumentInfo arg = arguments.get(i);
                if (arg.isTableArgument) {
                    int[] partitionIndices = getPartitionColumnIndices(arg);
                    TableSemantics semantics =
                            new TestHarnessTableSemantics(arg.dataType, partitionIndices);
                    tableSemanticsMap.put(i, semantics);
                }
            }

            TestHarnessCallContext callContext = new TestHarnessCallContext();
            callContext.typeFactory = dataTypeFactory;
            callContext.argumentDataTypes = argumentDataTypes;
            callContext.functionDefinition = function;
            callContext.tableSemantics = tableSemanticsMap;
            callContext.name = function.getClass().getSimpleName();

            TypeStrategy outputStrategy = systemTypeInference.getOutputTypeStrategy();
            Optional<DataType> outputTypeOpt = outputStrategy.inferType(callContext);

            if (outputTypeOpt.isEmpty()) {
                throw new IllegalStateException(
                        "Failed to derive output type from SystemTypeInference for "
                                + function.getClass().getSimpleName());
            }

            return outputTypeOpt.get();
        }

        private static List<String> extractFieldNames(DataType dataType) {
            LogicalType logicalType = dataType.getLogicalType();
            if (logicalType instanceof RowType) {
                return ((RowType) logicalType).getFieldNames();
            } else if (logicalType instanceof StructuredType) {
                return ((StructuredType) logicalType)
                        .getAttributes().stream()
                                .map(StructuredType.StructuredAttribute::getName)
                                .collect(java.util.stream.Collectors.toList());
            } else {
                throw new IllegalStateException(
                        "Expected RowType or StructuredType, got: "
                                + logicalType.getClass().getSimpleName());
            }
        }

        private int[] getPartitionColumnIndices(ArgumentInfo arg) {
            if (arg.partitionColumnNames == null || arg.partitionColumnNames.length == 0) {
                return new int[0];
            }

            List<String> fieldNames = extractFieldNames(arg.dataType);

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
    }

    private enum OutputPrependStrategy {
        NONE,
        PARTITION_KEYS,
        ALL_COLUMNS
    }

    private void handleEvalInvocationException(
            String contextMessage, Object[] args, InvocationTargetException e) throws Exception {
        Throwable cause = e.getCause();
        StringBuilder details = new StringBuilder();
        details.append(contextMessage);
        details.append("Expected parameter types: ");
        details.append(Arrays.toString(evalMethod.getParameterTypes()));
        details.append("\nActual arguments:\n");
        for (int i = 0; i < arguments.size(); i++) {
            ArgumentInfo arg = arguments.get(i);
            Object value = args[i];
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
     * Metadata for a single argument extracted from type inference.
     *
     * <p>Represents validated argument information combining PTF signature, type inference results,
     * and builder configuration.
     */
    private static class ArgumentInfo {
        final String name;
        final DataType dataType;
        final String[] partitionColumnNames;
        final boolean isScalar;
        final boolean isTableArgument;
        final boolean isSetSemantic;
        final OutputPrependStrategy prependStrategy;

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
            this.prependStrategy =
                    hasPassColumnsThrough
                            ? OutputPrependStrategy.ALL_COLUMNS
                            : (this.isSetSemantic && partitionColumnNames != null)
                                    ? OutputPrependStrategy.PARTITION_KEYS
                                    : OutputPrependStrategy.NONE;
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
}
