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

package org.apache.flink.table.types.extraction;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.AsyncScalarFunction;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.procedures.Procedure;
import org.apache.flink.table.procedures.ProcedureDefinition;
import org.apache.flink.table.types.extraction.FunctionResultTemplate.FunctionOutputTemplate;
import org.apache.flink.table.types.extraction.FunctionResultTemplate.FunctionStateTemplate;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.StateTypeStrategy;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.inference.TypeStrategy;

import javax.annotation.Nullable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.types.extraction.BaseMappingExtractor.createArgumentsFromParametersExtraction;
import static org.apache.flink.table.types.extraction.BaseMappingExtractor.createStateFromParametersExtraction;
import static org.apache.flink.table.types.extraction.ExtractionUtils.extractionError;
import static org.apache.flink.table.types.extraction.FunctionMappingExtractor.createOutputFromGenericInClass;
import static org.apache.flink.table.types.extraction.FunctionMappingExtractor.createOutputFromGenericInMethod;
import static org.apache.flink.table.types.extraction.FunctionMappingExtractor.createOutputFromReturnTypeInMethod;
import static org.apache.flink.table.types.extraction.FunctionMappingExtractor.createParameterAndCompletableFutureVerification;
import static org.apache.flink.table.types.extraction.FunctionMappingExtractor.createParameterAndOptionalContextVerification;
import static org.apache.flink.table.types.extraction.FunctionMappingExtractor.createParameterAndReturnTypeVerification;
import static org.apache.flink.table.types.extraction.FunctionMappingExtractor.createParameterVerification;
import static org.apache.flink.table.types.extraction.FunctionMappingExtractor.createStateFromGenericInClassOrParametersExtraction;
import static org.apache.flink.table.types.extraction.ProcedureMappingExtractor.createOutputFromArrayReturnTypeInMethod;
import static org.apache.flink.table.types.extraction.ProcedureMappingExtractor.createParameterWithOptionalContextAndArrayReturnTypeVerification;

/**
 * Reflection-based utility for extracting a {@link TypeInference} from a supported subclass of
 * {@link UserDefinedFunction}.
 *
 * <p>The behavior of this utility can be influenced by {@link DataTypeHint}s and {@link
 * FunctionHint}s which have higher precedence than the reflective information.
 *
 * <p>Note: This utility assumes that functions have been validated before regarding accessibility
 * of class/methods and serializability.
 */
@Internal
public final class TypeInferenceExtractor {

    /** Extracts a type inference from a {@link ScalarFunction}. */
    public static TypeInference forScalarFunction(
            DataTypeFactory typeFactory, Class<? extends ScalarFunction> function) {
        final FunctionMappingExtractor mappingExtractor =
                new FunctionMappingExtractor(
                        typeFactory,
                        function,
                        UserDefinedFunctionHelper.SCALAR_EVAL,
                        createArgumentsFromParametersExtraction(0),
                        null,
                        null,
                        createOutputFromReturnTypeInMethod(),
                        createParameterAndReturnTypeVerification());
        return extractTypeInference(mappingExtractor, false);
    }

    /** Extracts a type inference from a {@link AsyncScalarFunction}. */
    public static TypeInference forAsyncScalarFunction(
            DataTypeFactory typeFactory, Class<? extends AsyncScalarFunction> function) {
        final FunctionMappingExtractor mappingExtractor =
                new FunctionMappingExtractor(
                        typeFactory,
                        function,
                        UserDefinedFunctionHelper.ASYNC_SCALAR_EVAL,
                        createArgumentsFromParametersExtraction(1),
                        null,
                        null,
                        createOutputFromGenericInMethod(0, 0, true),
                        createParameterAndCompletableFutureVerification(function, false));
        return extractTypeInference(mappingExtractor, false);
    }

    /** Extracts a type inference from a {@link AggregateFunction}. */
    public static TypeInference forAggregateFunction(
            DataTypeFactory typeFactory, Class<? extends AggregateFunction<?, ?>> function) {
        final FunctionMappingExtractor mappingExtractor =
                new FunctionMappingExtractor(
                        typeFactory,
                        function,
                        UserDefinedFunctionHelper.AGGREGATE_ACCUMULATE,
                        createArgumentsFromParametersExtraction(1),
                        createStateFromGenericInClassOrParametersExtraction(
                                AggregateFunction.class, 1),
                        createParameterVerification(true),
                        createOutputFromGenericInClass(AggregateFunction.class, 0, true),
                        null);
        return extractTypeInference(mappingExtractor, false);
    }

    /** Extracts a type inference from a {@link TableFunction}. */
    public static TypeInference forTableFunction(
            DataTypeFactory typeFactory, Class<? extends TableFunction<?>> function) {
        final FunctionMappingExtractor mappingExtractor =
                new FunctionMappingExtractor(
                        typeFactory,
                        function,
                        UserDefinedFunctionHelper.TABLE_EVAL,
                        createArgumentsFromParametersExtraction(0),
                        null,
                        null,
                        createOutputFromGenericInClass(TableFunction.class, 0, true),
                        createParameterVerification(false));
        return extractTypeInference(mappingExtractor, false);
    }

    /** Extracts a type inference from a {@link TableAggregateFunction}. */
    public static TypeInference forTableAggregateFunction(
            DataTypeFactory typeFactory, Class<? extends TableAggregateFunction<?, ?>> function) {
        final FunctionMappingExtractor mappingExtractor =
                new FunctionMappingExtractor(
                        typeFactory,
                        function,
                        UserDefinedFunctionHelper.TABLE_AGGREGATE_ACCUMULATE,
                        createArgumentsFromParametersExtraction(1),
                        createStateFromGenericInClassOrParametersExtraction(
                                TableAggregateFunction.class, 1),
                        createParameterVerification(true),
                        createOutputFromGenericInClass(TableAggregateFunction.class, 0, true),
                        null);
        return extractTypeInference(mappingExtractor, false);
    }

    /** Extracts a type inference from a {@link AsyncTableFunction}. */
    public static TypeInference forAsyncTableFunction(
            DataTypeFactory typeFactory, Class<? extends AsyncTableFunction<?>> function) {
        final FunctionMappingExtractor mappingExtractor =
                new FunctionMappingExtractor(
                        typeFactory,
                        function,
                        UserDefinedFunctionHelper.ASYNC_TABLE_EVAL,
                        createArgumentsFromParametersExtraction(1),
                        null,
                        null,
                        createOutputFromGenericInClass(AsyncTableFunction.class, 0, true),
                        createParameterAndCompletableFutureVerification(function, true));
        return extractTypeInference(mappingExtractor, false);
    }

    /** Extracts a type inference from a {@link ProcessTableFunction}. */
    public static TypeInference forProcessTableFunction(
            DataTypeFactory typeFactory, Class<? extends ProcessTableFunction<?>> function) {
        final FunctionMappingExtractor mappingExtractor =
                new FunctionMappingExtractor(
                        typeFactory,
                        function,
                        UserDefinedFunctionHelper.PROCESS_TABLE_EVAL,
                        createArgumentsFromParametersExtraction(
                                0, ProcessTableFunction.Context.class),
                        createStateFromParametersExtraction(),
                        createParameterAndOptionalContextVerification(
                                ProcessTableFunction.Context.class, true),
                        createOutputFromGenericInClass(ProcessTableFunction.class, 0, true),
                        null);
        return extractTypeInference(mappingExtractor, true);
    }

    /** Extracts a type in inference from a {@link Procedure}. */
    public static TypeInference forProcedure(
            DataTypeFactory typeFactory, Class<? extends Procedure> procedure) {
        final ProcedureMappingExtractor mappingExtractor =
                new ProcedureMappingExtractor(
                        typeFactory,
                        procedure,
                        ProcedureDefinition.PROCEDURE_CALL,
                        createArgumentsFromParametersExtraction(1),
                        createOutputFromArrayReturnTypeInMethod(),
                        createParameterWithOptionalContextAndArrayReturnTypeVerification());
        return extractTypeInference(mappingExtractor);
    }

    private static TypeInference extractTypeInference(
            FunctionMappingExtractor mappingExtractor, boolean requiresStaticSignature) {
        try {
            return extractTypeInferenceOrError(mappingExtractor, requiresStaticSignature);
        } catch (Throwable t) {
            throw extractionError(
                    t,
                    "Could not extract a valid type inference for function class '%s'. "
                            + "Please check for implementation mistakes and/or provide a corresponding hint.",
                    mappingExtractor.getFunction().getName());
        }
    }

    private static TypeInference extractTypeInference(ProcedureMappingExtractor mappingExtractor) {
        try {
            final Map<FunctionSignatureTemplate, FunctionOutputTemplate> outputMapping =
                    mappingExtractor.extractOutputMapping();
            return buildInference(null, outputMapping, false);
        } catch (Throwable t) {
            throw extractionError(
                    t,
                    "Could not extract a valid type inference for procedure class '%s'. "
                            + "Please check for implementation mistakes and/or provide a corresponding hint.",
                    mappingExtractor.getFunctionClass().getName());
        }
    }

    private static TypeInference extractTypeInferenceOrError(
            FunctionMappingExtractor mappingExtractor, boolean requiresStaticSignature) {
        final Map<FunctionSignatureTemplate, FunctionOutputTemplate> outputMapping =
                mappingExtractor.extractOutputMapping();

        if (!mappingExtractor.supportsState()) {
            return buildInference(null, outputMapping, requiresStaticSignature);
        }

        final Map<FunctionSignatureTemplate, FunctionStateTemplate> stateMapping =
                mappingExtractor.extractStateMapping();

        return buildInference(stateMapping, outputMapping, requiresStaticSignature);
    }

    private static TypeInference buildInference(
            @Nullable Map<FunctionSignatureTemplate, FunctionStateTemplate> stateMapping,
            Map<FunctionSignatureTemplate, FunctionOutputTemplate> outputMapping,
            boolean requiresStaticSignature) {
        final TypeInference.Builder builder = TypeInference.newBuilder();

        if (!configureStaticArguments(builder, outputMapping)) {
            if (requiresStaticSignature) {
                throw extractionError(
                        "Process table functions require a non-overloaded, non-vararg, and static signature.");
            }
            builder.inputTypeStrategy(translateInputTypeStrategy(outputMapping));
        }

        if (stateMapping != null) {
            // verify that state and output are derived from the same signatures
            if (!stateMapping.keySet().equals(outputMapping.keySet())) {
                throw extractionError(
                        "Mismatch between state signature and output signature. "
                                + "Both intermediate and output results must be derived from the same input strategy.");
            }
            builder.stateTypeStrategies(translateStateTypeStrategies(stateMapping));
        }

        builder.outputTypeStrategy(translateOutputTypeStrategy(outputMapping));

        return builder.build();
    }

    private static boolean configureStaticArguments(
            TypeInference.Builder builder,
            Map<FunctionSignatureTemplate, FunctionOutputTemplate> outputMapping) {
        final Set<FunctionSignatureTemplate> signatures = outputMapping.keySet();
        if (signatures.size() != 1) {
            // Function is overloaded
            return false;
        }
        final List<StaticArgument> arguments = signatures.iterator().next().toStaticArguments();
        if (arguments == null) {
            // Function is var arg or non-static (e.g. uses input groups instead of typed arguments)
            return false;
        }
        builder.staticArguments(arguments);
        return true;
    }

    private static InputTypeStrategy translateInputTypeStrategy(
            Map<FunctionSignatureTemplate, FunctionOutputTemplate> outputMapping) {
        return outputMapping.keySet().stream()
                .map(FunctionSignatureTemplate::toInputTypeStrategy)
                .reduce(InputTypeStrategies::or)
                .orElse(InputTypeStrategies.sequence());
    }

    private static LinkedHashMap<String, StateTypeStrategy> translateStateTypeStrategies(
            Map<FunctionSignatureTemplate, FunctionStateTemplate> stateMapping) {
        // Simple signatures don't require a mapping, default for process table functions
        if (stateMapping.size() == 1) {
            final FunctionStateTemplate template =
                    stateMapping.entrySet().iterator().next().getValue();
            return template.toStateTypeStrategies();
        }
        // For overloaded signatures to accumulators in aggregating functions
        final Map<InputTypeStrategy, TypeStrategy> mappings =
                stateMapping.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        e -> e.getKey().toInputTypeStrategy(),
                                        e -> e.getValue().toAccumulatorTypeStrategy()));
        final StateTypeStrategy accumulatorStrategy =
                StateTypeStrategy.of(TypeStrategies.mapping(mappings));
        final Set<String> stateNames =
                stateMapping.values().stream()
                        .map(FunctionStateTemplate::toAccumulatorStateName)
                        .collect(Collectors.toSet());
        if (stateMapping.size() > 1 && stateNames.size() > 1) {
            throw extractionError(
                    "Overloaded aggregating functions must use the same name for state entries. "
                            + "Found: %s",
                    stateNames);
        }
        final String stateName = stateNames.iterator().next();
        final LinkedHashMap<String, StateTypeStrategy> stateTypeStrategies = new LinkedHashMap<>();
        stateTypeStrategies.put(stateName, accumulatorStrategy);
        return stateTypeStrategies;
    }

    private static TypeStrategy translateOutputTypeStrategy(
            Map<FunctionSignatureTemplate, FunctionOutputTemplate> outputMapping) {
        // Simple signatures don't require a mapping
        if (outputMapping.size() == 1) {
            final FunctionOutputTemplate template =
                    outputMapping.entrySet().iterator().next().getValue();
            return template.toTypeStrategy();
        }
        // For overloaded signatures
        final Map<InputTypeStrategy, TypeStrategy> mappings =
                outputMapping.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        e -> e.getKey().toInputTypeStrategy(),
                                        e -> e.getValue().toTypeStrategy(),
                                        (t1, t2) -> t2));
        return TypeStrategies.mapping(mappings);
    }
}
