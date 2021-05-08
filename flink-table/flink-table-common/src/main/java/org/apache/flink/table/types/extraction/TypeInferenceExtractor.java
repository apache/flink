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
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.inference.TypeStrategy;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.flink.table.types.extraction.ExtractionUtils.extractionError;
import static org.apache.flink.table.types.extraction.FunctionMappingExtractor.createGenericResultExtraction;
import static org.apache.flink.table.types.extraction.FunctionMappingExtractor.createParameterAndReturnTypeVerification;
import static org.apache.flink.table.types.extraction.FunctionMappingExtractor.createParameterSignatureExtraction;
import static org.apache.flink.table.types.extraction.FunctionMappingExtractor.createParameterVerification;
import static org.apache.flink.table.types.extraction.FunctionMappingExtractor.createParameterWithAccumulatorVerification;
import static org.apache.flink.table.types.extraction.FunctionMappingExtractor.createParameterWithArgumentVerification;
import static org.apache.flink.table.types.extraction.FunctionMappingExtractor.createReturnTypeResultExtraction;

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
                        createParameterSignatureExtraction(0),
                        null,
                        createReturnTypeResultExtraction(),
                        createParameterAndReturnTypeVerification());
        return extractTypeInference(mappingExtractor);
    }

    /** Extracts a type inference from a {@link AggregateFunction}. */
    public static TypeInference forAggregateFunction(
            DataTypeFactory typeFactory, Class<? extends AggregateFunction<?, ?>> function) {
        final FunctionMappingExtractor mappingExtractor =
                new FunctionMappingExtractor(
                        typeFactory,
                        function,
                        UserDefinedFunctionHelper.AGGREGATE_ACCUMULATE,
                        createParameterSignatureExtraction(1),
                        createGenericResultExtraction(AggregateFunction.class, 1, false),
                        createGenericResultExtraction(AggregateFunction.class, 0, true),
                        createParameterWithAccumulatorVerification());
        return extractTypeInference(mappingExtractor);
    }

    /** Extracts a type inference from a {@link TableFunction}. */
    public static TypeInference forTableFunction(
            DataTypeFactory typeFactory, Class<? extends TableFunction<?>> function) {
        final FunctionMappingExtractor mappingExtractor =
                new FunctionMappingExtractor(
                        typeFactory,
                        function,
                        UserDefinedFunctionHelper.TABLE_EVAL,
                        createParameterSignatureExtraction(0),
                        null,
                        createGenericResultExtraction(TableFunction.class, 0, true),
                        createParameterVerification());
        return extractTypeInference(mappingExtractor);
    }

    /** Extracts a type inference from a {@link TableAggregateFunction}. */
    public static TypeInference forTableAggregateFunction(
            DataTypeFactory typeFactory, Class<? extends TableAggregateFunction<?, ?>> function) {
        final FunctionMappingExtractor mappingExtractor =
                new FunctionMappingExtractor(
                        typeFactory,
                        function,
                        UserDefinedFunctionHelper.TABLE_AGGREGATE_ACCUMULATE,
                        createParameterSignatureExtraction(1),
                        createGenericResultExtraction(TableAggregateFunction.class, 1, false),
                        createGenericResultExtraction(TableAggregateFunction.class, 0, true),
                        createParameterWithAccumulatorVerification());
        return extractTypeInference(mappingExtractor);
    }

    /** Extracts a type inference from a {@link AsyncTableFunction}. */
    public static TypeInference forAsyncTableFunction(
            DataTypeFactory typeFactory, Class<? extends AsyncTableFunction<?>> function) {
        final FunctionMappingExtractor mappingExtractor =
                new FunctionMappingExtractor(
                        typeFactory,
                        function,
                        UserDefinedFunctionHelper.ASYNC_TABLE_EVAL,
                        createParameterSignatureExtraction(1),
                        null,
                        createGenericResultExtraction(AsyncTableFunction.class, 0, true),
                        createParameterWithArgumentVerification(CompletableFuture.class));
        return extractTypeInference(mappingExtractor);
    }

    private static TypeInference extractTypeInference(FunctionMappingExtractor mappingExtractor) {
        try {
            return extractTypeInferenceOrError(mappingExtractor);
        } catch (Throwable t) {
            throw extractionError(
                    t,
                    "Could not extract a valid type inference for function class '%s'. "
                            + "Please check for implementation mistakes and/or provide a corresponding hint.",
                    mappingExtractor.getFunction().getName());
        }
    }

    private static TypeInference extractTypeInferenceOrError(
            FunctionMappingExtractor mappingExtractor) {
        final Map<FunctionSignatureTemplate, FunctionResultTemplate> outputMapping =
                mappingExtractor.extractOutputMapping();

        if (!mappingExtractor.hasAccumulator()) {
            return buildInference(null, outputMapping);
        }

        final Map<FunctionSignatureTemplate, FunctionResultTemplate> accumulatorMapping =
                mappingExtractor.extractAccumulatorMapping();
        return buildInference(accumulatorMapping, outputMapping);
    }

    private static TypeInference buildInference(
            @Nullable Map<FunctionSignatureTemplate, FunctionResultTemplate> accumulatorMapping,
            Map<FunctionSignatureTemplate, FunctionResultTemplate> outputMapping) {
        final TypeInference.Builder builder = TypeInference.newBuilder();

        configureNamedArguments(builder, outputMapping);
        configureTypedArguments(builder, outputMapping);

        builder.inputTypeStrategy(translateInputTypeStrategy(outputMapping));

        if (accumulatorMapping != null) {
            // verify that accumulator and output are derived from the same input strategy
            if (!accumulatorMapping.keySet().equals(outputMapping.keySet())) {
                throw extractionError(
                        "Mismatch between accumulator signature and output signature. "
                                + "Both intermediate and output results must be derived from the same input strategy.");
            }
            builder.accumulatorTypeStrategy(translateResultTypeStrategy(accumulatorMapping));
        }

        builder.outputTypeStrategy(translateResultTypeStrategy(outputMapping));
        return builder.build();
    }

    private static void configureNamedArguments(
            TypeInference.Builder builder,
            Map<FunctionSignatureTemplate, FunctionResultTemplate> outputMapping) {
        final Set<FunctionSignatureTemplate> signatures = outputMapping.keySet();
        if (signatures.stream().anyMatch(s -> s.isVarArgs || s.argumentNames == null)) {
            return;
        }
        final Set<List<String>> argumentNames =
                signatures.stream()
                        .map(
                                s -> {
                                    assert s.argumentNames != null;
                                    return Arrays.asList(s.argumentNames);
                                })
                        .collect(Collectors.toSet());
        if (argumentNames.size() != 1) {
            return;
        }
        builder.namedArguments(argumentNames.iterator().next());
    }

    private static void configureTypedArguments(
            TypeInference.Builder builder,
            Map<FunctionSignatureTemplate, FunctionResultTemplate> outputMapping) {
        if (outputMapping.size() != 1) {
            return;
        }
        final FunctionSignatureTemplate signature = outputMapping.keySet().iterator().next();
        final List<DataType> dataTypes =
                signature.argumentTemplates.stream()
                        .map(a -> a.dataType)
                        .collect(Collectors.toList());
        if (!signature.isVarArgs && dataTypes.stream().allMatch(Objects::nonNull)) {
            builder.typedArguments(dataTypes);
        }
    }

    private static TypeStrategy translateResultTypeStrategy(
            Map<FunctionSignatureTemplate, FunctionResultTemplate> resultMapping) {
        final Map<InputTypeStrategy, TypeStrategy> mappings =
                resultMapping.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        e -> e.getKey().toInputTypeStrategy(),
                                        e -> e.getValue().toTypeStrategy()));
        return TypeStrategies.mapping(mappings);
    }

    private static InputTypeStrategy translateInputTypeStrategy(
            Map<FunctionSignatureTemplate, FunctionResultTemplate> outputMapping) {
        return outputMapping.keySet().stream()
                .map(FunctionSignatureTemplate::toInputTypeStrategy)
                .reduce(InputTypeStrategies::or)
                .orElse(InputTypeStrategies.sequence());
    }
}
