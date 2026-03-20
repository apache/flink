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

package org.apache.flink.table.runtime.functions.ptf;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.FunctionRequirement;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.StateTypeStrategy;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.utils.DataTypeUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Base class for built-in process table functions that are constructed from {@link
 * BuiltInFunctionDefinition#specialize(SpecializedContext)}.
 *
 * <p>Subclasses must offer a constructor that takes {@link SpecializedContext}.
 *
 * <p>By default, all built-in PTFs work on internal data structures. All argument types, output
 * type, and state types are converted to internal representation (e.g. {@code RowData} instead of
 * {@code Row}, {@code MapData} instead of {@code Map}).
 */
@Internal
public abstract class BuiltInProcessTableFunction<T> extends ProcessTableFunction<T> {

    private final transient BuiltInFunctionDefinition definition;

    private final transient List<DataType> argumentDataTypes;

    private final transient DataType internalOutputDataType;

    private final transient LinkedHashMap<String, StateTypeStrategy> internalStateStrategies;

    protected BuiltInProcessTableFunction(
            final BuiltInFunctionDefinition definition, final SpecializedContext context) {
        this.definition = definition;
        final CallContext callContext = context.getCallContext();
        final TypeInference definitionTypeInference =
                definition.getTypeInference(callContext.getDataTypeFactory());

        this.argumentDataTypes =
                callContext.getArgumentDataTypes().stream()
                        .map(DataTypeUtils::toInternalDataType)
                        .collect(Collectors.toList());

        this.internalOutputDataType =
                callContext
                        .getOutputDataType()
                        .map(DataTypeUtils::toInternalDataType)
                        .orElseThrow(IllegalStateException::new);

        this.internalStateStrategies =
                toInternalStateStrategies(definitionTypeInference, callContext);
    }

    // Uses deprecated typedArguments() because staticArguments() does not apply the conversion
    // class to TABLE arguments in TypeInferenceUtil.inferInputTypes, and we want to use internal
    // types in our built in functions.
    @SuppressWarnings("deprecation")
    @Override
    public TypeInference getTypeInference(final DataTypeFactory typeFactory) {
        final TypeInference definitionTypeInference = definition.getTypeInference(typeFactory);

        return TypeInference.newBuilder()
                .typedArguments(argumentDataTypes)
                .stateTypeStrategies(internalStateStrategies)
                .outputTypeStrategy(TypeStrategies.explicit(internalOutputDataType))
                .disableSystemArguments(definitionTypeInference.disableSystemArguments())
                .build();
    }

    /** Wraps each state type strategy to produce internal data types, preserving TTL. */
    private static LinkedHashMap<String, StateTypeStrategy> toInternalStateStrategies(
            final TypeInference definitionTypeInference, final CallContext callContext) {
        final LinkedHashMap<String, StateTypeStrategy> result = new LinkedHashMap<>();
        definitionTypeInference
                .getStateTypeStrategies()
                .forEach(
                        (name, strategy) ->
                                result.put(
                                        name,
                                        StateTypeStrategy.of(
                                                ctx ->
                                                        strategy.inferType(ctx)
                                                                .map(
                                                                        DataTypeUtils
                                                                                ::toInternalDataType),
                                                strategy.getTimeToLive(callContext).orElse(null))));
        return result;
    }

    @Override
    public Set<FunctionRequirement> getRequirements() {
        if (definition != null) {
            return definition.getRequirements();
        }
        return super.getRequirements();
    }

    @Override
    public boolean isDeterministic() {
        if (definition != null) {
            return definition.isDeterministic();
        }
        return super.isDeterministic();
    }
}
