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
import java.util.stream.IntStream;

/**
 * Base class for built-in process table functions that are constructed from {@link
 * BuiltInFunctionDefinition#specialize(SpecializedContext)}.
 *
 * <p>Subclasses must offer a constructor that takes {@link SpecializedContext}.
 *
 * <p>By default, all built-in PTFs work on internal data structures. Table arguments are converted
 * to internal representation (e.g. {@code RowData} instead of {@code Row}), while scalar arguments
 * (DESCRIPTOR, MAP, etc.) remain in their external form. Output and state types are also converted
 * to internal representation.
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

        // Convert TABLE args to internal (RowData), leave DESCRIPTOR/MAP args external
        final List<DataType> argTypes = callContext.getArgumentDataTypes();
        this.argumentDataTypes =
                IntStream.range(0, argTypes.size())
                        .mapToObj(
                                pos ->
                                        callContext.getTableSemantics(pos).isPresent()
                                                ? DataTypeUtils.toInternalDataType(argTypes.get(pos))
                                                : argTypes.get(pos))
                        .collect(Collectors.toList());

        this.internalOutputDataType =
                callContext
                        .getOutputDataType()
                        .map(DataTypeUtils::toInternalDataType)
                        .orElseThrow(IllegalStateException::new);

        this.internalStateStrategies =
                toInternalStateStrategies(
                        definition.getTypeInference(null), callContext);
    }

    // typedArguments is used here (same as BuiltInScalarFunction) to provide
    // enriched argument types with internal conversion classes instead of external static types
    @SuppressWarnings("deprecation")
    @Override
    public TypeInference getTypeInference(final DataTypeFactory typeFactory) {
        final TypeInference original = definition.getTypeInference(typeFactory);

        return TypeInference.newBuilder()
                .typedArguments(argumentDataTypes)
                .stateTypeStrategies(internalStateStrategies)
                .outputTypeStrategy(TypeStrategies.explicit(internalOutputDataType))
                .disableSystemArguments(original.disableSystemArguments())
                .build();
    }

    /** Wraps each state type strategy to produce internal data types, preserving TTL. */
    private static LinkedHashMap<String, StateTypeStrategy> toInternalStateStrategies(
            final TypeInference original, final CallContext callContext) {
        final LinkedHashMap<String, StateTypeStrategy> result = new LinkedHashMap<>();
        original.getStateTypeStrategies()
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
                                                strategy.getTimeToLive(callContext)
                                                        .orElse(null))));
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
