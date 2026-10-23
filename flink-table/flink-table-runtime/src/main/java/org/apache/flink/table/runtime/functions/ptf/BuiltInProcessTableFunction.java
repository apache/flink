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
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.api.dataview.ValueView;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.FunctionRequirement;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;
import org.apache.flink.table.runtime.dataview.DataViewUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.StateTypeStrategy;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.DataTypeUtils;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;
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
        this(definition, context, null);
    }

    /**
     * Creates a function whose state entries are declared by the specialized function itself
     * instead of the definition. Use {@link #internalState(Map.Entry[])} or {@link
     * #customState(Map.Entry[])} to create the strategies.
     */
    protected BuiltInProcessTableFunction(
            final BuiltInFunctionDefinition definition,
            final SpecializedContext context,
            @Nullable final LinkedHashMap<String, StateTypeStrategy> internalStateStrategies) {
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
                internalStateStrategies != null
                        ? internalStateStrategies
                        : toInternalStateStrategies(definitionTypeInference);
    }

    /**
     * Declares the state entries of a specialized function in order. The strategies may infer
     * external data types, they are converted to internal ones.
     */
    @SafeVarargs
    protected static LinkedHashMap<String, StateTypeStrategy> internalState(
            final Map.Entry<String, StateTypeStrategy>... entries) {
        return state(BuiltInProcessTableFunction::toInternalStateStrategy, entries);
    }

    /**
     * Declares the state entries of a specialized function in order. The strategies must already
     * infer the data types that the function works on, no internal conversion is applied.
     */
    @SafeVarargs
    protected static LinkedHashMap<String, StateTypeStrategy> customState(
            final Map.Entry<String, StateTypeStrategy>... entries) {
        return state(UnaryOperator.identity(), entries);
    }

    private static LinkedHashMap<String, StateTypeStrategy> state(
            final UnaryOperator<StateTypeStrategy> mapper,
            final Map.Entry<String, StateTypeStrategy>[] entries) {
        final LinkedHashMap<String, StateTypeStrategy> result = new LinkedHashMap<>();
        for (final Map.Entry<String, StateTypeStrategy> entry : entries) {
            result.put(entry.getKey(), mapper.apply(entry.getValue()));
        }
        return result;
    }

    private static StateTypeStrategy toInternalStateStrategy(final StateTypeStrategy strategy) {
        return new StateTypeStrategy() {
            @Override
            public Optional<DataType> inferType(final CallContext callContext) {
                return strategy.inferType(callContext)
                        .map(BuiltInProcessTableFunction::toInternalStateType);
            }

            @Override
            public Optional<Duration> getTimeToLive(final CallContext callContext) {
                return strategy.getTimeToLive(callContext);
            }

            @Override
            public boolean isBroadcast() {
                return strategy.isBroadcast();
            }
        };
    }

    /**
     * Converts a state type to its internal representation. For {@link ValueView}/{@link
     * MapView}/{@link ListView} only the value/key/element types are internalized; the view wrapper
     * is preserved, unlike {@link DataTypeUtils#toInternalDataType} which would rewrite the whole
     * view to {@code RowData}.
     */
    private static DataType toInternalStateType(final DataType stateType) {
        final LogicalType logicalType = stateType.getLogicalType();
        if (DataViewUtils.isDataView(logicalType, ValueView.class)) {
            final DataType valueType = stateType.getChildren().get(0);
            return ValueView.newValueViewDataType(DataTypeUtils.toInternalDataType(valueType));
        }
        if (DataViewUtils.isDataView(logicalType, MapView.class)) {
            final DataType mapType = stateType.getChildren().get(0);
            return MapView.newMapViewDataType(
                    DataTypeUtils.toInternalDataType(mapType.getChildren().get(0)),
                    DataTypeUtils.toInternalDataType(mapType.getChildren().get(1)));
        }
        if (DataViewUtils.isDataView(logicalType, ListView.class)) {
            final DataType arrayType = stateType.getChildren().get(0);
            return ListView.newListViewDataType(
                    DataTypeUtils.toInternalDataType(arrayType.getChildren().get(0)));
        }
        return DataTypeUtils.toInternalDataType(stateType);
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

    /** Wraps each state type strategy to produce internal data types. */
    private static LinkedHashMap<String, StateTypeStrategy> toInternalStateStrategies(
            final TypeInference definitionTypeInference) {
        final LinkedHashMap<String, StateTypeStrategy> result = new LinkedHashMap<>();
        definitionTypeInference
                .getStateTypeStrategies()
                .forEach((name, strategy) -> result.put(name, toInternalStateStrategy(strategy)));
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
