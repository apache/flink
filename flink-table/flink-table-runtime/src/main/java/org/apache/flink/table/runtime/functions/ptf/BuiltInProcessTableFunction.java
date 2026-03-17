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
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.utils.DataTypeUtils;

import java.util.Set;

/**
 * Base class for built-in process table functions that are constructed from {@link
 * BuiltInFunctionDefinition#specialize(SpecializedContext)}.
 *
 * <p>Subclasses must offer a constructor that takes {@link SpecializedContext}.
 *
 * <p>By default, all built-in PTFs work on internal data structures. The output type is captured at
 * specialization time and converted to internal representation (e.g. {@code RowData} instead of
 * {@code Row}), matching how other built-in functions operate.
 */
@Internal
public abstract class BuiltInProcessTableFunction<T> extends ProcessTableFunction<T> {

    private final transient BuiltInFunctionDefinition definition;

    private final transient DataType internalOutputDataType;

    protected BuiltInProcessTableFunction(
            final BuiltInFunctionDefinition definition, final SpecializedContext context) {
        this.definition = definition;
        this.internalOutputDataType =
                context.getCallContext()
                        .getOutputDataType()
                        .map(DataTypeUtils::toInternalDataType)
                        .orElseThrow(IllegalStateException::new);
    }

    @Override
    public TypeInference getTypeInference(final DataTypeFactory typeFactory) {
        final TypeInference original = definition.getTypeInference(typeFactory);

        final TypeInference.Builder builder =
                TypeInference.newBuilder()
                        .inputTypeStrategy(original.getInputTypeStrategy())
                        .stateTypeStrategies(original.getStateTypeStrategies())
                        .outputTypeStrategy(TypeStrategies.explicit(internalOutputDataType))
                        .disableSystemArguments(original.disableSystemArguments());

        original.getStaticArguments().ifPresent(builder::staticArguments);

        return builder.build();
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
