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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.functions.FunctionRequirement;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.types.inference.TypeInference;

import java.util.Set;

/**
 * Base class for built-in functions that need another level of specialization via {@link
 * BuiltInFunctionDefinition#specialize(SpecializedContext)}.
 *
 * <p>Subclasses can create a specific UDF runtime implementation targeted to the given (fully
 * known) arguments and derived output type. Subclasses must provide a default constructor.
 */
@Internal
public abstract class BuiltInSpecializedFunction implements SpecializedFunction {

    private final BuiltInFunctionDefinition definition;

    protected BuiltInSpecializedFunction(BuiltInFunctionDefinition definition) {
        this.definition = definition;
    }

    @Override
    public FunctionKind getKind() {
        return definition.getKind();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return definition.getTypeInference(typeFactory);
    }

    @Override
    public Set<FunctionRequirement> getRequirements() {
        return definition.getRequirements();
    }

    @Override
    public boolean isDeterministic() {
        return definition.isDeterministic();
    }
}
