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

package org.apache.flink.table.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.util.Preconditions;

import java.util.Objects;
import java.util.Set;

/**
 * A "marker" function definition of a user-defined scalar function that uses the old type system
 * stack.
 *
 * <p>This class can be dropped once we introduce a new type inference.
 */
@Internal
public final class ScalarFunctionDefinition implements FunctionDefinition {

	private final String name;
	private final ScalarFunction scalarFunction;

	public ScalarFunctionDefinition(String name, ScalarFunction scalarFunction) {
		this.name = Preconditions.checkNotNull(name);
		this.scalarFunction = Preconditions.checkNotNull(scalarFunction);
	}

	public String getName() {
		return name;
	}

	public ScalarFunction getScalarFunction() {
		return scalarFunction;
	}

	@Override
	public FunctionKind getKind() {
		return FunctionKind.SCALAR;
	}

	@Override
	public TypeInference getTypeInference(DataTypeFactory factory) {
		throw new TableException("Functions implemented for the old type system are not supported.");
	}

	@Override
	public Set<FunctionRequirement> getRequirements() {
		return scalarFunction.getRequirements();
	}

	@Override
	public boolean isDeterministic() {
		return scalarFunction.isDeterministic();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ScalarFunctionDefinition that = (ScalarFunctionDefinition) o;
		return name.equals(that.name);
	}

	@Override
	public int hashCode() {
		return Objects.hash(name);
	}

	@Override
	public String toString() {
		return name;
	}
}
