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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import java.util.Objects;
import java.util.Set;

/**
 * The function definition of an user-defined scalar function.
 *
 * <p>This class can be dropped once we introduce a new type inference.
 */
@PublicEvolving
public final class ScalarFunctionDefinition implements FunctionDefinition {

	private final String name;
	private final ScalarFunction scalarFunction;

	public ScalarFunctionDefinition(String name, ScalarFunction scalarFunction) {
		this.name = Preconditions.checkNotNull(name);
		this.scalarFunction = Preconditions.checkNotNull(scalarFunction);
	}

	public ScalarFunction getScalarFunction() {
		return scalarFunction;
	}

	@Override
	public FunctionKind getKind() {
		return FunctionKind.SCALAR;
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
