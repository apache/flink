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
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.InputTypeValidator;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.util.Preconditions;

import java.util.List;

/**
 * Definition of a built-in function. It enables unique identification across different
 * modules by reference equality.
 *
 * <p>Compared to regular {@link FunctionDefinition}, built-in functions have a default name. This
 * default name is used to lookup the function in a catalog during resolution.
 *
 * <p>Equality is defined by reference equality.
 */
@Internal
public final class BuiltInFunctionDefinition implements FunctionDefinition {

	private final String name;

	private final FunctionKind kind;

	private final TypeInference typeInference;

	private BuiltInFunctionDefinition(
			String name,
			FunctionKind kind,
			TypeInference typeInference) {
		this.name = Preconditions.checkNotNull(name, "Name must not be null.");
		this.kind = Preconditions.checkNotNull(kind, "Kind must not be null.");
		this.typeInference = Preconditions.checkNotNull(typeInference, "Type inference must not be null.");
	}

	public String getName() {
		return name;
	}

	/**
	 * Currently, the type inference is just exposed here. In the future, function definition will
	 * require it.
	 */
	public TypeInference getTypeInference() {
		return typeInference;
	}

	@Override
	public FunctionKind getKind() {
		return kind;
	}

	@Override
	public String toString() {
		return name;
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Builder for fluent definition of built-in functions.
	 */
	public static class Builder {

		private String name;

		private FunctionKind kind;

		private TypeInference.Builder typeInferenceBuilder = new TypeInference.Builder();

		public Builder() {
			// default constructor to allow a fluent definition
		}

		public Builder name(String name) {
			this.name = name;
			return this;
		}

		public Builder kind(FunctionKind kind) {
			this.kind = kind;
			return this;
		}

		public Builder inputTypeValidator(InputTypeValidator inputTypeValidator) {
			this.typeInferenceBuilder.inputTypeValidator(inputTypeValidator);
			return this;
		}

		public Builder accumulatorTypeStrategy(TypeStrategy accumulatorTypeStrategy) {
			this.typeInferenceBuilder.accumulatorTypeStrategy(accumulatorTypeStrategy);
			return this;
		}

		public Builder outputTypeStrategy(TypeStrategy outputTypeStrategy) {
			this.typeInferenceBuilder.outputTypeStrategy(outputTypeStrategy);
			return this;
		}

		public Builder namedArguments(List<String> argumentNames) {
			this.typeInferenceBuilder.namedArguments(argumentNames);
			return this;
		}

		public Builder typedArguments(List<DataType> argumentTypes) {
			this.typeInferenceBuilder.typedArguments(argumentTypes);
			return this;
		}

		public BuiltInFunctionDefinition build() {
			return new BuiltInFunctionDefinition(name, kind, typeInferenceBuilder.build());
		}
	}
}
