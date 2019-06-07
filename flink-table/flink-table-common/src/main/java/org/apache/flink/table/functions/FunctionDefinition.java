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

/**
 * Definition of a function for unique identification.
 */
@PublicEvolving
public class FunctionDefinition {

	/**
	 * Classifies the function definition.
	 */
	public enum Type {
		AGGREGATE_FUNCTION,
		SCALAR_FUNCTION,
		TABLE_FUNCTION,
		OTHER_FUNCTION
	}

	private final Type type;
	private final String name;

	public FunctionDefinition(String name, Type type) {
		this.name = Preconditions.checkNotNull(name);
		this.type = Preconditions.checkNotNull(type);
	}

	public Type getType() {
		return type;
	}

	public String getName() {
		return name;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		FunctionDefinition that = (FunctionDefinition) o;
		return Objects.equals(name, that.name);
	}

	@Override
	public int hashCode() {
		return name.hashCode();
	}

	@Override
	public String toString() {
		return name;
	}
}
