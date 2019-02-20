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

package org.apache.flink.table.expressions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

/**
 * The base class of function definition which can directly define built-in functions
 * with a name attribute.
 */
@PublicEvolving
public class FunctionDefinition {
	private final String name;
	private final FunctionType functionType;

	public FunctionDefinition(String name, FunctionType functionType) {
		this.name = Preconditions.checkNotNull(name);
		this.functionType = Preconditions.checkNotNull(functionType);
	}

	public String getName() {
		return name;
	}

	public FunctionType getFunctionType() {
		return functionType;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof FunctionDefinition)) {
			return false;
		}

		FunctionDefinition other = (FunctionDefinition) obj;
		return this == other || (other.getName().equalsIgnoreCase(name) && other.getFunctionType() == functionType);
	}

	@Override
	public int hashCode() {
		return name.hashCode() + 31 * functionType.hashCode();
	}

	@Override
	public String toString() {
		return name;
	}
}
