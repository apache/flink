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

package org.apache.flink.api.java.typeutils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * A special type information signifying that the type extraction failed. It contains
 * additional error information.
 */
public class MissingTypeInfo extends TypeInformation<InvalidTypesException> {

	private static final long serialVersionUID = -4212082837126702723L;
	
	private String functionName;
	private InvalidTypesException typeException;

	
	public MissingTypeInfo(String functionName) {
		this(functionName, new InvalidTypesException("An unknown error occured."));
	}

	public MissingTypeInfo(String functionName, InvalidTypesException typeException) {
		this.functionName = functionName;
		this.typeException = typeException;
	}
	
	// --------------------------------------------------------------------------------------------
	
	public String getFunctionName() {
		return functionName;
	}

	public InvalidTypesException getTypeException() {
		return typeException;
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public boolean isBasicType() {
		throw new UnsupportedOperationException("The missing type information cannot be used as a type information.");
	}

	@Override
	public boolean isTupleType() {
		throw new UnsupportedOperationException("The missing type information cannot be used as a type information.");
	}

	@Override
	public int getArity() {
		throw new UnsupportedOperationException("The missing type information cannot be used as a type information.");
	}

	@Override
	public Class<InvalidTypesException> getTypeClass() {
		throw new UnsupportedOperationException("The missing type information cannot be used as a type information.");
	}

	@Override
	public boolean isKeyType() {
		throw new UnsupportedOperationException("The missing type information cannot be used as a type information.");
	}

	@Override
	public TypeSerializer<InvalidTypesException> createSerializer(ExecutionConfig executionConfig) {
		throw new UnsupportedOperationException("The missing type information cannot be used as a type information.");
	}

	@Override
	public int getTotalFields() {
		throw new UnsupportedOperationException("The missing type information cannot be used as a type information.");
	}
}
