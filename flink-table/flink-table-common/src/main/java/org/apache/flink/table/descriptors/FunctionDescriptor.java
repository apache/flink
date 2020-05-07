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

package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Map;

/**
 * Descriptor for describing a function.
 */
@PublicEvolving
public class FunctionDescriptor implements Descriptor {
	private String from;
	private ClassInstance classInstance;
	private String fullyQualifiedName;

	/**
	 * Creates a function from a class description.
	 */
	public FunctionDescriptor fromClass(ClassInstance classType) {
		from = FunctionDescriptorValidator.FROM_VALUE_CLASS;
		this.classInstance = classType;
		this.fullyQualifiedName = null;
		return this;
	}

	/**
	 * Converts this descriptor into a set of properties.
	 */
	@Override
	public Map<String, String> toProperties() {
		DescriptorProperties properties = new DescriptorProperties();
		if (from != null) {
			properties.putString(FunctionDescriptorValidator.FROM, from);
		}
		if (classInstance != null) {
			properties.putProperties(classInstance.toProperties());
		}
		if (fullyQualifiedName != null) {
			properties.putString(PythonFunctionValidator.FULLY_QUALIFIED_NAME, fullyQualifiedName);
		}
		return properties.asMap();
	}
}
