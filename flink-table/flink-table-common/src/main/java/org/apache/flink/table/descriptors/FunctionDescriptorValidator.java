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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Validator for {@link FunctionDescriptor}.
 */
@Internal
public class FunctionDescriptorValidator implements DescriptorValidator {

	public static final String FROM = "from";
	public static final String FROM_VALUE_CLASS = "class";

	@Override
	public void validate(DescriptorProperties properties) {
		Map<String, Consumer<String>> enumValidation = new HashMap<>();
		enumValidation.put(FROM_VALUE_CLASS, s -> new ClassInstanceValidator().validate(properties));

		// check for 'from'
		if (properties.containsKey(FROM)) {
			properties.validateEnum(FROM, false, enumValidation);
		} else {
			throw new ValidationException("Could not find 'from' property for function.");
		}
	}
}
