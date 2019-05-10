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

/**
 * Validator for {@link Thrift}.
 */
@Internal
public class ThriftValidator extends FormatDescriptorValidator {

	public static final String FORMAT_TYPE_VALUE = "thrift";
	public static final String FORMAT_THRIFT_CLASS = "format.thrift-class";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		properties.validateBoolean(FORMAT_DERIVE_SCHEMA, true);
		final boolean hasSchema = properties.containsKey(FORMAT_THRIFT_CLASS);
		if (hasSchema) {
			properties.validateType(FORMAT_THRIFT_CLASS, false, true);
		}
	}
}
