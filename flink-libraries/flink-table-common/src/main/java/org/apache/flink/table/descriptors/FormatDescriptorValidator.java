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
 * Validator for {@link FormatDescriptor}.
 */
@Internal
public abstract class FormatDescriptorValidator implements DescriptorValidator {

	/**
	 * Prefix for format-related properties.
	 */
	public static final String FORMAT = "format";

	/**
	 * Key for describing the type of the format. Usually used for factory discovery.
	 */
	public static final String FORMAT_TYPE = "format.type";

	/**
	 *  Key for describing the property version. This property can be used for backwards
	 *  compatibility in case the property format changes.
	 */
	public static final String FORMAT_PROPERTY_VERSION = "format.property-version";

	/**
	 * Key for describing the version of the format. This property can be used for different
	 * format versions (e.g. Avro 1.8.2 or Avro 2.0).
	 */
	public static final String FORMAT_VERSION = "format.version";

	/**
	 * Key for deriving the schema of the format from the table's schema.
	 */
	public static final String FORMAT_DERIVE_SCHEMA = "format.derive-schema";

	@Override
	public void validate(DescriptorProperties properties) {
		properties.validateString(FORMAT_TYPE, false, 1);
		properties.validateString(FORMAT_PROPERTY_VERSION, true, 1);
	}
}
