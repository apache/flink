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

import org.apache.flink.util.Preconditions;

import static org.apache.flink.table.descriptors.StringValidator.FORMAT_ENCODING;
import static org.apache.flink.table.descriptors.StringValidator.FORMAT_FAIL_ON_EMPTY;
import static org.apache.flink.table.descriptors.StringValidator.FORMAT_FAIL_ON_NULL;
import static org.apache.flink.table.descriptors.StringValidator.FORMAT_SCHEMA;
import static org.apache.flink.table.descriptors.StringValidator.FORMAT_TYPE_VALUE;

/**
 * Format descriptor for String.
 */
public class StringFormat extends FormatDescriptor{

	private Boolean failOnNull;

	private Boolean failOnEmpty;

	private String schema;

	private String encoding;

	/**
	 * Format descriptor for String.
	 */
	public StringFormat() {
		super(FORMAT_TYPE_VALUE, 1);
	}

	/**
	 * Sets flag whether to fail if the string is null or not.
	 *
	 * @param failOnNull If set to true, the operation fails if the string is null.
	 */
	public StringFormat setFailOnNull(Boolean failOnNull) {
		this.failOnNull = failOnNull;
		return this;
	}

	/**
	 * Sets flag whether to fail if the string is empty.
	 *
	 * @param failOnEmpty If set to true, the operation fails if the string is empty.
	 */
	public StringFormat setFailOnEmpty(Boolean failOnEmpty) {
		this.failOnEmpty = failOnEmpty;
		return this;
	}

	public StringFormat setSchema(String schema) {
		Preconditions.checkNotNull(schema);
		this.schema = schema;
		return this;
	}

	/**
	 * Sets encoding of the string, "UTF-8" if not set.
	 *
	 * @param encoding encoding of the string.
	 */
	public StringFormat setEncoding(String encoding) {
		this.encoding = encoding;
		return this;
	}

	@Override
	public void addFormatProperties(DescriptorProperties properties) {
		if (failOnNull != null) {
			properties.putBoolean(FORMAT_FAIL_ON_NULL, failOnNull);
		}

		if (failOnEmpty != null) {
			properties.putBoolean(FORMAT_FAIL_ON_EMPTY, failOnEmpty);
		}

		if (schema != null) {
			properties.putString(FORMAT_SCHEMA, schema);
		}

		if (encoding != null) {
			properties.putString(FORMAT_ENCODING, encoding);
		}
	}

}
