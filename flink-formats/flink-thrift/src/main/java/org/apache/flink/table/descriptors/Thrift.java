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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.utils.TypeStringUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Map;

/**
 * Format descriptor for Thrift.
 */
public class Thrift extends FormatDescriptor {

	private String thriftClass;

	/**
	 * Format descriptor for Thrift.
	 */
	public Thrift() {
		super(ThriftValidator.FORMAT_TYPE_VALUE, 1);
	}

	/**
	 * Sets the Thrift class name.
	 *
	 * @param thriftClass Thrift schema
	 */
	public Thrift thriftSchema(String thriftClass) {
		Preconditions.checkNotNull(thriftClass);
		this.thriftClass = thriftClass;
		return this;
	}

	/**
	 * Sets the schema using type information.
	 *
	 * <p>JSON objects are represented as ROW types.
	 *
	 * <p>The schema might be nested.
	 *
	 * @param schemaType type information that describes the schema
	 */
	public Thrift schema(TypeInformation<Row> schemaType) {
		Preconditions.checkNotNull(schemaType);
		this.thriftClass = TypeStringUtils.writeTypeInfo(schemaType);
		return this;
	}

	@Override
	protected Map<String, String> toFormatProperties() {
		final DescriptorProperties properties = new DescriptorProperties();
		if (thriftClass != null) {
			properties.putString(ThriftValidator.FORMAT_THRIFT_CLASS, thriftClass);
		}
		return properties.asMap();
	}
}
