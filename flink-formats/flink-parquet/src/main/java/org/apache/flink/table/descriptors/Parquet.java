/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.descriptors;

import org.apache.flink.util.Preconditions;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;

import java.util.Map;

/**
 * Format descriptor for Parquet.
 */
public class Parquet extends FormatDescriptor {

	private Schema schema;
	private Class<? extends SpecificRecordBase> specificClass;
	private Class reflectClass;

	/**
	 * Format descriptor for  Parquet.
	 */
	public Parquet() {
		super(ParquetValidator.FORMAT_TYPE_VALUE, 1);
	}

	public Parquet specificClass(Class<? extends SpecificRecordBase> specificClass) {
		Preconditions.checkNotNull(specificClass);
		this.specificClass = specificClass;
		return this;
	}

	public Parquet schema(Schema schema) {
		Preconditions.checkNotNull(schema);
		this.schema = schema;
		return this;
	}

	public Parquet reflectClass(Class reflectClass) {
		Preconditions.checkNotNull(reflectClass);
		this.reflectClass = reflectClass;
		return this;
	}

	@Override
	protected Map<String, String> toFormatProperties() {
		final DescriptorProperties properties = new DescriptorProperties();

		if (null != reflectClass) {
			properties.putClass(ParquetValidator.FORMAT_REFLECT_CLASS, reflectClass);
		}
		if (null != specificClass) {
			properties.putClass(ParquetValidator.FORMAT_SPECIFIC_CLASS, specificClass);
		}
		if (null != schema) {
			properties.putString(ParquetValidator.FORMAT_PARQUET_SCHEMA, schema.toString());
		}


		return properties.asMap();
	}
}
