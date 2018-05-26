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

import org.apache.avro.specific.SpecificRecordBase;

/**
 * Format descriptor for Apache Avro records.
 */
public class Avro extends FormatDescriptor {

	private Class<? extends SpecificRecordBase> recordClass;

	/**
	 * Format descriptor for Apache Avro records.
	 */
	public Avro() {
		super(AvroValidator.FORMAT_TYPE_VALUE, 1);
	}

	/**
	 * Sets the class of the Avro specific record. Required.
	 *
	 * @param recordClass class of the Avro record.
	 */
	public Avro recordClass(Class<? extends SpecificRecordBase> recordClass) {
		Preconditions.checkNotNull(recordClass);
		this.recordClass = recordClass;
		return this;
	}

	/**
	 * Internal method for format properties conversion.
	 */
	@Override
	public void addFormatProperties(DescriptorProperties properties) {
		if (null != recordClass) {
			properties.putClass(AvroValidator.FORMAT_RECORD_CLASS, recordClass);
		}
	}
}
