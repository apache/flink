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
import org.apache.flink.table.descriptors.Bucket;
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;

/**
 * The validator for {@link Bucket}.
 */
@Internal
public class BucketValidator extends ConnectorDescriptorValidator {
	public static final String CONNECTOR_TYPE_VALUE_BUCKET = "bucket";
	public static final String CONNECTOR_BASEPATH = "connector.basepath";
	public static final String CONNECTOR_DATE_FORMAT = "connector.date.format";
	public static final String CONNECTOR_SINK_BUCKET_CLASS = "connector.bucket.class";
	public static final String CONNECTOR_SINK_WRITE_CLASS = "connector.write.class";

	public static final String CONNECTOR_DATA_TYPE = "connector.format.type";
	public static final String CONNECTOR_DATA_TYPE_ROW_VALUE = "row";
	public static final String CONNECTOR_DATA_TYPE_BULT_VALUE = "bult";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		properties.validateValue(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_BUCKET, false);
		properties.validateString(CONNECTOR_BASEPATH, false, 1, Integer.MAX_VALUE);
		properties.validateString(CONNECTOR_DATA_TYPE, false, 1, Integer.MAX_VALUE);
		properties.validateString(CONNECTOR_DATE_FORMAT, true, 1, Integer.MAX_VALUE);
//		validateDataType(properties);
	}

//	private void validateDataType(DescriptorProperties properties) {
//
//		final Map<String, Consumer<String>> validation = new HashMap<>();
//		validation.put(
//			getDataType(properties.getString(FormatDescriptorValidator.FORMAT_TYPE)),
//			noValidation());
//		properties.validateEnum(CONNECTOR_DATA_TYPE, false, validation);
//	}
//
//	private String getDataType(String formatType) {
//		switch (formatType) {
//			case JsonValidator.FORMAT_TYPE_VALUE:
//			case CsvValidator.FORMAT_TYPE_VALUE:
//				return CONNECTOR_DATA_TYPE_ROW_VALUE;
//			case AvroValidator.FORMAT_TYPE_VALUE:
//			case ParquetValidator.FORMAT_TYPE_VALUE:
//				return CONNECTOR_DATA_TYPE_BULT_VALUE;
//		}
//		throw new IllegalArgumentException("Invalid formatType.");
//	}

}
