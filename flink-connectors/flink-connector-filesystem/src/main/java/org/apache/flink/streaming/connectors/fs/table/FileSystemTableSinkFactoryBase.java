/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.fs.table;

import org.apache.flink.streaming.connectors.fs.table.descriptors.BucketValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.streaming.connectors.fs.table.descriptors.BucketValidator.CONNECTOR_TYPE_VALUE_BUCKET;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE_VALUE_APPEND;

/**
 * a base Factory for creating FileSystemTableSink .
 */
public abstract class FileSystemTableSinkFactoryBase implements StreamTableSinkFactory<Row> {

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(UPDATE_MODE, UPDATE_MODE_VALUE_APPEND); // append mode
		context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_BUCKET); // filesystem
		context.put(BucketValidator.CONNECTOR_DATA_TYPE, formatType());
		context.put(CONNECTOR_PROPERTY_VERSION, "1");
		return context;
	}

	protected abstract String formatType();

	protected DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);
		new BucketValidator().validate(descriptorProperties);
		return descriptorProperties;
	}

}
