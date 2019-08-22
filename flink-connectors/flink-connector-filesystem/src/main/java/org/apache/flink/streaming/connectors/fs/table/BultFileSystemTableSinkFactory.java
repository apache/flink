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

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.streaming.connectors.fs.table.descriptors.BucketValidator.CONNECTOR_BASEPATH;
import static org.apache.flink.streaming.connectors.fs.table.descriptors.BucketValidator.CONNECTOR_DATE_FORMAT;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT;

/**
 * Factory for creating configured instances of BultFileSystemTableSink .
 */
public abstract class BultFileSystemTableSinkFactory extends FileSystemTableSinkFactoryBase {

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();

		properties.add(BucketValidator.CONNECTOR_DATA_TYPE);
		properties.add(CONNECTOR_BASEPATH);
		properties.add(CONNECTOR_TYPE);
		properties.add(CONNECTOR_DATE_FORMAT);

		properties.add(FORMAT + ".*");

		return properties;
	}

	@Override
	protected String formatType() {
		return BucketValidator.CONNECTOR_DATA_TYPE_BULT_VALUE;
	}
}
