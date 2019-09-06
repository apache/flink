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

import java.util.Map;

import static org.apache.flink.table.descriptors.BucketValidator.CONNECTOR_BASEPATH;
import static org.apache.flink.table.descriptors.BucketValidator.CONNECTOR_DATA_TYPE;
import static org.apache.flink.table.descriptors.BucketValidator.CONNECTOR_DATE_FORMAT;
import static org.apache.flink.table.descriptors.BucketValidator.CONNECTOR_TYPE_VALUE_BUCKET;


/**
 * Connector descriptor for the Bucket File System .
 */
public class Bucket extends ConnectorDescriptor {

	private String basePath;
	private String dateFormat;
	private FormatType formatType;

	public Bucket() {
		super(CONNECTOR_TYPE_VALUE_BUCKET, 1, true);
	}

	public Bucket dateFormat(String dateFormat) {
		this.dateFormat = dateFormat;
		return this;
	}

	public Bucket rowFormat() {
		this.formatType = FormatType.ROW;
		return this;
	}

	public Bucket bultFormat() {
		this.formatType = FormatType.BULT;
		return this;
	}

	public Bucket basePath(String basePath) {
		Preconditions.checkNotNull(basePath);
		this.basePath = basePath;
		return this;
	}

	@Override
	protected Map<String, String> toConnectorProperties() {
		final DescriptorProperties properties = new DescriptorProperties();
		if (basePath != null) {
			properties.putString(CONNECTOR_BASEPATH, basePath);
		}

		if (formatType != null) {
			properties.putString(CONNECTOR_DATA_TYPE, formatType.getType());
		}

		if (dateFormat != null) {
			properties.putString(CONNECTOR_DATE_FORMAT, dateFormat);
		}
		return properties.asMap();
	}
}
