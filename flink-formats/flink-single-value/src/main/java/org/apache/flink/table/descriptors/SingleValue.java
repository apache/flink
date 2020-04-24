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

import java.util.Map;

import static org.apache.flink.table.descriptors.SingleValueValidator.FORMAT_TYPE_VALUE;

/**
 * Format descriptor for SINGLE-VALUE.
 */
public class SingleValue extends FormatDescriptor {

	/**
	 * Format descriptor for SINGLE-VALUE.
	 */
	public SingleValue() {
		super(FORMAT_TYPE_VALUE, 1);
	}

	@Override
	protected Map<String, String> toFormatProperties() {
		final DescriptorProperties properties = new DescriptorProperties();

		return properties.asMap();
	}
}
