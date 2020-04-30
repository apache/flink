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

package org.apache.flink.table.utils;

import org.apache.flink.table.descriptors.FormatDescriptor;

import java.util.HashMap;
import java.util.Map;

/**
 * Mocking {@link FormatDescriptor} for tests.
 */
public class FormatDescriptorMock extends FormatDescriptor {

	private Map<String, String> formatProperties = new HashMap<>();

	public FormatDescriptorMock(String type, int version) {
		super(type, version);
	}

	public FormatDescriptorMock property(String key, String value) {
		formatProperties.put(key, value);
		return this;
	}

	@Override
	protected Map<String, String> toFormatProperties() {
		return formatProperties;
	}
}
