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

import org.apache.flink.annotation.PublicEvolving;

import java.util.Map;

/**
 * Describes the format of data.
 */
@PublicEvolving
public abstract class FormatDescriptor extends DescriptorBase implements Descriptor {

	private String type;

	private int version;

	/**
	 * Constructs a {@link FormatDescriptor}.
	 *
	 * @param type string that identifies this format
	 * @param version property version for backwards compatibility
	 */
	public FormatDescriptor(String type, int version) {
		this.type = type;
		this.version = version;
	}

	@Override
	public final Map<String, String> toProperties() {
		final DescriptorProperties properties = new DescriptorProperties();
		properties.putString(FormatDescriptorValidator.FORMAT_TYPE, type);
		properties.putInt(FormatDescriptorValidator.FORMAT_PROPERTY_VERSION, version);
		properties.putProperties(toFormatProperties());
		return properties.asMap();
	}

	/**
	 * Converts this descriptor into a set of format properties. Usually prefixed with
	 * {@link FormatDescriptorValidator#FORMAT}.
	 */
	protected abstract Map<String, String> toFormatProperties();
}
