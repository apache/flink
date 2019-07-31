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

/**
 * Validator for a {@link HierarchyDescriptor}.
 */
@Internal
public abstract class HierarchyDescriptorValidator implements DescriptorValidator {

	public static final String EMPTY_PREFIX = "";

	private String keyPrefix;

	/**
	 * @param keyPrefix prefix to be added to every property before validation
	 */
	public HierarchyDescriptorValidator(String keyPrefix) {
		this.keyPrefix = keyPrefix;
	}

	@Override
	public void validate(DescriptorProperties properties) {
		validateWithPrefix(keyPrefix, properties);
	}

	/**
	 * Performs validation with a prefix for the keys.
	 */
	protected abstract void validateWithPrefix(String keyPrefix, DescriptorProperties properties);
}
