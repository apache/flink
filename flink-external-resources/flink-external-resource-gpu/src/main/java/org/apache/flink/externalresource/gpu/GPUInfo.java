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

package org.apache.flink.externalresource.gpu;

import org.apache.flink.api.common.externalresource.ExternalResourceInfo;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

/**
 * Information for GPU resource. Currently only including the GPU index.
 */
public class GPUInfo implements ExternalResourceInfo {

	private static final String PROPERTY_KEY_INDEX = "index";

	private final String index;

	GPUInfo(String index) {
		Preconditions.checkArgument(!StringUtils.isNullOrWhitespaceOnly(index));
		this.index = index;
	}

	@Override
	public String toString() {
		return String.format("GPU Device(%s)", index);
	}

	@Override
	public int hashCode() {
		return index.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		} else if (obj instanceof GPUInfo) {
			final GPUInfo other = (GPUInfo) obj;
			return this.index.equals(other.index);
		}
		return false;
	}

	@Override
	public Optional<String> getProperty(String key) {
		if (key.equals(PROPERTY_KEY_INDEX)) {
			return Optional.of(index);
		} else {
			return Optional.empty();
		}
	}

	@Override
	public Collection<String> getKeys() {
		return Collections.singleton(PROPERTY_KEY_INDEX);
	}
}
