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

package org.apache.flink.runtime.persistence;

import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.Objects;

/**
 * {@link ResourceVersion} implementation with {@link String} value. The resource version in Kubernetes
 * is {@link String}. And they have same length, we could easily compare string.
 */
public class StringResourceVersion implements ResourceVersion<StringResourceVersion> {

	private static final long serialVersionUID = 1L;

	private static final StringResourceVersion NOT_EXISTING = new StringResourceVersion("-1");

	private final String value;

	private StringResourceVersion(String value) {
		this.value = value;
	}

	@Override
	public int compareTo(@Nonnull StringResourceVersion other) {
		return this.value.compareTo(other.value);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		} else if (obj != null && obj.getClass() == StringResourceVersion.class) {
			final StringResourceVersion that = (StringResourceVersion) obj;
			return Objects.equals(this.value, that.value);
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(value);
	}

	@Override
	public boolean isExisting() {
		return this != NOT_EXISTING;
	}

	@Override
	public String toString() {
		return "StringResourceVersion{" + "value='" + value + '\'' + '}';
	}

	public String getValue() {
		return this.value;
	}

	public static StringResourceVersion notExisting() {
		return NOT_EXISTING;
	}

	public static StringResourceVersion valueOf(String value) {
		Preconditions.checkArgument(!value.equals(NOT_EXISTING.getValue()));
		return new StringResourceVersion(value);
	}
}
