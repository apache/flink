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

package org.apache.flink.api.common.resources;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for resources one can specify.
 */
@Internal
public abstract class Resource implements Serializable {

	private static final long serialVersionUID = 1L;

	private final String name;

	private final double value;

	protected Resource(String name, double value) {
		this.name = checkNotNull(name);
		this.value = value;
	}

	public Resource merge(Resource other) {
		checkNotNull(other, "Cannot merge with null resources");
		checkArgument(getClass() == other.getClass(), "Merge with different resource type");
		checkArgument(name.equals(other.name), "Merge with different resource name");

		return create(value + other.value);
	}

	public Resource subtract(Resource other) {
		checkNotNull(other, "Cannot subtract null resources");
		checkArgument(getClass() == other.getClass(), "Minus with different resource type");
		checkArgument(name.equals(other.name), "Minus with different resource name");
		checkArgument(value >= other.value, "Try to subtract a larger resource from this one.");

		return create(value - other.value);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		} else if (o != null && getClass() == o.getClass()) {
			Resource other = (Resource) o;

			return name.equals(other.name) && value == other.value;
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		int result = name.hashCode();
		result = 31 * result + (int) value;
		return result;
	}

	public String getName() {
		return name;
	}

	public double getValue() {
		return value;
	}

	/**
	 * Create a new instance of the sub resource.
	 *
	 * @param value The value of the resource
	 * @return A new instance of the sub resource
	 */
	protected abstract Resource create(double value);
}
