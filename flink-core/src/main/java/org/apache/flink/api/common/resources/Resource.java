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
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for resources one can specify.
 */
@Internal
public abstract class Resource implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * Enum defining how resources are aggregated.
	 */
	public enum ResourceAggregateType {
		/**
		 * Denotes keeping the sum of the values with same name when merging two resource specs for operator chaining.
		 */
		AGGREGATE_TYPE_SUM,

		/**
		 * Denotes keeping the max of the values with same name when merging two resource specs for operator chaining.
		 */
		AGGREGATE_TYPE_MAX
	}

	private final String name;

	private final double value;

	private final ResourceAggregateType resourceAggregateType;

	protected Resource(String name, double value, ResourceAggregateType type) {
		this.name = checkNotNull(name);
		this.value = value;
		this.resourceAggregateType = checkNotNull(type);
	}

	public Resource merge(Resource other) {
		Preconditions.checkArgument(getClass() == other.getClass(), "Merge with different resource resourceAggregateType");
		Preconditions.checkArgument(this.name.equals(other.name), "Merge with different resource name");
		Preconditions.checkArgument(this.resourceAggregateType == other.resourceAggregateType, "Merge with different aggregate resourceAggregateType");

		final double aggregatedValue;
		switch (resourceAggregateType) {
			case AGGREGATE_TYPE_MAX :
				aggregatedValue = Math.max(other.value, this.value);
				break;

			case AGGREGATE_TYPE_SUM:
			default:
				aggregatedValue = this.value + other.value;
		}

		return create(aggregatedValue, resourceAggregateType);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		} else if (o != null && getClass() == o.getClass()) {
			Resource other = (Resource) o;

			return name.equals(other.name) && resourceAggregateType == other.resourceAggregateType && value == other.value;
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		int result = name.hashCode();
		result = 31 * result + resourceAggregateType.ordinal();
		result = 31 * result + (int) value;
		return result;
	}

	public String getName() {
		return name;
	}

	public ResourceAggregateType getResourceAggregateType() {
		return this.resourceAggregateType;
	}

	public double getValue() {
		return this.value;
	}

	/**
	 * Create a resource of the same resource resourceAggregateType.
	 *
	 * @param value The value of the resource
	 * @param type The aggregate resourceAggregateType of the resource
	 * @return A new instance of the sub resource
	 */
	protected abstract Resource create(double value, ResourceAggregateType type);
}
