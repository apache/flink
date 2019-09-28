/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators.windowing;

/**
 * A non-Pojo type used in {@link WindowOperatorMigrationTest} to test
 * migration behaviours for window operators that have a Kryo-serialized key.
 *
 * <p>NOTE: modifying this class would cause tests in {@link WindowOperatorMigrationTest} to fail,
 * since it is serialized in older version test savepoints.
 */
public class NonPojoType implements Comparable<NonPojoType> {

	private final String data;

	NonPojoType(String data) {
		this.data = data;
	}

	public String getData() {
		return data;
	}

	@Override
	public int compareTo(NonPojoType o) {
		return data.compareTo(o.data);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}

		if (obj == null) {
			return false;
		}

		if (!(obj instanceof NonPojoType)) {
			return false;
		}

		NonPojoType other = (NonPojoType) obj;

		return data.equals(other.data);
	}

	@Override
	public int hashCode() {
		return data.hashCode();
	}
}
