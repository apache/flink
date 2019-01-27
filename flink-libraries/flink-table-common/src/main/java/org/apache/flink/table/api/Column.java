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

package org.apache.flink.table.api;

import org.apache.flink.table.api.types.InternalType;

/**
 * A column that represents a table's column with field name, type and nullability.
 */
public class Column {
	private final String name;
	private final InternalType type;
	private final boolean isNullable;

	public Column(String name, InternalType type, boolean isNullable) {
		this.name = name;
		this.type = type;
		this.isNullable = isNullable;
	}

	public Column(String name, InternalType type) {
		this(name, type, true);
	}

	public String name() {
		return this.name;
	}

	public InternalType internalType() {
		return this.type;
	}

	public boolean isNullable() {
		return this.isNullable;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		Column other = (Column) o;
		return name.equals(other.name) &&
			type.equals(other.internalType()) &&
			isNullable == other.isNullable();
	}
}
