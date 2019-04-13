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

package org.apache.flink.table.type;

/**
 * Binary type, It differs from ArrayType(Byte):
 *
 * <p>1. Comparisons: Unsigned comparisons, not signed byte comparisons.
 * According to: https://docs.oracle.com/cd/E11882_01/timesten.112/e21642/types.htm#TTSQL148
 * The BINARY data type is a fixed-length binary value with a length of n bytes. In database,
 * byte value usually expresses a range of 0-255, so the comparison is unsigned comparisons.
 *
 * <p>2. Its elements cannot have null values.
 */
public class BinaryType implements AtomicType {

	private static final long serialVersionUID = 1L;

	public static final BinaryType INSTANCE = new BinaryType();

	private BinaryType() {}

	@Override
	public boolean equals(Object o) {
		return this == o || o != null && getClass() == o.getClass();
	}

	@Override
	public int hashCode() {
		return getClass().hashCode();
	}

	@Override
	public String toString() {
		return getClass().getSimpleName();
	}
}
