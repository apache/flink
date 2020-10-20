/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.util.collections;

/**
 * Byte hash set.
 */
public class ByteHashSet {

	protected boolean containsNull;

	protected boolean[] used;

	// Do not remove the param "dummy", it is needed to simplify
	// the code generation, see CodeGeneratorContext.addReusableHashSet.
	public ByteHashSet(final int dummy) {
		used = new boolean[Byte.MAX_VALUE - Byte.MIN_VALUE + 1];
	}

	public boolean add(final byte k) {
		return !used[k - Byte.MIN_VALUE] && (used[k - Byte.MIN_VALUE] = true);
	}

	public void addNull() {
		this.containsNull = true;
	}

	public boolean contains(final byte k) {
		return used[k - Byte.MIN_VALUE];
	}

	public boolean containsNull() {
		return containsNull;
	}

	public void optimize() {
	}
}
