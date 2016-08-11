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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.state.KeyGroupAssigner;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.Preconditions;

/**
 * Hash based key group assigner. The assigner assigns each key to a key group using the hash value
 * of the key.
 *
 * @param <K> Type of the key
 */
public class HashKeyGroupAssigner<K> implements KeyGroupAssigner<K> {
	private static final long serialVersionUID = -6319826921798945448L;

	private static final int UNDEFINED_NUMBER_KEY_GROUPS = Integer.MIN_VALUE;

	private int numberKeyGroups;

	public HashKeyGroupAssigner() {
		this(UNDEFINED_NUMBER_KEY_GROUPS);
	}

	public HashKeyGroupAssigner(int numberKeyGroups) {
		Preconditions.checkArgument(numberKeyGroups > 0 || numberKeyGroups == UNDEFINED_NUMBER_KEY_GROUPS,
			"The number of key groups has to be greater than 0 or undefined. Use " +
			"setMaxParallelism() to specify the number of key groups.");
		this.numberKeyGroups = numberKeyGroups;
	}

	public int getNumberKeyGroups() {
		return numberKeyGroups;
	}

	@Override
	public int getKeyGroupIndex(K key) {
		return MathUtils.murmurHash(key.hashCode()) % numberKeyGroups;
	}

	@Override
	public void setup(int numberOfKeygroups) {
		Preconditions.checkArgument(numberOfKeygroups > 0, "The number of key groups has to be " +
			"greater than 0. Use setMaxParallelism() to specify the number of key " +
			"groups.");

		this.numberKeyGroups = numberOfKeygroups;
	}
}
