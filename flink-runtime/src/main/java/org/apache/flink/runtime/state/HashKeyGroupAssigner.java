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
import org.apache.flink.runtime.util.MathUtils;

/**
 * Hash based key group assigner
 *
 * The assigner assigns each key to a key group using the hash value of the key
 *
 * @param <K> Type of the key
 */
public class HashKeyGroupAssigner<K> implements KeyGroupAssigner<K> {
	private static final long serialVersionUID = -6319826921798945448L;

	private final int numberKeyGroups;

	public HashKeyGroupAssigner(int numberKeyGroups) {
		this.numberKeyGroups = numberKeyGroups;
	}

	@Override
	public int getKeyGroupID(K key) {
		if (key == null) {
			return 0;
		} else {
			if (numberKeyGroups > 0) {
				return MathUtils.murmurHash(key.hashCode()) % numberKeyGroups;
			} else {
				return MathUtils.murmurHash(key.hashCode());
			}
		}
	}
}
