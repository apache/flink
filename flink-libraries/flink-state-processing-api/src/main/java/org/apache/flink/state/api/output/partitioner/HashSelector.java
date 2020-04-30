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

package org.apache.flink.state.api.output.partitioner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.util.Preconditions;

/**
 * A wrapper around a {@link KeySelector} that returns the {@link Object#hashCode()} of the returned
 * key.
 *
 * @param <IN> Type of objects to extract the key from.
 */
@Internal
public class HashSelector<IN> implements KeySelector<IN, Integer> {

	private static final long serialVersionUID = 1L;

	private final KeySelector<IN, ?> keySelector;

	public HashSelector(KeySelector<IN, ?> keySelector) {
		Preconditions.checkNotNull(keySelector);
		this.keySelector = keySelector;
	}

	@Override
	public Integer getKey(IN value) throws Exception {
		return keySelector.getKey(value).hashCode();
	}
}

