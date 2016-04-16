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

package org.apache.flink.graph.translate;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.types.LongValue;

/**
 * Translate {@link LongValue} by adding a constant offset value.
 */
public class LongValueAddOffset
implements Translator<LongValue,LongValue> {

	private final long offset;

	/**
	 * Translate {@link LongValue} by adding a constant offset value.
	 *
	 * @param offset value to be added to each element
	 */
	public LongValueAddOffset(long offset) {
		this.offset = offset;
	}

	@Override
	public LongValue translate(LongValue value, LongValue reuse) {
		value.setValue(offset + value.getValue());

		return value;
	}

	@Override
	public TypeHint<LongValue> getTypeHint() {
			return new TypeHint<LongValue>(){};
		}
}
