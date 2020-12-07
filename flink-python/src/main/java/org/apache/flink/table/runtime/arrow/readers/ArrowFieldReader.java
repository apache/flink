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

package org.apache.flink.table.runtime.arrow.readers;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import org.apache.arrow.vector.ValueVector;

/**
 * Base class for arrow field reader.
 *
 * @param <OUT> Type of the row to write.
 */
@Internal
public abstract class ArrowFieldReader<OUT> {

	/**
	 * Container which is used to store the sequence of values of a column to read.
	 */
	private final ValueVector valueVector;

	public ArrowFieldReader(ValueVector valueVector) {
		this.valueVector = Preconditions.checkNotNull(valueVector);
	}

	/**
	 * Returns the underlying container which stores the sequence of values of a column to read.
	 */
	public ValueVector getValueVector() {
		return valueVector;
	}

	/**
	 * Sets the field value as the specified value.
	 */
	public abstract OUT read(int index);
}
