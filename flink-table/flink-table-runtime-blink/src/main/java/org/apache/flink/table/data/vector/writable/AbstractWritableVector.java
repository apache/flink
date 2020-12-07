/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.data.vector.writable;

import org.apache.flink.table.data.vector.ColumnVector;
import org.apache.flink.table.data.vector.Dictionary;

import java.io.Serializable;

/**
 * Contains the shared structure for {@link ColumnVector}s, including NULL information and dictionary.
 * NOTE: if there are some nulls, must set {@link #noNulls} to false.
 */
public abstract class AbstractWritableVector implements WritableColumnVector, Serializable {

	private static final long serialVersionUID = 1L;

	// If the whole column vector has no nulls, this is true, otherwise false.
	protected boolean noNulls = true;

	/**
	 * The Dictionary for this column.
	 * If it's not null, will be used to decode the value in get().
	 */
	protected Dictionary dictionary;

	/**
	 * Update the dictionary.
	 */
	@Override
	public void setDictionary(Dictionary dictionary) {
		this.dictionary = dictionary;
	}

	/**
	 * Returns true if this column has a dictionary.
	 */
	@Override
	public boolean hasDictionary() {
		return this.dictionary != null;
	}
}
