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

package org.apache.flink.table.runtime.hashtable;

import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.util.RowIterator;
import org.apache.flink.util.MutableObjectIterator;

import java.io.IOException;

/**
 * Wrap {@link MutableObjectIterator} to java {@link RowIterator}.
 */
public class WrappedRowIterator<T extends BaseRow> implements RowIterator<T> {

	private final MutableObjectIterator<T> iterator;
	private final T reuse;
	private T instance;

	public WrappedRowIterator(MutableObjectIterator<T> iterator, T reuse) {
		this.iterator = iterator;
		this.reuse = reuse;
	}

	@Override
	public boolean advanceNext() {
		try {
			this.instance = iterator.next(reuse);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return this.instance != null;
	}

	@Override
	public T getRow() {
		return instance;
	}
}
