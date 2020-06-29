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

package org.apache.flink.connector.file.src.util;

import org.apache.flink.connector.file.src.reader.BulkFormat;

import javax.annotation.Nullable;

/**
 * A simple {@link BulkFormat.RecordIterator} that returns the elements of an array, one after the other.
 * The iterator is mutable to support object reuse and supports recycling.
 *
 * @param <E> The type of the record returned by the iterator.
 */
public final class ArrayResultIterator<E> implements BulkFormat.RecordIterator<E> {

	private E[] records;

	private int num;
	private int pos;

	private final MutableRecordAndPosition<E> recordAndPosition;

	@Nullable
	private final Runnable recycler;

	public ArrayResultIterator() {
		this(null);
	}

	public ArrayResultIterator(@Nullable Runnable recycler) {
		this.recycler = recycler;
		this.recordAndPosition = new MutableRecordAndPosition<>();
	}

	// -------------------------------------------------------------------------
	//  Setting
	// -------------------------------------------------------------------------

	public void set(final E[] records, final int num, final long offsetOfFirst, final long skipCountOfFirst) {
		this.records = records;
		this.num = num;
		this.pos = 0;
		this.recordAndPosition.set(null, offsetOfFirst, skipCountOfFirst);
	}

	// -------------------------------------------------------------------------
	//  Result Iterator Methods
	// -------------------------------------------------------------------------

	@Nullable
	@Override
	public RecordAndPosition<E> next() {
		if (pos < num) {
			recordAndPosition.setNext(records[pos++]);
			return recordAndPosition;
		}
		else {
			return null;
		}
	}

	@Override
	public void close() {
		if (recycler != null) {
			recycler.run();
		}
	}
}
