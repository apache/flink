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

package org.apache.flink.hadoopcompatibility.mapred.wrapper;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.operators.translation.TupleUnwrappingIterator;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Iterator;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Wraps a Flink Tuple2 (key-value-pair) iterator into an iterator over the second (value) field.
 */
public class HadoopTupleUnwrappingIterator<KEY, VALUE>
		extends TupleUnwrappingIterator<VALUE, KEY> implements java.io.Serializable {

	private static final long serialVersionUID = 1L;

	private final TypeSerializer<KEY> keySerializer;

	private transient Iterator<Tuple2<KEY, VALUE>> iterator;

	private transient KEY curKey;
	private transient VALUE firstValue;
	private transient boolean atFirst;

	public HadoopTupleUnwrappingIterator(TypeSerializer<KEY> keySerializer) {
		this.keySerializer = checkNotNull(keySerializer);
	}

	/**
	 * Set the Flink iterator to wrap.
	 *
	 * @param iterator The Flink iterator to wrap.
	 */
	@Override
	public void set(final Iterator<Tuple2<KEY, VALUE>> iterator) {
		this.iterator = iterator;
		if (this.hasNext()) {
			final Tuple2<KEY, VALUE> tuple = iterator.next();
			this.curKey = keySerializer.copy(tuple.f0);
			this.firstValue = tuple.f1;
			this.atFirst = true;
		} else {
			this.atFirst = false;
		}
	}

	@Override
	public boolean hasNext() {
		if (this.atFirst) {
			return true;
		}
		return iterator.hasNext();
	}

	@Override
	public VALUE next() {
		if (this.atFirst) {
			this.atFirst = false;
			return firstValue;
		}

		final Tuple2<KEY, VALUE> tuple = iterator.next();
		return tuple.f1;
	}

	public KEY getCurrentKey() {
		return this.curKey;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
}
