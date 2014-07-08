/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.runtime.io.api;

import java.io.IOException;

import eu.stratosphere.core.io.IOReadableWritable;

public final class UnionRecordReader<T extends IOReadableWritable> extends AbstractUnionRecordReader<T> implements Reader<T> {
	
	private final Class<T> recordType;
	
	private T lookahead;
	

	public UnionRecordReader(MutableRecordReader<T>[] recordReaders, Class<T> recordType) {
		super(recordReaders);
		this.recordType = recordType;
	}

	@Override
	public boolean hasNext() throws IOException, InterruptedException {
		if (this.lookahead != null) {
			return true;
		} else {
			T record = instantiateRecordType();
			if (getNextRecord(record)) {
				this.lookahead = record;
				return true;
			} else {
				return false;
			}
		}
	}

	@Override
	public T next() throws IOException, InterruptedException {
		if (hasNext()) {
			T tmp = this.lookahead;
			this.lookahead = null;
			return tmp;
		} else {
			return null;
		}
	}
	
	private T instantiateRecordType() {
		try {
			return this.recordType.newInstance();
		} catch (InstantiationException e) {
			throw new RuntimeException("Cannot instantiate class '" + this.recordType.getName() + "'.", e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException("Cannot instantiate class '" + this.recordType.getName() + "'.", e);
		}
	}
}
