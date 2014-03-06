/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.java.io;

import java.io.IOException;
import java.util.Iterator;

import eu.stratosphere.api.common.io.GenericInputFormat;
import eu.stratosphere.core.io.GenericInputSplit;
import eu.stratosphere.util.SplittableIterator;


/**
 * An input format that generates data in parallel through a {@link SplittableIterator}.
 */
public class ParallelIteratorInputFormat<T> extends GenericInputFormat<T> {

	private static final long serialVersionUID = 1L;
	
	
	private final SplittableIterator<T> source;
	
	private transient Iterator<T> splitIterator;
	
	
	
	public ParallelIteratorInputFormat(SplittableIterator<T> iterator) {
		this.source = iterator;
	}
	
	@Override
	public void open(GenericInputSplit split) throws IOException {
		super.open(split);
		
		this.splitIterator = this.source.getSplit(split.getSplitNumber(), split.getTotalNumberOfSplits());
	}
	
	@Override
	public boolean reachedEnd() {
		return !this.splitIterator.hasNext();
	}

	@Override
	public T nextRecord(T reuse) {
		return this.splitIterator.next();
	}
}
