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

package eu.stratosphere.nephele.template;

import java.util.Iterator;

import eu.stratosphere.core.io.InputSplit;

/**
 * The input split iterator allows an {@link AbstractInputTask} to iterator over all input splits it is supposed to
 * consume. Internally, the input split iterator calls an {@link InputSplitProvider} on each <code>next</code> call in
 * order to facilitate lazy split assignment.
 * 
 * @param <T>
 */
public class InputSplitIterator<T extends InputSplit> implements Iterator<T> {

	/**
	 * The {@link InputSplitProvider} that is called to provide new input splits.
	 */
	private final InputSplitProvider inputSplitProvider;

	/**
	 * Buffers the next input split to be returned by this iterator or <code>null</code> it no split is buffered.
	 */
	private T nextInputSplit = null;

	/**
	 * Constructs a new input split iterator.
	 * 
	 * @param inputSplitProvider
	 *        the input split provider to be called for new input splits
	 */
	public InputSplitIterator(final InputSplitProvider inputSplitProvider) {
		this.inputSplitProvider = inputSplitProvider;
	}


	@SuppressWarnings("unchecked")
	@Override
	public boolean hasNext() {

		if (this.nextInputSplit == null) {
			this.nextInputSplit = (T) inputSplitProvider.getNextInputSplit();
		}

		if (this.nextInputSplit == null) {
			return false;
		}

		return true;
	}


	@SuppressWarnings("unchecked")
	@Override
	public T next() {

		T retVal = null;

		if (this.nextInputSplit == null) {
			this.nextInputSplit = (T) inputSplitProvider.getNextInputSplit();
		}

		retVal = this.nextInputSplit;
		this.nextInputSplit = null;

		return retVal;
	}


	@Override
	public void remove() {

		throw new RuntimeException("The InputSplitIterator does not implement the remove method");
	}

}
