/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.testing;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import eu.stratosphere.pact.common.io.InputFormat;
import eu.stratosphere.pact.common.stub.Stub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;

/**
 * Provides an {@link Iterator} for {@link InputFormat}s. If multiple formats are specified, it is assumed that they are
 * homogeneous and most likely the result of a parallel execution of the previous {@link Stub}.
 * 
 * @author Arvid Heise
 * @param <K>
 *        the type of the keys
 * @param <V>
 *        the type of the values
 */
public class InputFileIterator<K extends Key, V extends Value> implements Iterator<KeyValuePair<K, V>>, Closeable {
	private final List<InputFormat<K, V>> inputFormats;

	private final Iterator<InputFormat<K, V>> formatIterator;

	private InputFormat<K, V> currentFormat;

	private KeyValuePair<K, V> pair;

	private final boolean reusePair;

	/**
	 * Initializes InputFileIterator from already configured and opened {@link InputFormat}s.
	 * 
	 * @param reusePair
	 *        true if the pair needs only to be created once and is refilled for each subsequent {@link #next()}
	 * @param inputFormats
	 *        the inputFormats to wrap
	 */
	public InputFileIterator(final boolean reusePair, final InputFormat<K, V>... inputFormats) {
		this.inputFormats = Arrays.asList(inputFormats);
		this.formatIterator = this.inputFormats.iterator();
		this.currentFormat = inputFormats[0];

		this.reusePair = reusePair;
		if (reusePair)
			this.pair = this.currentFormat.createPair();
	}

	@Override
	public boolean hasNext() {
		return this.currentFormat() != null;
	}

	/**
	 * @return the current format which can return a pair
	 */
	private InputFormat<K, V> currentFormat() {
		InputFormat<K, V> format = this.currentFormat;
		try {
			while (format != null && format.reachedEnd())
				if (this.formatIterator.hasNext())
					format = this.currentFormat = this.formatIterator.next();
				else
					format = this.currentFormat = null;
		} catch (final IOException e) {
			throw new IllegalStateException("cannot verify end of format " + format, e);
		}
		return format;
	}

	@Override
	public KeyValuePair<K, V> next() {
		try {
			if (!this.hasNext())
				throw new NoSuchElementException();
			final InputFormat<K, V> currentFormat = this.currentFormat();
			if (!this.reusePair)
				this.pair = currentFormat.createPair();
			currentFormat.nextPair(this.pair);
			return this.pair;
		} catch (final IOException e) {
			TestPlan.fail(e, "reading expected values");
			return null;
		}
	}

	@Override
	public void close() throws IOException {
		for (final InputFormat<K, V> inputFormat : this.inputFormats)
			inputFormat.close();
	}

	/**
	 * Not supported.
	 */
	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
}