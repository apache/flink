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

import junit.framework.Assert;
import eu.stratosphere.nephele.util.StringUtils;
import eu.stratosphere.pact.common.io.FileInputFormat;
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
	private final List<FileInputFormat<K, V>> inputFormats;

	private final Iterator<FileInputFormat<K, V>> formatIterator;

	private FileInputFormat<K, V> currentFormat;

	private KeyValuePair<K, V> nextPair;

	@SuppressWarnings("unchecked")
	private KeyValuePair<K, V>[] buffer = new KeyValuePair[2];

	private int bufferIndex;

	private final boolean reusePair;

	private static KeyValuePair<?, ?> NO_MORE_PAIR = new KeyValuePair<Key, Value>();

	/**
	 * Initializes InputFileIterator from already configured and opened {@link InputFormat}s.
	 * 
	 * @param reusePair
	 *        true if the pair needs only to be created once and is refilled for each subsequent {@link #next()}
	 * @param inputFormats
	 *        the inputFormats to wrap
	 */
	public InputFileIterator(final boolean reusePair, final FileInputFormat<K, V>... inputFormats) {
		this.inputFormats = Arrays.asList(inputFormats);
		this.formatIterator = this.inputFormats.iterator();
		this.currentFormat = formatIterator.next();

		this.reusePair = reusePair;
		if (reusePair) {
			this.buffer[0] = this.currentFormat.createPair();
			this.buffer[1] = this.currentFormat.createPair();
		}
		loadNextPair();
	}

	@Override
	public boolean hasNext() {
		return this.nextPair != NO_MORE_PAIR;
	}

	@SuppressWarnings("unchecked")
	private void loadNextPair() {
		try {
			do {
				while (this.currentFormat != null && this.currentFormat.reachedEnd())
					if (this.formatIterator.hasNext())
						this.currentFormat = this.formatIterator.next();
					else {
						nextPair = (KeyValuePair<K, V>) NO_MORE_PAIR;
						return;
					}

				if (!this.reusePair)
					nextPair = currentFormat.createPair();
				else
					nextPair = buffer[bufferIndex++ % 2];
			} while (!currentFormat.nextRecord(this.nextPair));

		} catch (final IOException e) {
			nextPair = (KeyValuePair<K, V>) NO_MORE_PAIR;
			Assert.fail("reading expected values " + StringUtils.stringifyException(e));
		}
	}

	@Override
	public KeyValuePair<K, V> next() {
		if (!this.hasNext())
			throw new NoSuchElementException();
		final KeyValuePair<K, V> pair = nextPair;
		loadNextPair();
		return pair;
	}

	@Override
	public void close() throws IOException {
		for (final FileInputFormat<K, V> inputFormat : this.inputFormats)
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