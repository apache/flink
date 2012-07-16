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
import eu.stratosphere.pact.common.type.PactRecord;

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
public class InputFileIterator implements Iterator<PactRecord>, Closeable {
	private final List<FileInputFormat> inputFormats;

	private final Iterator<FileInputFormat> formatIterator;

	private FileInputFormat currentFormat;

	private PactRecord[] buffer = new PactRecord[] { new PactRecord(), new PactRecord() };

	private int bufferIndex;

	private PactRecord nextRecord;

	private static PactRecord NO_MORE_PAIR = new PactRecord();

	/**
	 * Initializes InputFileIterator from already configured and opened {@link InputFormat}s.
	 * 
	 * @param reusePair
	 *        true if the pair needs only to be created once and is refilled for each subsequent {@link #next()}
	 * @param inputFormats
	 *        the inputFormats to wrap
	 */
	public InputFileIterator(final FileInputFormat... inputFormats) {
		this.inputFormats = Arrays.asList(inputFormats);
		this.formatIterator = this.inputFormats.iterator();
		this.currentFormat = this.formatIterator.next();

		this.loadNextPair();
	}

	@Override
	public boolean hasNext() {
		return this.nextRecord != NO_MORE_PAIR;
	}

	private void loadNextPair() {
		try {
			do {
				while (this.currentFormat != null && this.currentFormat.reachedEnd())
					if (this.formatIterator.hasNext())
						this.currentFormat = this.formatIterator.next();
					else {
						this.nextRecord = NO_MORE_PAIR;
						return;
					}

				this.nextRecord = this.buffer[this.bufferIndex++ % 2];
			} while (!this.currentFormat.nextRecord(this.nextRecord));

		} catch (final IOException e) {
			this.nextRecord = NO_MORE_PAIR;
			Assert.fail("reading expected values " + StringUtils.stringifyException(e));
		}
	}

	@Override
	public PactRecord next() {
		if (!this.hasNext())
			throw new NoSuchElementException();
		final PactRecord pair = this.nextRecord;
		this.loadNextPair();
		return pair;
	}

	@Override
	public void close() throws IOException {
		for (final FileInputFormat inputFormat : this.inputFormats)
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