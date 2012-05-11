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

package eu.stratosphere.pact.runtime.test.util;

import java.io.File;

import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.template.InputSplit;
import eu.stratosphere.nephele.template.InputSplitProvider;

/**
 * The mock input split provider implements the {@link InputSplitProvider} interface to serve input splits to the test
 * jobs.
 * 
 * @author warneke
 */
public class MockInputSplitProvider implements InputSplitProvider {

	/**
	 * The input splits to be served during the test.
	 */
	private final InputSplit[] inputSplits;

	/**
	 * Index pointing to the next input split to be served.
	 */
	private int nextSplit = 0;

	/**
	 * Constructs a new mock input split provider.
	 * 
	 * @param path
	 *        the path of the local file to generate the input splits from
	 * @param noSplits
	 *        the number of input splits to be generated from the given input file
	 */
	public MockInputSplitProvider(final String path, int noSplits) {

		this.inputSplits = new InputSplit[noSplits];
		String[] hosts = { "localhost" };

		String localPath = path.substring(7, path.length());
		File inFile = new File(localPath);

		long splitLength = inFile.length() / noSplits;
		long pos = 0;

		for (int i = 0; i < noSplits - 1; i++) {
			this.inputSplits[i] =
				new FileInputSplit(i, new Path(path), pos, splitLength, hosts);
			pos += splitLength;
		}

		this.inputSplits[noSplits - 1] =
			new FileInputSplit(noSplits - 1, new Path(path), pos, inFile.length() - pos, hosts);

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InputSplit getNextInputSplit() {

		if (this.nextSplit < this.inputSplits.length) {
			return this.inputSplits[this.nextSplit++];
		}

		return null;
	}

}
