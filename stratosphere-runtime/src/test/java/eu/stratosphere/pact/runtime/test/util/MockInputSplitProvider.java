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
import java.net.URI;
import java.net.URISyntaxException;

import eu.stratosphere.core.fs.FileInputSplit;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.core.io.InputSplit;
import eu.stratosphere.nephele.template.InputSplitProvider;

/**
 * The mock input split provider implements the {@link InputSplitProvider} interface to serve input splits to the test
 * jobs.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public class MockInputSplitProvider implements InputSplitProvider {

	/**
	 * The input splits to be served during the test.
	 */
	private volatile InputSplit[] inputSplits;

	/**
	 * Index pointing to the next input split to be served.
	 */
	private int nextSplit = 0;

	/**
	 * Generates a set of input splits from an input path
	 * 
	 * @param path
	 *        the path of the local file to generate the input splits from
	 * @param noSplits
	 *        the number of input splits to be generated from the given input file
	 */
	public void addInputSplits(final String path, final int noSplits) {

		final InputSplit[] tmp = new InputSplit[noSplits];
		final String[] hosts = { "localhost" };

		final String localPath;
		try {
			localPath = new URI(path).getPath();
		} catch (URISyntaxException e) {
			throw new IllegalArgumentException("Path URI can not be transformed to local path.");
		}
		
		final File inFile = new File(localPath);

		final long splitLength = inFile.length() / noSplits;
		long pos = 0;

		for (int i = 0; i < noSplits - 1; i++) {
			tmp[i] = new FileInputSplit(i, new Path(path), pos, splitLength, hosts);
			pos += splitLength;
		}

		tmp[noSplits - 1] = new FileInputSplit(noSplits - 1, new Path(path), pos, inFile.length() - pos, hosts);

		this.inputSplits = tmp;
	}


	@Override
	public InputSplit getNextInputSplit() {

		if (this.nextSplit < this.inputSplits.length) {
			return this.inputSplits[this.nextSplit++];
		}

		return null;
	}

}
