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

package eu.stratosphere.pact.common.recordio;


import java.io.IOException;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.FSDataOutputStream;


/**
 * Describes the base interface that is used for writing to a output.
 * By overriding the createPair() and writePair() the values can be written
 * using a custom format.
 * 
 * @author Moritz Kaufmann
 */
public abstract class FileOutputFormat implements OutputFormat
{
	protected FSDataOutputStream stream;


	/**
	 * Sets the output stream to which the data should be written.
	 * 
	 * @param fdos The output stream to which the data should be written.
	 */
	public void setOutput(FSDataOutputStream fdos) {
		this.stream = fdos;
	}

	// --------------------------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.recordio.OutputFormat#configure(eu.stratosphere.nephele.configuration.Configuration)
	 */
	@Override
	public void configure(Configuration parameters) {
	}


	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.recordio.OutputFormat#open()
	 */
	@Override
	public void open() throws IOException {
	}


	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.recordio.OutputFormat#close()
	 */
	@Override
	public void close() throws IOException {
	}
}
