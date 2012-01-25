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
package eu.stratosphere.pact.testing.ioformats;

import java.io.DataInputStream;
import java.io.IOException;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.pact.common.io.FileInputFormat;
import eu.stratosphere.pact.common.io.statistics.BaseStatistics;
import eu.stratosphere.pact.common.type.PactRecord;

/**
 * Reads the key/value pairs from the native format which is deserialable without configuration. The type parameter can
 * be omitted (no need to subclass this format) since all type information are stored in the file.
 * 
 * @author Arvid Heise
 * @param <K>
 *        the type of the key to read
 * @param <V>
 *        the type of the value to read
 * @see SequentialOutputFormat
 */
public class SequentialInputFormat extends FileInputFormat
{

	private DataInputStream dataInputStream;

	public SequentialInputFormat() {
	}

	@Override
	public void close() throws IOException {
		super.close();
		this.dataInputStream.close();
	}

	@Override
	public void configure(final Configuration parameters)
	{
		super.configure(parameters);
	}

	@Override
	public boolean nextRecord(final PactRecord record) throws IOException {
		if (this.dataInputStream.available() == 0)
			return false;
		record.read(this.dataInputStream);
		return true;
	}

	@Override
	public void open(FileInputSplit split) throws IOException {
		super.open(split);

		this.dataInputStream = new DataInputStream(this.stream);
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return this.dataInputStream.available() == 0;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.InputFormat#getStatistics()
	 */
	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
		return null;
	}
}