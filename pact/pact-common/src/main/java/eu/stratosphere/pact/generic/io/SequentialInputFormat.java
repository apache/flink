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
package eu.stratosphere.pact.generic.io;

import java.io.DataInput;
import java.io.IOException;

import eu.stratosphere.nephele.types.Record;

/**
 * Reads the {@link Record}s from the native format which is deserializable without configuration.
 * 
 * @author Arvid Heise
 * @see SequentialOutputFormat
 */
public class SequentialInputFormat<T extends Record> extends BinaryInputFormat<T> {
	private static final long serialVersionUID = 1L;
	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.pact.testing.ioformats.BlockBasedInputFormat#deserialize(eu.stratosphere.pact.common.type.PactRecord
	 * , java.io.DataInput)
	 */
	@Override
	protected void deserialize(Record record, DataInput dataInput) throws IOException {
		record.read(dataInput);		
	}
}