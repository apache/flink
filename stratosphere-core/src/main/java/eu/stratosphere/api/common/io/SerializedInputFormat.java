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
package eu.stratosphere.api.common.io;

import java.io.DataInput;
import java.io.IOException;

import eu.stratosphere.core.io.IOReadableWritable;

/**
 * Reads elements by deserializing them with their regular serialization/deserialization functionality.
 * 
 * @see SerializedOutputFormat
 */
public class SerializedInputFormat<T extends IOReadableWritable> extends BinaryInputFormat<T> {

	private static final long serialVersionUID = 1L;

	@Override
	protected T deserialize(T reuse, DataInput dataInput) throws IOException {
		reuse.read(dataInput);
		return reuse;
	}
}
