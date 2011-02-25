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

package eu.stratosphere.pact.runtime.sort;

import java.io.IOException;
import java.util.Comparator;

import eu.stratosphere.nephele.services.iomanager.Deserializer;
import eu.stratosphere.nephele.services.iomanager.RawComparator;

public final class DeserializerComparator<T> implements RawComparator
{
	private final DataInputBuffer buffer = new DataInputBuffer();

	private final Deserializer<T> deserializer;

	private final Comparator<T> comparator;

	private T key1;

	private T key2;

	public DeserializerComparator(Deserializer<T> deserializer, Comparator<T> comparator)
			throws IOException
	{
		this.comparator = comparator;
		this.deserializer = deserializer;
		this.deserializer.open(buffer);
	}

	public int compare(byte[] keyBytes1, byte[] keyBytes2, int startKey1, int startKey2, int lenKey1, int lenKey2) {
		try {
			buffer.reset(keyBytes1, startKey1, lenKey1);
			key1 = deserializer.deserialize(key1);

			buffer.reset(keyBytes2, startKey2, lenKey2);
			key2 = deserializer.deserialize(key2);

			return comparator.compare(key1, key2);
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
	}
}
