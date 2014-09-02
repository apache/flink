/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.runtime.operators.util;

import java.io.IOException;

import org.apache.flink.runtime.io.network.api.MutableReader;
import org.apache.flink.types.Record;
import org.apache.flink.util.MutableObjectIterator;

/**
* A {@link MutableObjectIterator} that wraps a Nephele Reader producing {@link Record}s.
*/
public final class RecordReaderIterator implements MutableObjectIterator<Record> {
	
	private final MutableReader<Record> reader;		// the source

	/**
	 * Creates a new iterator, wrapping the given reader.
	 * 
	 * @param reader The reader to wrap.
	 */
	public RecordReaderIterator(MutableReader<Record> reader) {
		this.reader = reader;
	}

	@Override
	public Record next(Record reuse) throws IOException {
		try {
			if (this.reader.next(reuse)) {
				return reuse;
			} else {
				return null;
			}
		}
		catch (InterruptedException e) {
			throw new IOException("Reader interrupted.", e);
		}
	}
}
