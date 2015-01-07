/*
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


package org.apache.flink.runtime.operators.testutils;

import java.io.IOException;
import java.util.Iterator;

import org.apache.flink.types.Record;
import org.apache.flink.util.MutableObjectIterator;


/**
 *
 */
public class MutableObjectIteratorWrapper implements MutableObjectIterator<Record>
{
	private final Iterator<Record> source;
	
	public MutableObjectIteratorWrapper(Iterator<Record> source)
	{
		this.source = source;
	}


	@Override
	public Record next(Record reuse) throws IOException {
		if (this.source.hasNext()) {
			this.source.next().copyTo(reuse);
			return reuse;
		}
		else {
			return null;
		}
	}

	@Override
	public Record next() throws IOException {
		if (this.source.hasNext()) {
			Record result = new Record();
			this.source.next().copyTo(result);
			return result;
		}
		else {
			return null;
		}
	}


}
