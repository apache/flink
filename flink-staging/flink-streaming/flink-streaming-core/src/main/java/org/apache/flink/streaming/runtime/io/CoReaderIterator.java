/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import java.io.IOException;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.ReusingDeserializationDelegate;

/**
 * A CoReaderIterator wraps a {@link CoRecordReader} producing records of two
 * input types.
 */
public class CoReaderIterator<T1, T2> {

	private final CoRecordReader<DeserializationDelegate<T1>, DeserializationDelegate<T2>> reader; // the
																									// source

	protected final ReusingDeserializationDelegate<T1> delegate1;
	protected final ReusingDeserializationDelegate<T2> delegate2;

	public CoReaderIterator(
			CoRecordReader<DeserializationDelegate<T1>, DeserializationDelegate<T2>> reader,
			TypeSerializer<T1> serializer1, TypeSerializer<T2> serializer2) {
		this.reader = reader;
		this.delegate1 = new ReusingDeserializationDelegate<T1>(serializer1);
		this.delegate2 = new ReusingDeserializationDelegate<T2>(serializer2);
	}

	public int next(T1 target1, T2 target2) throws IOException {
		this.delegate1.setInstance(target1);
		this.delegate2.setInstance(target2);

		try {
			return this.reader.getNextRecord(this.delegate1, this.delegate2);

		} catch (InterruptedException e) {
			throw new IOException("Reader interrupted.", e);
		}
	}
}
