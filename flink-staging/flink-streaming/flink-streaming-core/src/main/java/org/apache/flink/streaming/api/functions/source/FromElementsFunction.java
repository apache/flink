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

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.InputViewDataInputStreamWrapper;
import org.apache.flink.core.memory.OutputViewDataOutputStreamWrapper;

import java.io.*;
import java.util.Iterator;

public class FromElementsFunction<T> implements SourceFunction<T> {
	
	private static final long serialVersionUID = 1L;

	private final TypeSerializer<T> serializer;
	private final byte[] elements;

	private volatile boolean isRunning = true;

	public FromElementsFunction(TypeSerializer<T> serializer, final T... elements) {
		this(serializer, new Iterable<T>() {
			@Override
			public Iterator<T> iterator() {
				return new Iterator<T>() {
					int index = 0;

					@Override
					public boolean hasNext() {
						return index < elements.length;
					}

					@Override
					public T next() {
						return elements[index++];
					}
				};
			}
		});
	}

	public FromElementsFunction(TypeSerializer<T> serializer, Iterable<T> elements) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		OutputViewDataOutputStreamWrapper wrapper = new OutputViewDataOutputStreamWrapper(new DataOutputStream(baos));

		try {
			for (T element : elements)
				serializer.serialize(element, wrapper);
		} catch (IOException e) {
			// ByteArrayOutputStream doesn't throw IOExceptions when written to
		}
		// closing the DataOutputStream would just flush the ByteArrayOutputStream, which in turn doesn't do anything.

		this.serializer = serializer;
		this.elements = baos.toByteArray();
	}

	@Override
	public void run(SourceContext<T> ctx) throws Exception {
		T value = serializer.createInstance();
		ByteArrayInputStream bais = new ByteArrayInputStream(elements);
		DataInputView input = new InputViewDataInputStreamWrapper(new DataInputStream(bais));

		while (isRunning && bais.available() > 0) {
			value = serializer.deserialize(value, input);
			ctx.collect(value);
		}
		// closing the DataOutputStream would just close the ByteArrayInputStream, which doesn't do anything
	}

	@Override
	public void cancel() {
		isRunning = false;
	}
}
