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

package org.apache.flink.runtime.operators.sort;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.disk.iomanager.ChannelWriterOutputView;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * A simple collector that collects Key and Value and writes them into a given <code>Writer</code>.
 */
final class WriterCollector<E> implements Collector<E> {
	private final ChannelWriterOutputView output; // the writer to write to
	private final TypeSerializer<E> serializer;

	/**
	 * Creates a new writer collector that writes to the given writer.
	 *
	 * @param output The writer output view to write to.
	 */
	WriterCollector(ChannelWriterOutputView output, TypeSerializer<E> serializer) {
		this.output = output;
		this.serializer = serializer;
	}

	@Override
	public void collect(E record) {
		try {
			this.serializer.serialize(record, this.output);
		} catch (IOException ioex) {
			throw new RuntimeException("An error occurred forwarding the record to the writer.", ioex);
		}
	}

	@Override
	public void close() {
	}
}
