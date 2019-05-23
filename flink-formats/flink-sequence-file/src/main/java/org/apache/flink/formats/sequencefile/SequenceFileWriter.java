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

package org.apache.flink.formats.sequencefile;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link BulkWriter} implementation that wraps a {@link SequenceFile.Writer}.
 *
 * @param <K> The type of key written.
 * @param <V> The type of value written.
 */
@PublicEvolving
public class SequenceFileWriter<K extends Writable, V extends Writable> implements BulkWriter<Tuple2<K, V>> {

	private final SequenceFile.Writer writer;

	SequenceFileWriter(SequenceFile.Writer writer) {
		this.writer = checkNotNull(writer);
	}

	@Override
	public void addElement(Tuple2<K, V> element) throws IOException {
		writer.append(element.f0, element.f1);
	}

	@Override
	public void flush() throws IOException {
		writer.hsync();
	}

	@Override
	public void finish() throws IOException {
		writer.close();
	}
}
