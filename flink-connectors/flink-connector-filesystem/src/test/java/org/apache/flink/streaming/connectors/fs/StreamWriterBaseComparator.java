/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.fs;

import org.apache.hadoop.io.Writable;

import java.util.Objects;

/**
 * Helper class to perform partial comparisons of {@link StreamWriterBase} instances. During comparisons
 * it ignores changes in underlying output streams.
 */
public class StreamWriterBaseComparator {

	public static <T> boolean equals(
			StreamWriterBase<T> writer1,
			StreamWriterBase<T> writer2) {
		return Objects.equals(writer1.isSyncOnFlush(), writer2.isSyncOnFlush());
	}

	public static <K, V> boolean equals(
			AvroKeyValueSinkWriter<K, V> writer1,
			AvroKeyValueSinkWriter<K, V> writer2) {
		return equals((StreamWriterBase) writer1, (StreamWriterBase) writer2) &&
			Objects.equals(writer1.getProperties(), writer2.getProperties());
	}

	public static <K extends Writable, V extends Writable> boolean equals(
			SequenceFileWriter<K, V> writer1,
			SequenceFileWriter<K, V> writer2) {
		return equals((StreamWriterBase) writer1, (StreamWriterBase) writer2) &&
			Objects.equals(writer1.getCompressionCodecName(), writer2.getCompressionCodecName()) &&
			Objects.equals(writer1.getCompressionType(), writer2.getCompressionType()) &&
			Objects.equals(writer1.getKeyClass(), writer2.getKeyClass()) &&
			Objects.equals(writer1.getValueClass(), writer2.getValueClass());
	}

	public static <T> boolean equals(
		StringWriter<T> writer1,
		StringWriter<T> writer2) {
		return equals((StreamWriterBase) writer1, (StreamWriterBase) writer2) &&
			Objects.equals(writer1.getCharsetName(), writer2.getCharsetName());
	}
}
