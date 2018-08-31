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

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileConstants;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link AvroKeyValueSinkWriter}.
 */
public class AvroKeyValueSinkWriterTest {

	@Test
	public void testDuplicate() {
		Map<String, String> properties = new HashMap<>();
		Schema keySchema = Schema.create(Schema.Type.STRING);
		Schema valueSchema = Schema.create(Schema.Type.STRING);
		properties.put(AvroKeyValueSinkWriter.CONF_OUTPUT_KEY_SCHEMA, keySchema.toString());
		properties.put(AvroKeyValueSinkWriter.CONF_OUTPUT_VALUE_SCHEMA, valueSchema.toString());
		properties.put(AvroKeyValueSinkWriter.CONF_COMPRESS, String.valueOf(true));
		properties.put(AvroKeyValueSinkWriter.CONF_COMPRESS_CODEC, DataFileConstants.SNAPPY_CODEC);

		AvroKeyValueSinkWriter<String, String> writer = new AvroKeyValueSinkWriter(properties);
		writer.setSyncOnFlush(true);
		AvroKeyValueSinkWriter<String, String> other = writer.duplicate();

		assertTrue(StreamWriterBaseComparator.equals(writer, other));

		writer.setSyncOnFlush(false);
		assertFalse(StreamWriterBaseComparator.equals(writer, other));
	}
}
