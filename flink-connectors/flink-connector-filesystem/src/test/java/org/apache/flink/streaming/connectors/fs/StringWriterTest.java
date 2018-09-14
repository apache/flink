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

import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link StringWriter}.
 */
public class StringWriterTest {

	@Test
	public void testDuplicate() {
		StringWriter<String> writer = new StringWriter(StandardCharsets.UTF_16.name());
		writer.setSyncOnFlush(true);
		StringWriter<String> other = writer.duplicate();

		assertTrue(StreamWriterBaseComparator.equals(writer, other));

		writer.setSyncOnFlush(false);
		assertFalse(StreamWriterBaseComparator.equals(writer, other));
		assertFalse(StreamWriterBaseComparator.equals(writer, new StringWriter<>()));
	}
}
