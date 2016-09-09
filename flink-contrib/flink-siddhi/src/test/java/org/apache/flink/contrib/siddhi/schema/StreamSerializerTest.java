/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.siddhi.schema;

import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.contrib.siddhi.source.Event;
import org.junit.Assert;
import org.junit.Test;

public class StreamSerializerTest {
	private final static long CURRENT = System.currentTimeMillis();

	@Test
	public void testSimplePojoRead() {
		Event event = new Event();
		event.setId(1);
		event.setName("test");
		event.setPrice(56.7);
		event.setTimestamp(CURRENT);

		StreamSchema<Event> schema = new StreamSchema<>(TypeExtractor.createTypeInfo(Event.class), "id", "name", "price", "timestamp");
		StreamSerializer<Event> reader = new StreamSerializer<>(schema);
		Assert.assertArrayEquals(new Object[]{1, "test", 56.7, CURRENT}, reader.getRow(event));
	}
}
