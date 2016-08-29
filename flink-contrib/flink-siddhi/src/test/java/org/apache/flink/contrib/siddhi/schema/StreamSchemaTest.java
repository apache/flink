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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.contrib.siddhi.Event;

import static org.junit.Assert.*;
import org.junit.Test;

/**
 * TODO: Add more unit test for other type info
 */
public class StreamSchemaTest {
	@Test
	public void testStreamSchemaWithPojo(){
		TypeInformation<Event> typeInfo = TypeExtractor.createTypeInfo(Event.class);
		assertTrue("Type information should be PojoTypeInfo",typeInfo instanceof PojoTypeInfo);
		StreamSchema<Event> schema = new StreamSchema<>(typeInfo, "id", "timestamp", "name", "price");
		assertEquals(4,schema.getFieldIndexes().length);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testStreamSchemaWithPojoAndUnknownField(){
		TypeInformation<Event> typeInfo = TypeExtractor.createTypeInfo(Event.class);
		new StreamSchema<>(typeInfo, "id", "timestamp", "name", "price","unknown");
	}

}
