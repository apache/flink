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

package org.apache.flink.formats.string;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.descriptors.Descriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.StringFormat;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.SerializationSchemaFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link StringRowFormatFactory}.
 */
public class StringRowFormatFactoryTest extends TestLogger {

	@Test
	public void testSchema() {
		final Map<String, String> properties = toMap(
			new StringFormat()
				.setEncoding("UTF-8")
				.setSchema("nid")
				.setFailOnNull(true)
				.setFailOnEmpty(true)
		);

		testSchemaSerializationSchema(properties);

		testSchemaDeserializationSchema(properties);
	}

	private void testSchemaDeserializationSchema(Map<String, String> properties) {
		final DeserializationSchema<?> actual2 = TableFactoryService
			.find(DeserializationSchemaFactory.class, properties)
			.createDeserializationSchema(properties);
		final StringRowDeserializationSchema expected2 = new StringRowDeserializationSchema("nid", "UTF-8");
		expected2.setFailOnNull(true);
		expected2.setFailOnEmpty(true);
		assertEquals(expected2, actual2);
	}

	private void testSchemaSerializationSchema(Map<String, String> properties) {
		final SerializationSchema<?> actual1 = TableFactoryService
			.find(SerializationSchemaFactory.class, properties)
			.createSerializationSchema(properties);
		final SerializationSchema expected1 = new StringRowSerializationSchema("UTF-8");
		assertEquals(expected1, actual1);
	}

	private static Map<String, String> toMap(Descriptor... desc) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		for (Descriptor d : desc) {
			d.addProperties(descriptorProperties);
		}
		return descriptorProperties.asMap();
	}
}
