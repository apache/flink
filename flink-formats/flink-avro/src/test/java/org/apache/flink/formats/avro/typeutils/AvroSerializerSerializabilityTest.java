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

package org.apache.flink.formats.avro.typeutils;

import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.junit.Assert.assertEquals;

/**
 * Test that validates that the serialized form of the AvroSerializer is the same as in
 * previous Flink versions.
 *
 * <p>While that is not strictly necessary for FLink to work, it increases user experience
 * in job upgrade situations.
 */
public class AvroSerializerSerializabilityTest {

	private static final String RESOURCE_NAME = "flink-1.4-serializer-java-serialized";

	@Test
	public void testDeserializeSerializer() throws Exception {
		final AvroSerializer<String> currentSerializer = new AvroSerializer<>(String.class);

		try (ObjectInputStream in = new ObjectInputStream(
				getClass().getClassLoader().getResourceAsStream(RESOURCE_NAME))) {

			@SuppressWarnings("unchecked")
			AvroSerializer<String> deserialized = (AvroSerializer<String>) in.readObject();

			assertEquals(currentSerializer, deserialized);
		}
	}

	// ------------------------------------------------------------------------
	//  To create a serialized serializer file
	// ------------------------------------------------------------------------

	public static void main(String[] args) throws Exception {
		final AvroSerializer<String> serializer = new AvroSerializer<>(String.class);

		final File file = new File("flink-formats/flink-avro/src/test/resources/" + RESOURCE_NAME).getAbsoluteFile();

		try (FileOutputStream fos = new FileOutputStream(file);
				ObjectOutputStream out = new ObjectOutputStream(fos)) {

			out.writeObject(serializer);
		}
	}
}
