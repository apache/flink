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

package org.apache.flink.streaming.connectors.kafka.internals;

import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link KafkaTopicPartition}.
 */
public class KafkaTopicPartitionTest {

	@Test
	public void validateUid() {
		Field uidField;
		try {
			uidField = KafkaTopicPartition.class.getDeclaredField("serialVersionUID");
			uidField.setAccessible(true);
		}
		catch (NoSuchFieldException e) {
			fail("serialVersionUID is not defined");
			return;
		}

		assertTrue(Modifier.isStatic(uidField.getModifiers()));
		assertTrue(Modifier.isFinal(uidField.getModifiers()));
		assertTrue(Modifier.isPrivate(uidField.getModifiers()));

		assertEquals(long.class, uidField.getType());

		// the UID has to be constant to make sure old checkpoints/savepoints can be read
		try {
			assertEquals(722083576322742325L, uidField.getLong(null));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
