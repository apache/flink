/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.sort;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Comparator;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.nephele.services.iomanager.Deserializer;
import eu.stratosphere.nephele.services.iomanager.RawComparator;
import eu.stratosphere.nephele.services.iomanager.SerializationFactory;
import eu.stratosphere.nephele.services.iomanager.Serializer;
import eu.stratosphere.pact.runtime.serialization.WritableSerializationFactory;
import eu.stratosphere.pact.runtime.sort.DeserializerComparator;
import eu.stratosphere.pact.runtime.test.util.TestData;

/**
 * @author Erik Nijkamp
 */
public class TestDeserializerComparator {

	@Test
	public void testCompare() throws Exception {
		// initialize comparator
		SerializationFactory<TestData.Key> keySerialization = new WritableSerializationFactory<TestData.Key>(
			TestData.Key.class);
		Comparator<TestData.Key> keyComparator = new TestData.KeyComparator();
		Deserializer<TestData.Key> keyDeserializer = keySerialization.getDeserializer();
		Serializer<TestData.Key> keySerializer = keySerialization.getSerializer();
		RawComparator rawComparator = new DeserializerComparator<TestData.Key>(keyDeserializer, keyComparator);

		// sample data
		TestData.Key key1 = new TestData.Key(10);
		TestData.Key key2 = new TestData.Key(20);

		// serialize
		ByteArrayOutputStream buffer1 = new ByteArrayOutputStream();
		DataOutputStream dataOutput1 = new DataOutputStream(buffer1);
		keySerializer.open(dataOutput1);
		keySerializer.serialize(key1);
		byte[] key1bytes = buffer1.toByteArray();

		ByteArrayOutputStream buffer2 = new ByteArrayOutputStream();
		DataOutputStream dataOutput2 = new DataOutputStream(buffer2);
		keySerializer.open(dataOutput2);
		keySerializer.serialize(key2);
		byte[] key2bytes = buffer2.toByteArray();

		// assertion
		Assert.assertTrue(rawComparator.compare(key1bytes, key2bytes, 0, 0, key1bytes.length, key2bytes.length) < 0);
		Assert.assertTrue(rawComparator.compare(key2bytes, key1bytes, 0, 0, key2bytes.length, key1bytes.length) > 0);
		Assert.assertTrue(rawComparator.compare(key1bytes, key1bytes, 0, 0, key1bytes.length, key1bytes.length) == 0);
	}
}
