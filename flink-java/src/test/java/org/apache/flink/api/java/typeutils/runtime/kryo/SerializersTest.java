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
package org.apache.flink.api.java.typeutils.runtime.kryo;

import de.javakaffee.kryoserializers.jodatime.JodaIntervalSerializer;
import org.apache.flink.api.common.ExecutionConfig;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class SerializersTest {

	// recursive
	public static class Node {
		private Node parent;
	}
	public static class FromNested {
		Node recurseMe;
	}
	public static class FromGeneric {

	}
	public static class Nested1 {
		private FromNested fromNested;
		private Interval yodaIntervall;
	}

	public static class ClassWithNested {
		Nested1 nested;
		int ab;
		ArrayList<FromGeneric> addGenType;
	}

	@Test
	public void testTypeRegistration() {
		ExecutionConfig conf = new ExecutionConfig();
		Serializers.recursivelyRegisterType(ClassWithNested.class, conf);
		KryoSerializer kryo = new KryoSerializer(String.class, conf); // we create Kryo from another type.

		Assert.assertTrue(kryo.getKryo().getRegistration(FromNested.class).getId() > 0);
		Assert.assertTrue(kryo.getKryo().getRegistration(ClassWithNested.class).getId() > 0);
		Assert.assertTrue(kryo.getKryo().getRegistration(Interval.class).getId() > 0);
		Assert.assertTrue(kryo.getKryo().getRegistration(Interval.class).getSerializer().getClass() == JodaIntervalSerializer.class);
		// check if the generic type from one field is also registered (its very likely that
		// generic types are also used as fields somewhere.
		Assert.assertTrue(kryo.getKryo().getRegistration(FromGeneric.class).getId() > 0);
		Assert.assertTrue(kryo.getKryo().getRegistration(Node.class).getId() > 0);
	}
}
