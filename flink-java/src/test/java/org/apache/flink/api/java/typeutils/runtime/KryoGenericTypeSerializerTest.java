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

package org.apache.flink.api.java.typeutils.runtime;


import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;

public class KryoGenericTypeSerializerTest extends AbstractGenericTypeSerializerTest {

	@Test
	public void testJavaList(){
		Collection<Integer> a = new ArrayList<Integer>();

		fillCollection(a);

		runTests(a);
	}

	@Test
	public void testJavaSet(){
		Collection<Integer> b = new HashSet<Integer>();

		fillCollection(b);

		runTests(b);
	}

	@Test
	public void testJavaDequeue(){
		Collection<Integer> c = new LinkedList<Integer>();

		fillCollection(c);

		runTests(c);
	}

	private void fillCollection(Collection<Integer> coll){
		coll.add(42);
		coll.add(1337);
		coll.add(49);
		coll.add(1);
	}

	@Override
	protected <T> TypeSerializer<T> createSerializer(Class<T> type) {
		return new KryoSerializer<T>(type);
	}
}