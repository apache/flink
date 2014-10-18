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

package org.apache.flink.api.common.typeutils.base;

import java.util.Random;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ShortSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;

/**
 * A test for the {@link StringSerializer}.
 */
public class ShortSerializerTest extends SerializerTestBase<Short> {
	
	@Override
	protected TypeSerializer<Short> createSerializer() {
		return new ShortSerializer();
	}
	
	@Override
	protected int getLength() {
		return 2;
	}
	
	@Override
	protected Class<Short> getTypeClass() {
		return Short.class;
	}
	
	@Override
	protected Short[] getTestData() {
		Random rnd = new Random(874597969123412341L);
		int rndInt = rnd.nextInt(32767);
		
		return new Short[] {new Short((short) 0), new Short((short) 1), new Short((short) -1),
							new Short((short) rndInt), new Short((short) -rndInt),
							new Short((short) -32767), new Short((short) 32768)};
	}
}
	
