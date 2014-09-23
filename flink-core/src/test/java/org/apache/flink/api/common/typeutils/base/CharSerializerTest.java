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
import org.apache.flink.api.common.typeutils.base.CharSerializer;

/**
 * A test for the {@link CharSerializer}.
 */
public class CharSerializerTest extends SerializerTestBase<Character> {
	
	@Override
	protected TypeSerializer<Character> createSerializer() {
		return new CharSerializer();
	}
	
	@Override
	protected int getLength() {
		return 2;
	}
	
	@Override
	protected Class<Character> getTypeClass() {
		return Character.class;
	}
	
	@Override
	protected Character[] getTestData() {
		Random rnd = new Random(874597969123412341L);
		int rndInt = rnd.nextInt((int) Character.MAX_VALUE);
		
		return new Character[] {new Character('a'), new Character('@'), new Character('ä'),
								new Character('1'), new Character((char) rndInt),
								Character.MAX_VALUE, Character.MIN_VALUE};
	}
}
