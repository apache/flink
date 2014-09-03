/**
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
import org.apache.flink.api.common.typeutils.base.IntSerializer;
/**
 * A test for the {@link IntSerializerTest}.
 */
public class IntSerializerTest extends SerializerTestBase<Integer> {
	
	@Override
	protected TypeSerializer<Integer> createSerializer() {
		return new IntSerializer();
	}
	
	@Override
	protected int getLength() {
		return 4;
	}
	
	@Override
	protected Class<Integer> getTypeClass() {
		return Integer.class;
	}
	
	@Override
	protected Integer[] getTestData() {
		Random rnd = new Random(874597969123412341L);
		int rndInt = rnd.nextInt();
		
		return new Integer[] {new Integer(0), new Integer(1), new Integer(-1),
							new Integer(Integer.MAX_VALUE), new Integer(Integer.MIN_VALUE),
							new Integer(rndInt), new Integer(-rndInt)};
	}
}	
