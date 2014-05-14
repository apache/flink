/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.api.java.typeutils.runtime;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.Assert;

import eu.stratosphere.api.common.typeutils.SerializerTestInstance;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.java.tuple.Tuple;

public class TupleSerializerTestInstance<T extends Tuple> extends SerializerTestInstance<T> {

	public TupleSerializerTestInstance(TypeSerializer<T> serializer, Class<T> typeClass, int length, T[] testData) {
		super(serializer, typeClass, length, testData);
	}
	
	protected void deepEquals(String message, T shouldTuple, T isTuple) {
		Assert.assertEquals(shouldTuple.getArity(), isTuple.getArity());
		
		for (int i = 0; i < shouldTuple.getArity(); i++) {
			Object should = shouldTuple.getField(i);
			Object is = isTuple.getField(i);
			
			if (should.getClass().isArray()) {
				if (should instanceof boolean[]) {
					Assert.assertTrue(message, Arrays.equals((boolean[]) should, (boolean[]) is));
				}
				else if (should instanceof byte[]) {
					assertArrayEquals(message, (byte[]) should, (byte[]) is);
				}
				else if (should instanceof short[]) {
					assertArrayEquals(message, (short[]) should, (short[]) is);
				}
				else if (should instanceof int[]) {
					assertArrayEquals(message, (int[]) should, (int[]) is);
				}
				else if (should instanceof long[]) {
					assertArrayEquals(message, (long[]) should, (long[]) is);
				}
				else if (should instanceof float[]) {
					assertArrayEquals(message, (float[]) should, (float[]) is, 0.0f);
				}
				else if (should instanceof double[]) {
					assertArrayEquals(message, (double[]) should, (double[]) is, 0.0);
				}
				else if (should instanceof char[]) {
					assertArrayEquals(message, (char[]) should, (char[]) is);
				}
				else {
					assertArrayEquals(message, (Object[]) should, (Object[]) is);
				}
			}
			else {
				assertEquals(message,  should, is);
			}
		}
	}
}
