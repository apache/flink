/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.common.utils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.ml.common.linalg.DenseVector;
import org.apache.flink.ml.common.linalg.SparseVector;
import org.apache.flink.ml.common.linalg.Vector;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Test cases for VectorTypes.
 */
public class VectorTypesTest {
	@SuppressWarnings("unchecked")
	private static <V extends Vector> void doVectorSerDeserTest(TypeSerializer ser, V vector) throws IOException {
		DataOutputSerializer out = new DataOutputSerializer(1024);
		ser.serialize(vector, out);
		DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
		Vector deserialize = (Vector) ser.deserialize(in);
		Assert.assertEquals(vector.getClass(), deserialize.getClass());
		Assert.assertEquals(vector, deserialize);
	}

	@Test
	public void testVectorsSerDeser() throws IOException {
		// Prepare data
		SparseVector sparseVector = new SparseVector(10, new HashMap<Integer, Double>() {{
			ThreadLocalRandom rand = ThreadLocalRandom.current();
			for (int i = 0; i < 10; i += 2) {
				this.put(i, rand.nextDouble());
			}
		}});
		DenseVector denseVector = DenseVector.rand(10);

		// Prepare serializer
		ExecutionConfig config = new ExecutionConfig();
		TypeSerializer<Vector> vecSer = VectorTypes.VECTOR.createSerializer(config);
		TypeSerializer<SparseVector> sparseSer = VectorTypes.SPARSE_VECTOR.createSerializer(config);
		TypeSerializer<DenseVector> denseSer = VectorTypes.DENSE_VECTOR.createSerializer(config);

		// Do tests.
		doVectorSerDeserTest(vecSer, sparseVector);
		doVectorSerDeserTest(vecSer, denseVector);
		doVectorSerDeserTest(sparseSer, sparseVector);
		doVectorSerDeserTest(denseSer, denseVector);
	}
}
