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

package org.apache.flink.table.runtime.typeutils.serializers.python;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.table.dataformat.BinaryArray;
import org.apache.flink.table.dataformat.BinaryMap;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.FloatType;

/**
 * Test for {@link BinaryMapSerializerTest}.
 */
public class BinaryMapSerializerTest extends SerializerTestBase<BinaryMap> {
	@Override
	protected TypeSerializer<BinaryMap> createSerializer() {
		return new BinaryMapSerializer(
			new BigIntType(), new FloatType(), LongSerializer.INSTANCE, FloatSerializer.INSTANCE);
	}

	@Override
	protected int getLength() {
		return -1;
	}

	@Override
	protected Class<BinaryMap> getTypeClass() {
		return BinaryMap.class;
	}

	@Override
	protected BinaryMap[] getTestData() {
		BinaryArray keyBinary = BinaryArray.fromPrimitiveArray(new long[]{10L});
		BinaryArray valueBinary = BinaryArray.fromPrimitiveArray(new float[]{10.2F});
		return new BinaryMap[]{BinaryMap.valueOf(keyBinary, valueBinary)};
	}
}
