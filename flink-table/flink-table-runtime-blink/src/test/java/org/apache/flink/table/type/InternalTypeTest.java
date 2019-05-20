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

package org.apache.flink.table.type;

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.util.InstantiationUtil;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static org.apache.flink.table.type.TypeConverters.createInternalTypeFromTypeInfo;

/**
 * Test for {@link InternalType}s.
 */
public class InternalTypeTest {

	@Test
	public void testHashCodeAndEquals() throws IOException, ClassNotFoundException {
		testHashCodeAndEquals(InternalTypes.INT);
		testHashCodeAndEquals(InternalTypes.DATE);
		testHashCodeAndEquals(InternalTypes.TIME);
		testHashCodeAndEquals(InternalTypes.TIMESTAMP);
		testHashCodeAndEquals(InternalTypes.BINARY);
		testHashCodeAndEquals(InternalTypes.STRING);
		testHashCodeAndEquals(new DecimalType(15, 5));
		testHashCodeAndEquals(new GenericType<>(InternalTypeTest.class));
		testHashCodeAndEquals(new RowType(InternalTypes.STRING, InternalTypes.INT, InternalTypes.INT));
		testHashCodeAndEquals(new ArrayType(InternalTypes.INT));
		testHashCodeAndEquals(new ArrayType(InternalTypes.STRING));
		testHashCodeAndEquals(new MapType(InternalTypes.STRING, InternalTypes.INT));
	}

	private void testHashCodeAndEquals(InternalType type) throws IOException, ClassNotFoundException {
		InternalType newType = InstantiationUtil.deserializeObject
				(InstantiationUtil.serializeObject(type),
						Thread.currentThread().getContextClassLoader());

		Assert.assertEquals(type.hashCode(), newType.hashCode());
		Assert.assertEquals(type, newType);

		// need override toString.
		Assert.assertFalse(newType.toString().contains("@"));
	}

	@Test
	public void testConverter() throws IOException, ClassNotFoundException {
		testConvertToRowType(new RowTypeInfo(
				new TypeInformation[] {Types.INT, Types.STRING},
				new String[] {"field1", "field2"}));
		testConvertToRowType((CompositeType) TypeInformation.of(MyPojo.class));

		testConvertCompare(PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO,
				new ArrayType(InternalTypes.DOUBLE));
		testConvertCompare(BasicArrayTypeInfo.DOUBLE_ARRAY_TYPE_INFO, new ArrayType(InternalTypes.DOUBLE));
		testConvertCompare(ObjectArrayTypeInfo.getInfoFor(TypeInformation.of(MyPojo.class)),
				new ArrayType(createInternalTypeFromTypeInfo(TypeInformation.of(MyPojo.class))));
		testConvertCompare(new MapTypeInfo<>(Types.INT, Types.STRING),
				new MapType(InternalTypes.INT, InternalTypes.STRING));
		testConvertCompare(new GenericTypeInfo<>(MyPojo.class), new GenericType<>(MyPojo.class));
	}

	@Test
	public void testDecimalInferType() {
		Assert.assertEquals(DecimalType.of(20, 13), DecimalType.inferDivisionType(5, 2, 10, 4));
		Assert.assertEquals(DecimalType.of(7, 0), DecimalType.inferIntDivType(5, 2, 4));
		Assert.assertEquals(DecimalType.of(38, 5), DecimalType.inferAggSumType(5));
		Assert.assertEquals(DecimalType.of(38, 6), DecimalType.inferAggAvgType(5));
		Assert.assertEquals(DecimalType.of(8, 2), DecimalType.inferRoundType(10, 5, 2));
		Assert.assertEquals(DecimalType.of(8, 2), DecimalType.inferRoundType(10, 5, 2));
	}

	private void testConvertToRowType(CompositeType typeInfo) {
		RowType rowType = (RowType) createInternalTypeFromTypeInfo(typeInfo);
		Assert.assertArrayEquals(
				new InternalType[] {InternalTypes.INT, InternalTypes.STRING},
				rowType.getFieldTypes());
		Assert.assertArrayEquals(
				new String[] {"field1", "field2"},
				rowType.getFieldNames());
	}

	private void testConvertCompare(TypeInformation typeInfo, InternalType internalType) {
		InternalType converted = createInternalTypeFromTypeInfo(typeInfo);
		Assert.assertEquals(internalType, converted);
		Assert.assertEquals(internalType.hashCode(), converted.hashCode());
	}

	/**
	 * Test pojo.
	 */
	public static class MyPojo {
		public int field1;
		public String field2;
	}
}
