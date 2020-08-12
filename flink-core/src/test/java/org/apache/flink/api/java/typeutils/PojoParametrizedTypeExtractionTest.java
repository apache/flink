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

package org.apache.flink.api.java.typeutils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 *  Tests concerning type extraction of Parametrized Pojo and its superclasses.
 */
public class PojoParametrizedTypeExtractionTest {
	@Test
	public void testDirectlyCreateTypeInfo() {
		final TypeInformation<ParameterizedParentImpl> directTypeInfo =
			TypeExtractor.createTypeInfo(ParameterizedParentImpl.class);

		assertThat(directTypeInfo, equalTo(getParameterizedParentTypeInformation()));
	}

	@Test
	public void testMapReturnTypeInfo(){
		TypeInformation<ParameterizedParentImpl> expectedTypeInfo = getParameterizedParentTypeInformation();

		TypeInformation<ParameterizedParentImpl> mapReturnTypeInfo = TypeExtractor
			.getMapReturnTypes(new ConcreteMapFunction(), Types.INT);

		assertThat(mapReturnTypeInfo, equalTo(expectedTypeInfo));
	}

	private TypeInformation<ParameterizedParentImpl> getParameterizedParentTypeInformation() {
		Map<String, TypeInformation<?>> nestedFields = new HashMap<>();
		nestedFields.put("digits", Types.INT);
		nestedFields.put("letters", Types.STRING);

		Map<String, TypeInformation<?>> fields = new HashMap<>();
		fields.put("precise", Types.DOUBLE);
		fields.put("pojoField", Types.POJO(Pojo.class, nestedFields));

		return Types.POJO(
			ParameterizedParentImpl.class,
			fields
		);
	}

	/**
	 * Representation of Pojo class with 2 fields.
	 */
	public static class Pojo {
		public int digits;
		public String letters;
	}

	/**
	 * Representation of class which is parametrized by some pojo.
	 */
	public static class ParameterizedParent<T> {
		public T pojoField;
	}

	/**
	 * Implementation of ParametrizedParent parametrized by Pojo.
	 */
	public static class ParameterizedParentImpl extends ParameterizedParent<Pojo> {
		public double precise;
	}
	/**
	 * Representation of map function for type extraction.
	 */
	public static class ConcreteMapFunction implements MapFunction<Integer, ParameterizedParentImpl> {
		@Override
		public ParameterizedParentImpl map(Integer value) throws Exception {
			return null;
		}
	}
}
