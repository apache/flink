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

package org.apache.flink.api.common.typeutils.base.array;

import java.util.Random;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.array.StringArraySerializer;
import org.apache.flink.util.StringUtils;
import org.junit.Test;

import static org.junit.Assert.assertFalse;


/**
 * A test for the {@link StringArraySerializer}.
 */
public class StringArraySerializerTest extends SerializerTestBase<String[]> {

	@Override
	protected TypeSerializer<String[]> createSerializer() {
		return new StringArraySerializer();
	}

	@Override
	protected Class<String[]> getTypeClass() {
		return String[].class;
	}
	
	@Override
	protected int getLength() {
		return -1;
	}

	@Override
	protected String[][] getTestData() {
		Random rnd = new Random(874597969123412341L);
		

		return new String[][] {
			new String[] {"a", "", "bcd", "jbmbmner8 jhk hj \n \t üäßß@µ", "", "non-empty"},
			new String[] {"a", null, "", null, "bcd", null, "jbmbmner8 jhk hj \n \t üäßß@µ", null, "", null, "non-empty"},
			new String[] {StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2),
				StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2),
				StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2),
				StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2)},
			new String[] {StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2),
				"",
				StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2),
				null,
				StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2),
				null,
				"",
				StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2),
				"",
				null}
		};
	}

	@Test
	public void arrayTypeIsMutable() {
		StringArraySerializer serializer = (StringArraySerializer) createSerializer();
		assertFalse(serializer.isImmutableType());
	}
}
