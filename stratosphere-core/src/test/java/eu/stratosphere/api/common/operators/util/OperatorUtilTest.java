/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package eu.stratosphere.api.common.operators.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import eu.stratosphere.api.common.functions.Function;
import eu.stratosphere.api.common.functions.GenericCoGrouper;
import eu.stratosphere.api.common.functions.GenericCollectorMap;
import eu.stratosphere.api.common.functions.GenericCrosser;
import eu.stratosphere.api.common.functions.GenericGroupReduce;
import eu.stratosphere.api.common.functions.GenericJoiner;
import eu.stratosphere.api.common.io.DelimitedInputFormat;
import eu.stratosphere.api.common.io.FileOutputFormat;
import eu.stratosphere.api.common.operators.GenericDataSink;
import eu.stratosphere.api.common.operators.GenericDataSource;
import eu.stratosphere.api.common.operators.base.CoGroupOperatorBase;
import eu.stratosphere.api.common.operators.base.CrossOperatorBase;
import eu.stratosphere.api.common.operators.base.GroupReduceOperatorBase;
import eu.stratosphere.api.common.operators.base.JoinOperatorBase;
import eu.stratosphere.api.common.operators.base.MapOperatorBase;
import eu.stratosphere.types.IntValue;

/**
 * Tests {@link OperatorUtil}.
 */
public class OperatorUtilTest {
	/**
	 * Test {@link OperatorUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnCoGroupForCoGroupStub() {
		final Class<?> result = OperatorUtil.getContractClass(CoGrouper.class);
		assertEquals(CoGroupOperatorBase.class, result);
	}

	/**
	 * Test {@link OperatorUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnCrossForCrossStub() {
		final Class<?> result = OperatorUtil.getContractClass(Crosser.class);
		assertEquals(CrossOperatorBase.class, result);
	}

	/**
	 * Test {@link OperatorUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnMapForMapStub() {
		final Class<?> result = OperatorUtil.getContractClass(Mapper.class);
		assertEquals(MapOperatorBase.class, result);
	}

	/**
	 * Test {@link OperatorUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnMatchForMatchStub() {
		final Class<?> result = OperatorUtil.getContractClass(Matcher.class);
		assertEquals(JoinOperatorBase.class, result);
	}

	/**
	 * Test {@link OperatorUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnNullForStub() {
		final Class<?> result = OperatorUtil.getContractClass(Function.class);
		assertEquals(null, result);
	}

	/**
	 * Test {@link OperatorUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnReduceForReduceStub() {
		final Class<?> result = OperatorUtil.getContractClass(Reducer.class);
		assertEquals(GroupReduceOperatorBase.class, result);
	}

	/**
	 * Test {@link OperatorUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnSinkForOutputFormat() {
		final Class<?> result = OperatorUtil.getContractClass(FileOutputFormat.class);
		assertEquals(GenericDataSink.class, result);
	}

	/**
	 * Test {@link OperatorUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnSourceForInputFormat() {
		final Class<?> result = OperatorUtil.getContractClass(DelimitedInputFormat.class);
		assertEquals(GenericDataSource.class, result);
	}

	static abstract class CoGrouper implements GenericCoGrouper<IntValue, IntValue, IntValue> {}

	static abstract class Crosser implements GenericCrosser<IntValue, IntValue, IntValue> {}

	static abstract class Mapper implements GenericCollectorMap<IntValue, IntValue> {}

	static abstract class Matcher implements GenericJoiner<IntValue, IntValue, IntValue> {}

	static abstract class Reducer implements GenericGroupReduce<IntValue, IntValue> {}
}
