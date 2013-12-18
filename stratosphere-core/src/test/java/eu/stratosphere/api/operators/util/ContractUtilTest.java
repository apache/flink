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

package eu.stratosphere.api.operators.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import eu.stratosphere.api.functions.GenericCoGrouper;
import eu.stratosphere.api.functions.GenericCrosser;
import eu.stratosphere.api.functions.GenericMapper;
import eu.stratosphere.api.functions.GenericJoiner;
import eu.stratosphere.api.functions.GenericReducer;
import eu.stratosphere.api.functions.Function;
import eu.stratosphere.api.io.DelimitedInputFormat;
import eu.stratosphere.api.io.FileOutputFormat;
import eu.stratosphere.api.operators.GenericDataSink;
import eu.stratosphere.api.operators.GenericDataSource;
import eu.stratosphere.api.operators.base.CoGroupOperatorBase;
import eu.stratosphere.api.operators.base.CrossOperatorBase;
import eu.stratosphere.api.operators.base.MapOperatorBase;
import eu.stratosphere.api.operators.base.JoinOperatorBase;
import eu.stratosphere.api.operators.base.ReduceOperatorBase;
import eu.stratosphere.types.PactInteger;

/**
 * Tests {@link ContractUtil}.
 */
public class ContractUtilTest {
	/**
	 * Test {@link ContractUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnCoGroupForCoGroupStub() {
		final Class<?> result = ContractUtil.getContractClass(CoGrouper.class);
		assertEquals(CoGroupOperatorBase.class, result);
	}

	/**
	 * Test {@link ContractUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnCrossForCrossStub() {
		final Class<?> result = ContractUtil.getContractClass(Crosser.class);
		assertEquals(CrossOperatorBase.class, result);
	}

	/**
	 * Test {@link ContractUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnMapForMapStub() {
		final Class<?> result = ContractUtil.getContractClass(Mapper.class);
		assertEquals(MapOperatorBase.class, result);
	}

	/**
	 * Test {@link ContractUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnMatchForMatchStub() {
		final Class<?> result = ContractUtil.getContractClass(Matcher.class);
		assertEquals(JoinOperatorBase.class, result);
	}

	/**
	 * Test {@link ContractUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnNullForStub() {
		final Class<?> result = ContractUtil.getContractClass(Function.class);
		assertEquals(null, result);
	}

	/**
	 * Test {@link ContractUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnReduceForReduceStub() {
		final Class<?> result = ContractUtil.getContractClass(Reducer.class);
		assertEquals(ReduceOperatorBase.class, result);
	}

	/**
	 * Test {@link ContractUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnSinkForOutputFormat() {
		final Class<?> result = ContractUtil.getContractClass(FileOutputFormat.class);
		assertEquals(GenericDataSink.class, result);
	}

	/**
	 * Test {@link ContractUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnSourceForInputFormat() {
		final Class<?> result = ContractUtil.getContractClass(DelimitedInputFormat.class);
		assertEquals(GenericDataSource.class, result);
	}

	static abstract class CoGrouper implements GenericCoGrouper<PactInteger, PactInteger, PactInteger> {}

	static abstract class Crosser implements GenericCrosser<PactInteger, PactInteger, PactInteger> {}

	static abstract class Mapper implements GenericMapper<PactInteger, PactInteger> {}

	static abstract class Matcher implements GenericJoiner<PactInteger, PactInteger, PactInteger> {}

	static abstract class Reducer implements GenericReducer<PactInteger, PactInteger> {}
}
