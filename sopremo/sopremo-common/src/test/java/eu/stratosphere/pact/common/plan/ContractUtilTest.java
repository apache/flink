package eu.stratosphere.pact.common.plan;

import static org.junit.Assert.assertEquals;

import java.util.Iterator;

import org.junit.Test;

import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.io.TextOutputFormat;
import eu.stratosphere.pact.common.stub.CoGroupStub;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.CrossStub;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.stub.Stub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;

/**
 * Tests {@link ContractUtil}.
 * 
 * @author Arvid Heise
 */
public class ContractUtilTest {
	/**
	 * Test {@link ContractUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnNullForStub() {
		Class<?> result = ContractUtil.getContractClass(Stub.class);
		assertEquals(null, result);
	}

	/**
	 * Test {@link ContractUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnMapForMapStub() {
		Class<?> result = ContractUtil.getContractClass(Mapper.class);
		assertEquals(MapContract.class, result);
	}

	/**
	 * Test {@link ContractUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnReduceForReduceStub() {
		Class<?> result = ContractUtil.getContractClass(Reducer.class);
		assertEquals(ReduceContract.class, result);
	}

	/**
	 * Test {@link ContractUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnSourceForInputFormat() {
		Class<?> result = ContractUtil.getContractClass(TextInputFormat.class);
		assertEquals(DataSourceContract.class, result);
	}

	/**
	 * Test {@link ContractUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnSinkForOutputFormat() {
		Class<?> result = ContractUtil.getContractClass(TextOutputFormat.class);
		assertEquals(DataSinkContract.class, result);
	}

	/**
	 * Test {@link ContractUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnMatchForMatchStub() {
		Class<?> result = ContractUtil.getContractClass(Matcher.class);
		assertEquals(MatchContract.class, result);
	}

	/**
	 * Test {@link ContractUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnCrossForCrossStub() {
		Class<?> result = ContractUtil.getContractClass(Crosser.class);
		assertEquals(CrossContract.class, result);
	}

	/**
	 * Test {@link ContractUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnCoGroupForCoGroupStub() {
		Class<?> result = ContractUtil.getContractClass(CoGrouper.class);
		assertEquals(CoGroupContract.class, result);
	}

	static class Mapper extends MapStub<Key, Value, Key, Value> {
		@Override
		public void map(Key key, Value value, Collector<Key, Value> out) {
		}
	}

	static class Reducer extends ReduceStub<Key, Value, Key, Value> {
		@Override
		public void reduce(Key key, Iterator<Value> values, Collector<Key, Value> out) {
		}
	}

	static class CoGrouper extends CoGroupStub<Key, Value, Value, Key, Value> {
		@Override
		public void coGroup(Key key, Iterator<Value> values1, Iterator<Value> values2, Collector<Key, Value> out) {
		}
	}

	static class Matcher extends MatchStub<Key, Value, Value, Key, Value> {
		@Override
		public void match(Key key, Value value1, Value value2, Collector<Key, Value> out) {
		}
	}

	static class Crosser extends CrossStub<Key, Value, Key, Value, Key, Value> {
		@Override
		public void cross(Key key1, Value value1, Key key2, Value value2, Collector<Key, Value> out) {
		}
	}
}
