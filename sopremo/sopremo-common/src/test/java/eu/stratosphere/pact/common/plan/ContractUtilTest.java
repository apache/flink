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
	public void getContractClassShouldReturnCoGroupForCoGroupStub() {
		final Class<?> result = ContractUtil.getContractClass(CoGrouper.class);
		assertEquals(CoGroupContract.class, result);
	}

	/**
	 * Test {@link ContractUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnCrossForCrossStub() {
		final Class<?> result = ContractUtil.getContractClass(Crosser.class);
		assertEquals(CrossContract.class, result);
	}

	/**
	 * Test {@link ContractUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnMapForMapStub() {
		final Class<?> result = ContractUtil.getContractClass(Mapper.class);
		assertEquals(MapContract.class, result);
	}

	/**
	 * Test {@link ContractUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnMatchForMatchStub() {
		final Class<?> result = ContractUtil.getContractClass(Matcher.class);
		assertEquals(MatchContract.class, result);
	}

	/**
	 * Test {@link ContractUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnNullForStub() {
		final Class<?> result = ContractUtil.getContractClass(Stub.class);
		assertEquals(null, result);
	}

	/**
	 * Test {@link ContractUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnReduceForReduceStub() {
		final Class<?> result = ContractUtil.getContractClass(Reducer.class);
		assertEquals(ReduceContract.class, result);
	}

	/**
	 * Test {@link ContractUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnSinkForOutputFormat() {
		final Class<?> result = ContractUtil.getContractClass(TextOutputFormat.class);
		assertEquals(DataSinkContract.class, result);
	}

	/**
	 * Test {@link ContractUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnSourceForInputFormat() {
		final Class<?> result = ContractUtil.getContractClass(TextInputFormat.class);
		assertEquals(DataSourceContract.class, result);
	}

	static class CoGrouper extends CoGroupStub<Key, Value, Value, Key, Value> {
		@Override
		public void coGroup(final Key key, final Iterator<Value> values1, final Iterator<Value> values2,
				final Collector<Key, Value> out) {
		}
	}

	static class Crosser extends CrossStub<Key, Value, Key, Value, Key, Value> {
		@Override
		public void cross(final Key key1, final Value value1, final Key key2, final Value value2,
				final Collector<Key, Value> out) {
		}
	}

	static class Mapper extends MapStub<Key, Value, Key, Value> {
		@Override
		public void map(final Key key, final Value value, final Collector<Key, Value> out) {
		}
	}

	static class Matcher extends MatchStub<Key, Value, Value, Key, Value> {
		@Override
		public void match(final Key key, final Value value1, final Value value2, final Collector<Key, Value> out) {
		}
	}

	static class Reducer extends ReduceStub<Key, Value, Key, Value> {
		@Override
		public void reduce(final Key key, final Iterator<Value> values, final Collector<Key, Value> out) {
		}
	}
}
