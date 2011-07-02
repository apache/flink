package eu.stratosphere.sopremo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Iterator;

import org.junit.Test;

import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.stub.Stub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;

public class ElementaryOperatorTest {
	public ElementaryOperator getDefault() {
		return new ElementaryOperator();
	}

	@Test
	public void getStubClassShouldReturnNullIfNoStub() {
		assertEquals(null, new OperatorWithNoStubs().getStubClass());
	}

	@Test
	public void getStubClassShouldReturnTheOnlyStub() {
		assertEquals(OperatorWithOneStub.Implementation.class, new OperatorWithOneStub().getStubClass());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void getStubClassShouldReturnTheFirstStub() {
		Class<? extends Stub<?, ?>> stubClass = new OperatorWithTwoStubs().getStubClass();
		assertEquals(OperatorWithTwoStubs.class, stubClass.getDeclaringClass());
		assertTrue(Arrays.asList(OperatorWithTwoStubs.Implementation1.class,
			OperatorWithTwoStubs.Implementation2.class).contains(stubClass));
	}

	@Test(expected = IllegalStateException.class)
	public void getContractShouldFailIfNoStub() {
		new OperatorWithNoStubs().getContract();
	}

	@Test
	public void getContractShouldReturnTheMatchingContractToTheOnlyStub() {
		Contract contract = new OperatorWithOneStub().getContract();
		assertEquals(MapContract.class, contract.getClass());
		assertEquals(OperatorWithOneStub.Implementation.class, contract.getStubClass());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void getContractShouldReturnTheMatchingContractToTheFirstStub() {
		Contract contract = new OperatorWithTwoStubs().getContract();
		assertEquals(ReduceContract.class, contract.getClass());
		assertTrue(Arrays.asList(OperatorWithTwoStubs.Implementation1.class,
			OperatorWithTwoStubs.Implementation2.class).contains(contract.getStubClass()));
	}

	static class OperatorWithNoStubs extends ElementaryOperator {
		private static final long serialVersionUID = 1L;
	}

	static class OperatorWithOneStub extends ElementaryOperator {
		private static final long serialVersionUID = 1L;

		static class Implementation extends MapStub<Key, Value, Key, Value> {
			@Override
			public void map(Key key, Value value, Collector<Key, Value> out) {
			}
		}
	}

	static class OperatorWithTwoStubs extends ElementaryOperator {
		private static final long serialVersionUID = 1L;

		static class Implementation1 extends ReduceStub<Key, Value, Key, Value> {
			@Override
			public void reduce(Key key, Iterator<Value> values, Collector<Key, Value> out) {
			}
		}

		static class Implementation2 extends ReduceStub<Key, Value, Key, Value> {
			@Override
			public void reduce(Key key, Iterator<Value> values, Collector<Key, Value> out) {
			}
		}
	}
}
