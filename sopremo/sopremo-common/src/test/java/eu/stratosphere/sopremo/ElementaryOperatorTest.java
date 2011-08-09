package eu.stratosphere.sopremo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Iterator;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.SingleInputContract;
import eu.stratosphere.pact.common.plan.ContractUtil;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.stub.SingleInputStub;
import eu.stratosphere.pact.common.stub.Stub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;

/**
 * The class <code>ElementaryOperatorTest</code> contains tests for the class <code>{@link ElementaryOperator}</code>.
 * 
 * @author Arvid Heise
 */
@RunWith(PowerMockRunner.class)
public class ElementaryOperatorTest {
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test(expected = IllegalStateException.class)
	@PrepareForTest(ContractUtil.class)
	public void getContractShouldFailIfContractNotInstancable() {
		PowerMockito.mockStatic(ContractUtil.class);
		Mockito.when(ContractUtil.getContractClass(OperatorWithOneStub.Implementation.class)).thenReturn(
			(Class) UninstanceableContract.class);
		new OperatorWithOneStub().getContract();
	}

	@Test(expected = IllegalStateException.class)
	public void getContractShouldFailIfNoStub() {
		new OperatorWithNoStubs().getContract();
	}

	@Test(expected = IllegalStateException.class)
	public void getContractShouldFailIfOnlyInstanceStub() {
		new OperatorWithInstanceStub().getContract();
	}

	@Test(expected = IllegalStateException.class)
	public void getContractShouldFailIfOnlyUnknownStub() {
		new OperatorWithUnknownStub().getContract();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void getContractShouldReturnTheMatchingContractToTheFirstStub() {
		final Contract contract = new OperatorWithTwoStubs().getContract();
		assertEquals(ReduceContract.class, contract.getClass());
		assertTrue(Arrays.asList(OperatorWithTwoStubs.Implementation1.class,
			OperatorWithTwoStubs.Implementation2.class).contains(contract.getUserCodeClass()));
	}

	@Test
	public void getContractShouldReturnTheMatchingContractToTheOnlyStub() {
		final Contract contract = new OperatorWithOneStub().getContract();
		assertEquals(MapContract.class, contract.getClass());
		assertEquals(OperatorWithOneStub.Implementation.class, contract.getUserCodeClass());
	}

	public ElementaryOperator getDefault() {
		return new ElementaryOperator();
	}

	@Test
	public void getStubClassShouldReturnNullIfNoStub() {
		assertEquals(null, new OperatorWithNoStubs().getStubClass());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void getStubClassShouldReturnTheFirstStub() {
		final Class<? extends Stub<?, ?>> stubClass = new OperatorWithTwoStubs().getStubClass();
		assertEquals(OperatorWithTwoStubs.class, stubClass.getDeclaringClass());
		assertTrue(Arrays.asList(OperatorWithTwoStubs.Implementation1.class,
			OperatorWithTwoStubs.Implementation2.class).contains(stubClass));
	}

	@Test
	public void getStubClassShouldReturnTheOnlyStub() {
		assertEquals(OperatorWithOneStub.Implementation.class,
			new OperatorWithOneStub().getStubClass());
	}

	static class OperatorWithInstanceStub extends ElementaryOperator {
		private static final long serialVersionUID = 1L;

		class Implementation extends MapStub<Key, Value, Key, Value> {
			@Override
			public void map(final Key key, final Value value, final Collector<Key, Value> out) {
			}
		}
	}

	static class OperatorWithNoStubs extends ElementaryOperator {
		private static final long serialVersionUID = 1L;
	}

	static class OperatorWithOneStub extends ElementaryOperator {
		private static final long serialVersionUID = 1L;

		static class Implementation extends MapStub<Key, Value, Key, Value> {
			@Override
			public void map(final Key key, final Value value, final Collector<Key, Value> out) {
			}
		}
	}

	static class OperatorWithTwoStubs extends ElementaryOperator {
		private static final long serialVersionUID = 1L;

		static class Implementation1 extends ReduceStub<Key, Value, Key, Value> {
			@Override
			public void reduce(final Key key, final Iterator<Value> values, final Collector<Key, Value> out) {
			}
		}

		static class Implementation2 extends ReduceStub<Key, Value, Key, Value> {
			@Override
			public void reduce(final Key key, final Iterator<Value> values, final Collector<Key, Value> out) {
			}
		}
	}

	static class OperatorWithUnknownStub extends ElementaryOperator {
		private static final long serialVersionUID = 1L;

		static class Implementation extends SingleInputStub<Key, Value, Key, Value> {
		}
	}

	static class UninstanceableContract extends SingleInputContract<Key, Value, Key, Value> {

		public UninstanceableContract(
				final Class<? extends SingleInputStub<Key, Value, Key, Value>> clazz, final String name) {
			super(clazz, name);
			throw new IllegalStateException("not instanceable");
		}

	}
}
