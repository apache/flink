package eu.stratosphere.sopremo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

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
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.pact.SopremoReduce;
import eu.stratosphere.sopremo.serialization.ObjectSchema;
import eu.stratosphere.sopremo.serialization.Schema;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * The class <code>ElementaryOperatorTest</code> contains tests for the class <code>{@link ElementaryOperator}</code>.
 * 
 * @author Arvid Heise
 */
@RunWith(PowerMockRunner.class)
public class ElementaryOperatorTest {
	/**
	 * 
	 */
	private static final Schema SCHEMA = new ObjectSchema();

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test(expected = IllegalStateException.class)
	@PrepareForTest(ContractUtil.class)
	public void getContractShouldFailIfContractNotInstancable() {
		PowerMockito.mockStatic(ContractUtil.class);
		Mockito.when(ContractUtil.getContractClass(OperatorWithOneStub.Implementation.class)).thenReturn(
			(Class) UninstanceableContract.class);
		new OperatorWithOneStub().getContract(SCHEMA);
	}

	@Test(expected = IllegalStateException.class)
	public void getContractShouldFailIfNoStub() {
		new OperatorWithNoStubs().getContract(SCHEMA);
	}

	@Test(expected = IllegalStateException.class)
	public void getContractShouldFailIfOnlyInstanceStub() {
		new OperatorWithInstanceStub().getContract(SCHEMA);
	}

	@Test(expected = IllegalStateException.class)
	public void getContractShouldFailIfOnlyUnknownStub() {
		new OperatorWithUnknownStub().getContract(SCHEMA);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void getContractShouldReturnTheMatchingContractToTheFirstStub() {
		ObjectSchema schema = new ObjectSchema();
		schema.setMappings("someField");
		final Contract contract = new OperatorWithTwoStubs().getContract(schema);
		assertEquals(ReduceContract.class, contract.getClass());
		assertTrue(Arrays.asList(OperatorWithTwoStubs.Implementation1.class,
			OperatorWithTwoStubs.Implementation2.class).contains(contract.getUserCodeClass()));
	}

	@Test
	public void getContractShouldReturnTheMatchingContractToTheOnlyStub() {
		final Contract contract = new OperatorWithOneStub().getContract(SCHEMA);
		assertEquals(MapContract.class, contract.getClass());
		assertEquals(OperatorWithOneStub.Implementation.class, contract.getUserCodeClass());
	}

	@SuppressWarnings({ "serial", "rawtypes" })
	public ElementaryOperator<?> getDefault() {
		return new ElementaryOperator() {
		};
	}

	@Test
	public void getStubClassShouldReturnNullIfNoStub() {
		assertEquals(null, new OperatorWithNoStubs().getStubClass());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void getStubClassShouldReturnTheFirstStub() {
		final Class<? extends eu.stratosphere.pact.common.stubs.Stub> stubClass = new OperatorWithTwoStubs()
			.getStubClass();
		assertEquals(OperatorWithTwoStubs.class, stubClass.getDeclaringClass());
		assertTrue(Arrays.asList(OperatorWithTwoStubs.Implementation1.class,
			OperatorWithTwoStubs.Implementation2.class).contains(stubClass));
	}

	@Test
	public void getStubClassShouldReturnTheOnlyStub() {
		assertEquals(OperatorWithOneStub.Implementation.class,
			new OperatorWithOneStub().getStubClass());
	}

	static class OperatorWithInstanceStub extends ElementaryOperator<OperatorWithInstanceStub> {
		private static final long serialVersionUID = 1L;

		class Implementation extends SopremoMap {
			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.SopremoMap#map(eu.stratosphere.sopremo.type.IJsonNode,
			 * eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void map(final IJsonNode value, final JsonCollector out) {
			}
		}
	}

	static class OperatorWithNoStubs extends ElementaryOperator<OperatorWithNoStubs> {
		private static final long serialVersionUID = 1L;
	}

	static class OperatorWithOneStub extends ElementaryOperator<OperatorWithOneStub> {
		private static final long serialVersionUID = 1L;

		static class Implementation extends SopremoMap {
			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.SopremoMap#map(eu.stratosphere.sopremo.type.IJsonNode,
			 * eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void map(final IJsonNode value, final JsonCollector out) {
			}
		}
	}

	static class OperatorWithTwoStubs extends ElementaryOperator<OperatorWithTwoStubs> {
		private static final long serialVersionUID = 1L;

		/* (non-Javadoc)
		 * @see eu.stratosphere.sopremo.ElementaryOperator#getKeyExpressions()
		 */
		@Override
		public Iterable<? extends EvaluationExpression> getKeyExpressions() {
			return Arrays.asList(new ObjectAccess("someField"));
		}
		
		static class Implementation1 extends SopremoReduce {
			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.SopremoReduce#reduce(eu.stratosphere.sopremo.type.ArrayNode,
			 * eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void reduce(final IArrayNode values, final JsonCollector out) {
			}
		}

		static class Implementation2 extends SopremoReduce {
			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.SopremoReduce#reduce(eu.stratosphere.sopremo.type.ArrayNode,
			 * eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void reduce(final IArrayNode values, final JsonCollector out) {
			}
		}
	}

	static class OperatorWithUnknownStub extends ElementaryOperator<OperatorWithUnknownStub> {
		private static final long serialVersionUID = 1L;

		static class Implementation extends Stub {
		}
	}

	static class UninstanceableContract extends SingleInputContract<Stub> {

		public UninstanceableContract(final Class<? extends Stub> clazz, final String name) {
			super(clazz, name);
			throw new IllegalStateException("not instanceable");
		}

	}
}
