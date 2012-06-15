package eu.stratosphere.sopremo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.plan.ContractNavigator;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoCross;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.util.dag.GraphLevelPartitioner;
import eu.stratosphere.util.dag.GraphLevelPartitioner.Level;

/**
 * The class <code>CompositeOperatorTest</code> contains tests for the class <code>{@link CompositeOperator<?>}</code>.
 * 
 * @author Arvid Heise
 */
public class CompositeOperatorTest extends SopremoTest<CompositeOperatorTest.CompositeOperatorImpl> {
	@Override
	protected CompositeOperatorImpl createDefaultInstance(final int index) {
		return new CompositeOperatorImpl(index);
	}

	/**
	 * Run the PactModule asPactModule(EvaluationContext) method test.
	 */
	@Test
	public void testAsPactModule() throws Exception {
		final Operator<?> input1 = new Source("file://1");
		final Operator<?> input2 = new Source("file://2");
		final Operator<?> input3 = new Source("file://3");
		final CompositeOperator<?> fixture = new CompositeOperatorImpl(1);
		fixture.setInputs(input1, input2, input3);
		final EvaluationContext context = new EvaluationContext();

		final PactModule result = fixture.asPactModule(context);

		assertNotNull(result);
		final List<Level<Contract>> reachableNodes = GraphLevelPartitioner.getLevels(
			result.getAllOutputs(), ContractNavigator.INSTANCE);
		assertEquals(3, reachableNodes.get(0).getLevelNodes().size());
		assertEquals(1, reachableNodes.get(1).getLevelNodes().size());
		assertEquals(1, reachableNodes.get(2).getLevelNodes().size());
		assertEquals(1, reachableNodes.get(3).getLevelNodes().size());

		for (int index = 0; index < 3; index++)
			assertTrue(FileDataSource.class.isInstance(reachableNodes.get(0).getLevelNodes()
				.get(index)));
		assertSame(ElementaryOperatorImpl.Implementation.class, reachableNodes.get(1)
			.getLevelNodes().get(0).getUserCodeClass());
		assertSame(ElementaryOperatorImpl.Implementation.class, reachableNodes.get(2)
			.getLevelNodes().get(0).getUserCodeClass());
		assertTrue(FileDataSink.class.isInstance(reachableNodes.get(3).getLevelNodes().get(0)));
	}

	static class CompositeOperatorImpl extends CompositeOperator<CompositeOperatorImpl> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		private final int index;

		public CompositeOperatorImpl(final int index) {
			super(3, 1);
			this.index = index;
		}

		@Override
		public ElementarySopremoModule asElementaryOperators() {
			return ElementarySopremoModule.valueOf(this.getName(),
				new ElementaryOperatorImpl().withInputs(getInput(0),
					new ElementaryOperatorImpl().withInputs(getInput(1), getInput(2))));
		}

		@Override
		public boolean equals(final Object obj) {
			if (this == obj)
				return true;
			if (!super.equals(obj))
				return false;
			if (!(obj instanceof CompositeOperatorImpl))
				return false;
			final CompositeOperatorImpl other = (CompositeOperatorImpl) obj;
			if (this.index != other.index)
				return false;
			return true;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = super.hashCode();
			result = prime * result + this.index;
			return result;
		}

	}

	@InputCardinality(min = 2, max = 2)
	static class ElementaryOperatorImpl extends ElementaryOperator<ElementaryOperatorImpl> {
		private static final long serialVersionUID = 1L;

		static class Implementation extends SopremoCross {
			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.SopremoCross#cross(eu.stratosphere.sopremo.type.IJsonNode,
			 * eu.stratosphere.sopremo.type.IJsonNode, eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void cross(final IJsonNode value1, final IJsonNode value2, final JsonCollector out) {
			}
		}
	}
}