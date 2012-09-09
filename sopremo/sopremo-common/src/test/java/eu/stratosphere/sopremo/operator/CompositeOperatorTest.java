package eu.stratosphere.sopremo.operator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

import java.util.List;

import org.junit.Test;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.io.Sink;
import eu.stratosphere.sopremo.io.Source;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoCross;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.util.dag.GraphLevelPartitioner;
import eu.stratosphere.util.dag.GraphLevelPartitioner.Level;

/**
 * The class <code>CompositeOperatorTest</code> contains tests for the class
 * <code>{@link CompositeOperator<?>}</code>.
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
	public void testAsElementaryOperators() throws Exception {
		final Operator<?> input1 = new Source("file://1");
		final Operator<?> input2 = new Source("file://2");
		final Operator<?> input3 = new Source("file://3");
		final CompositeOperator<?> fixture = new CompositeOperatorImpl(1);
		fixture.setInputs(input1, input2, input3);
		final EvaluationContext context = new EvaluationContext();

		final ElementarySopremoModule module = fixture.asElementaryOperators(context);

		assertNotNull(module);
		final List<Level<Operator<?>>> reachableNodes = GraphLevelPartitioner.getLevels(
				module.getAllOutputs(), OperatorNavigator.INSTANCE);
		assertEquals(3, reachableNodes.get(0).getLevelNodes().size());
		assertEquals(1, reachableNodes.get(1).getLevelNodes().size());
		assertEquals(1, reachableNodes.get(2).getLevelNodes().size());
		assertEquals(1, reachableNodes.get(3).getLevelNodes().size());

		for (int index = 0; index < 3; index++)
			assertSame(Source.class, reachableNodes.get(0).getLevelNodes().get(index).getClass());
		assertSame(ElementaryOperatorImpl.class, reachableNodes.get(1)
				.getLevelNodes().get(0).getClass());
		assertSame(ElementaryOperatorImpl.class, reachableNodes.get(2)
				.getLevelNodes().get(0).getClass());
		assertSame(Sink.class, reachableNodes.get(3).getLevelNodes().get(0).getClass());
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

		/*
		 * (non-Javadoc)
		 * 
		 * @see eu.stratosphere.sopremo.operator.CompositeOperator#asModule(eu.
		 * stratosphere.sopremo.EvaluationContext)
		 */
		@Override
		public void addImplementation(SopremoModule module, EvaluationContext context) {
			module.embed(new ElementaryOperatorImpl().withInputs(null,
					new ElementaryOperatorImpl().withInputs(null, null)));
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
			 * 
			 * @see
			 * eu.stratosphere.sopremo.pact.SopremoCross#cross(eu.stratosphere
			 * .sopremo.type.IJsonNode, eu.stratosphere.sopremo.type.IJsonNode,
			 * eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void cross(final IJsonNode value1, final IJsonNode value2, final JsonCollector out) {
			}
		}
	}
}