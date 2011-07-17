package eu.stratosphere.sopremo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.junit.Test;

import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.plan.ContractNavigator;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.SopremoCross;
import eu.stratosphere.util.dag.GraphLevelPartitioner;
import eu.stratosphere.util.dag.GraphLevelPartitioner.Level;

/**
 * The class <code>CompositeOperatorTest</code> contains tests for the class
 * <code>{@link CompositeOperator}</code>.
 * 
 * @author Arvid Heise
 */
public class CompositeOperatorTest extends SopremoTest<CompositeOperatorTest.CompositeOperatorImpl> {
	static class CompositeOperatorImpl extends CompositeOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private int index;

		public CompositeOperatorImpl(int index, JsonStream... streams) {
			super(1, streams);
			this.index = index;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = super.hashCode();
			result = prime * result + index;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (!super.equals(obj))
				return false;
			if (!(obj instanceof CompositeOperatorImpl))
				return false;
			CompositeOperatorImpl other = (CompositeOperatorImpl) obj;
			if (index != other.index)
				return false;
			return true;
		}

		@Override
		public SopremoModule asElementaryOperators() {
			return SopremoModule.valueOf(getName(), new ElementaryOperatorImpl(getInput(0),
				new ElementaryOperatorImpl(getInput(1), getInput(2))));
		}

	}

	static class ElementaryOperatorImpl extends ElementaryOperator {
		private static final long serialVersionUID = 1L;

		public ElementaryOperatorImpl(JsonStream stream1, JsonStream stream2) {
			super(stream1, stream2);
		}

		static class Implementation
				extends
				SopremoCross<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject> {
			@Override
			protected void cross(JsonNode key1, JsonNode value1, JsonNode key2, JsonNode value2,
					JsonCollector out) {
			}
		}
	}

	protected CompositeOperatorImpl createDefaultInstance(int index) {
		return new CompositeOperatorImpl(index);
	}

	/**
	 * Run the PactModule asPactModule(EvaluationContext) method test.
	 */
	@Test
	public void testAsPactModule() throws Exception {
		Operator input1 = new Source(PersistenceType.HDFS, "1");
		Operator input2 = new Source(PersistenceType.HDFS, "2");
		Operator input3 = new Source(PersistenceType.HDFS, "3");
		CompositeOperator fixture = new CompositeOperatorImpl(1, input1, input2, input3);
		EvaluationContext context = new EvaluationContext();

		PactModule result = fixture.asPactModule(context);

		assertNotNull(result);
		List<Level<Contract>> reachableNodes = GraphLevelPartitioner.getLevels(
			result.getAllOutputs(), ContractNavigator.INSTANCE);
		assertEquals(3, reachableNodes.get(0).getLevelNodes().size());
		assertEquals(1, reachableNodes.get(1).getLevelNodes().size());
		assertEquals(1, reachableNodes.get(2).getLevelNodes().size());
		assertEquals(1, reachableNodes.get(3).getLevelNodes().size());

		for (int index = 0; index < 3; index++)
			assertTrue(DataSourceContract.class.isInstance(reachableNodes.get(0).getLevelNodes()
				.get(index)));
		assertSame(ElementaryOperatorImpl.Implementation.class, reachableNodes.get(1)
			.getLevelNodes().get(0).getStubClass());
		assertSame(ElementaryOperatorImpl.Implementation.class, reachableNodes.get(2)
			.getLevelNodes().get(0).getStubClass());
		assertTrue(DataSinkContract.class.isInstance(reachableNodes.get(3).getLevelNodes().get(0)));
	}
}