package eu.stratosphere.sopremo.operator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.operator.ElementaryOperator;
import eu.stratosphere.sopremo.operator.ElementarySopremoModule;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.JsonStream;
import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.operator.OutputCardinality;

/**
 * The class <code>OperatorTest</code> contains tests for the class <code>{@link Operator<?>}</code>.
 * 
 * @author Arvid Heise
 */
public class OperatorTest extends SopremoTest<OperatorTest.OpImpl> {
	@Override
	protected OpImpl createDefaultInstance(final int index) {
		return new OpImpl(index);
	}

	/**
	 * Run the Operator<?> clone() method test.
	 */
	@Test
	public void shouldCloneCorrectly() {
		final Operator<?> fixture = new OpImpl(0);

		final Operator<?> result = fixture.clone();

		assertNotNull(result);
		assertEquals(fixture, result);
		assertNotSame(fixture, result);
	}

	/**
	 * Run the void setName(String) method test.
	 */
	@Test
	public void testChangeName() {
		final Operator<?> fixture = new OpImpl(0);
		final String name = "";

		fixture.setName(name);

		assertEquals(name, fixture.getName());
	}

	/**
	 * Run the JsonStream getInput(int) method test.
	 */
	@Test
	public void testGetInput() {
		final Operator<?> input1 = new OpImpl(0);
		final Operator<?> input2 = new OpImpl(1);
		final Operator<?> fixture = new OpImpl(0).withInputs(input1, input2);

		assertSame(input1.getOutput(0), fixture.getInput(0));
		assertSame(input2.getOutput(0), fixture.getInput(1));
	}

	/**
	 * Run the List<Operator<?>> getInputOperators() method test.
	 */
	@Test
	public void testGetInputOperators() {
		final Operator<?> input1 = new OpImpl(0);
		final Operator<?> input2 = new OpImpl(1);
		final Operator<?> fixture = new OpImpl(0).withInputs(input1, input2);

		assertSame(input1, fixture.getInputOperators().get(0));
		assertSame(input2, fixture.getInputOperators().get(1));
	}

	/**
	 * Run the List<JsonStream> getInputs() method test.
	 */
	@Test
	public void testGetInputs() {
		final Operator<?> input1 = new OpImpl(0);
		final Operator<?> input2 = new OpImpl(1);
		final Operator<?> fixture = new OpImpl(0).withInputs(input1, input2);

		final List<JsonStream> result = fixture.getInputs();

		assertNotNull(result);
		assertEquals(2, result.size());

		final List<JsonStream> expectedResults = new ArrayList<JsonStream>();
		expectedResults.add(input1.getOutput(0));
		expectedResults.add(input2.getOutput(0));
		assertEquals(expectedResults, result);
	}

	/**
	 * Run the JsonStream getOutput(int) method test.
	 */
	@Test
	public void testGetOutput() {
		final Operator<?> fixture = new OpImpl(0);

		final JsonStream result = fixture.getOutput(0);

		assertNotNull(result);
		assertEquals(0, result.getSource().getIndex());
	}

	/**
	 * Run the List<JsonStream> getOutputs() method test.
	 */
	@Test
	public void testGetOutputs() {
		final Operator<?> fixture = new OpImpl(0);

		final List<JsonStream> result = fixture.getOutputs();

		assertNotNull(result);
		assertEquals(1, result.size());

		final List<JsonStream> expectedResults = new ArrayList<JsonStream>();
		expectedResults.add(fixture.getOutput(0));
		assertEquals(expectedResults, result);
	}

	/**
	 * Run the JsonStream getSource() method test.
	 */
	@Test
	public void testGetSource() {
		final Operator<?> fixture = new OpImpl(0);

		final JsonStream result = fixture.getSource();

		assertNotNull(result);
		assertEquals(0, result.getSource().getIndex());
		assertSame(fixture.getOutput(0), result);
	}

	/**
	 * Run the void setInputs(JsonStream[]) method test.
	 */
	@Test
	public void testSetArrayInputs() {
		final Operator<?> input1 = new OpImpl(0);
		final Operator<?> input2 = new OpImpl(1);
		final Operator<?> fixture = new OpImpl(0).withInputs(input1, input2);

		final Operator<?> newInput = new OpImpl(2);
		fixture.setInputs(newInput);

		assertEquals(1, fixture.getInputs().size());

		final List<JsonStream> expectedResults = new ArrayList<JsonStream>();
		expectedResults.add(newInput.getOutput(0));
		assertEquals(expectedResults, fixture.getInputs());
	}

	/**
	 * Run the void setInputs(JsonStream[]) method test.
	 */
	@Test
	public void testSetArrayInputsWithNullElement() {
		final Operator<?> input1 = new OpImpl(0);
		final Operator<?> input2 = new OpImpl(1);
		final Operator<?> fixture = new OpImpl(0).withInputs(input1, input2);

		fixture.setInputs((Operator<?>) null);

		assertEquals(1, fixture.getInputs().size());

		final List<JsonStream> expectedResults = new ArrayList<JsonStream>();
		expectedResults.add(null);
		assertEquals(expectedResults, fixture.getInputs());
	}

	/**
	 * Run the void setInput(int,JsonStream) method test.
	 */
	@Test
	public void testSetInput() {
		final Operator<?> input1 = new OpImpl(0);
		final Operator<?> input2 = new OpImpl(1);
		final Operator<?> fixture = new OpImpl(0).withInputs(input1, input2);

		final Operator<?> newInput2 = new OpImpl(2);

		fixture.setInput(1, newInput2);

		assertSame(input1.getOutput(0), fixture.getInput(0));
		assertSame(newInput2.getOutput(0), fixture.getInput(1));
		assertNotSame(input2, newInput2);
	}

	/**
	 * Run the void setInputs(List<? extends JsonStream>) method test.
	 */
	@Test
	public void testSetInputs() {
		final Operator<?> input1 = new OpImpl(0);
		final Operator<?> input2 = new OpImpl(1);
		final Operator<?> fixture = new OpImpl(0).withInputs(input1, input2);

		final Operator<?> newInput = new OpImpl(2);
		final List<JsonStream> inputs = new ArrayList<JsonStream>();
		inputs.add(newInput);
		fixture.setInputs(inputs);

		assertEquals(1, fixture.getInputs().size());
		final List<JsonStream> expectedResults = new ArrayList<JsonStream>();
		expectedResults.add(newInput.getOutput(0));
		assertEquals(expectedResults, fixture.getInputs());
	}

	/**
	 * Run the void setInputs(List<? extends JsonStream>) method test.
	 */
	@Test(expected = java.lang.NullPointerException.class)
	public void testSetInputsWithNull() {
		@SuppressWarnings({ "serial", "rawtypes" })
		final Operator<?> fixture = new ElementaryOperator(1, 1) {
		};
		final List<? extends JsonStream> inputs = null;

		fixture.setInputs(inputs);
	}

	/**
	 * Run the void setInputs(List<? extends JsonStream>) method test.
	 */
	@Test
	public void testSetInputsWithNullElement() {
		final Operator<?> input1 = new OpImpl(0);
		final Operator<?> input2 = new OpImpl(1);
		final Operator<?> fixture = new OpImpl(0).withInputs(input1, input2);

		final List<JsonStream> newInputs = new ArrayList<JsonStream>();
		newInputs.add(null);
		fixture.setInputs(newInputs);

		assertEquals(1, fixture.getInputs().size());
		final List<JsonStream> expectedInputs = new ArrayList<JsonStream>();
		expectedInputs.add(null);
		assertEquals(expectedInputs, fixture.getInputs());
	}

	/**
	 * Run the void setInput(int,JsonStream) method test.
	 */
	@Test
	public void testSetInputWithNullElements() {
		final Operator<?> input1 = new OpImpl(0);
		final Operator<?> input2 = new OpImpl(1);
		final Operator<?> fixture = new OpImpl(0).withInputs(input1, input2);

		fixture.setInput(0, null);

		assertNull(fixture.getInput(0));
		assertNotNull(fixture.getInput(1));
	}

	/**
	 * Run the void setName(String) method test.
	 */
	@Test(expected = java.lang.NullPointerException.class)
	public void testSetNameWithNull() {
		final Operator<?> fixture = new OpImpl(0);
		final String name = null;

		fixture.setName(name);
	}

	@InputCardinality(min = 1, max = 2)
	@OutputCardinality(1)
	static class OpImpl extends Operator<OpImpl> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 612400796674192242L;

		private final int index;

		public OpImpl(final int index) {
			this.index = index;
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.Operator#asElementaryOperators()
		 */
		@Override
		public ElementarySopremoModule asElementaryOperators(final EvaluationContext context) {
			return null;
		}

		@Override
		public PactModule asPactModule(final EvaluationContext context) {
			return null;
		}

		@Override
		public boolean equals(final Object obj) {
			if (this == obj)
				return true;
			if (!super.equals(obj))
				return false;
			if (!(obj instanceof OpImpl))
				return false;
			final OpImpl other = (OpImpl) obj;
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

}