package eu.stratosphere.sopremo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import eu.stratosphere.pact.common.plan.PactModule;

/**
 * The class <code>OperatorTest</code> contains tests for the class
 * <code>{@link Operator}</code>.
 * 
 * @author Arvid Heise
 */
public class OperatorTest extends SopremoTest<OperatorTest.OpImpl> {
	static class OpImpl extends Operator {
		/**
		 * 
		 */
		private static final long serialVersionUID = 612400796674192242L;

		private int index;

		public OpImpl(int index, JsonStream... streams) {
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
			if (!(obj instanceof OpImpl))
				return false;
			OpImpl other = (OpImpl) obj;
			if (index != other.index)
				return false;
			return true;
		}

		@Override
		public PactModule asPactModule(EvaluationContext context) {
			// TODO Auto-generated method stub
			return null;
		}
	}

	/**
	 * Run the Operator clone() method test.
	 */
	@Test
	public void shouldCloneCorrectly() {
		Operator fixture = new OpImpl(0);

		Operator result = fixture.clone();

		assertNotNull(result);
		assertEquals(fixture, result);
		assertNotSame(fixture, result);
	}

	@Override
	protected OpImpl createDefaultInstance(int index) {
		return new OpImpl(index);
	}

	/**
	 * Run the Operator.Output getInput(int) method test.
	 */
	@Test
	public void testGetInput() {
		Operator input1 = new OpImpl(0);
		Operator input2 = new OpImpl(1);
		Operator fixture = new OpImpl(0, input1, input2);

		assertSame(input1.getOutput(0), fixture.getInput(0));
		assertSame(input2.getOutput(0), fixture.getInput(1));
	}

	/**
	 * Run the List<Operator> getInputOperators() method test.
	 */
	@Test
	public void testGetInputOperators() {
		Operator input1 = new OpImpl(0);
		Operator input2 = new OpImpl(1);
		Operator fixture = new OpImpl(0, input1, input2);

		assertSame(input1, fixture.getInputOperators().get(0));
		assertSame(input2, fixture.getInputOperators().get(1));
	}

	/**
	 * Run the List<Operator.Output> getInputs() method test.
	 */
	@Test
	public void testGetInputs() {
		Operator input1 = new OpImpl(0);
		Operator input2 = new OpImpl(1);
		Operator fixture = new OpImpl(0, input1, input2);

		List<Operator.Output> result = fixture.getInputs();

		
		assertNotNull(result);
		assertEquals(2, result.size());
		assertEquals(Arrays.asList(input1.getOutput(0), input2.getOutput(0)), result);
	}

	/**
	 * Run the Operator.Output getOutput(int) method test.
	 */
	@Test
	public void testGetOutput() {
		Operator fixture = new OpImpl(0);

		Operator.Output result = fixture.getOutput(0);

		assertNotNull(result);
		assertEquals(0, result.getIndex());
	}

	/**
	 * Run the List<Operator.Output> getOutputs() method test.
	 */
	@Test
	public void testGetOutputs() {
		Operator fixture = new OpImpl(0);

		List<Operator.Output> result = fixture.getOutputs();

		
		assertNotNull(result);
		assertEquals(1, result.size());
		assertEquals(Arrays.asList(fixture.getOutput(0)), result);
	}

	/**
	 * Run the Operator.Output getSource() method test.
	 */
	@Test
	public void testGetSource() {
		Operator fixture = new OpImpl(0);

		Operator.Output result = fixture.getSource();

		
		assertNotNull(result);
		assertEquals(0, result.getIndex());
		assertSame(fixture.getOutput(0), result);
	}

	/**
	 * Run the void setInput(int,JsonStream) method test.
	 */
	@Test
	public void testSetInput() {
		Operator input1 = new OpImpl(0);
		Operator input2 = new OpImpl(1);
		Operator fixture = new OpImpl(0, input1, input2);

		Operator newInput2 = new OpImpl(2);

		fixture.setInput(1, newInput2);

		assertSame(input1.getOutput(0), fixture.getInput(0));
		assertSame(newInput2.getOutput(0), fixture.getInput(1));
		assertNotSame(input2, newInput2);
	}

	/**
	 * Run the void setInput(int,JsonStream) method test.
	 */
	@Test
	public void testSetInputWithNullElements() {
		Operator input1 = new OpImpl(0);
		Operator input2 = new OpImpl(1);
		Operator fixture = new OpImpl(0, input1, input2);

		fixture.setInput(0, null);

		assertNull(fixture.getInput(0));
		assertNotNull(fixture.getInput(1));
	}

	/**
	 * Run the void setInputs(List<? extends JsonStream>) method test.
	 */
	@Test
	public void testSetInputs() {
		Operator input1 = new OpImpl(0);
		Operator input2 = new OpImpl(1);
		Operator fixture = new OpImpl(0, input1, input2);

		Operator newInput = new OpImpl(2);
		fixture.setInputs(Arrays.asList(newInput));

		assertEquals(1, fixture.getInputs().size());
		assertEquals(Arrays.asList(newInput.getOutput(0)), fixture.getInputs());
	}

	/**
	 * Run the void setInputs(List<? extends JsonStream>) method test.
	 */
	@Test
	public void testSetInputsWithNullElement() {
		Operator input1 = new OpImpl(0);
		Operator input2 = new OpImpl(1);
		Operator fixture = new OpImpl(0, input1, input2);

		fixture.setInputs(Arrays.asList((Operator) null));

		assertEquals(1, fixture.getInputs().size());
		assertEquals(Arrays.asList((Operator) null), fixture.getInputs());
	}


	/**
	 * Run the void setInputs(List<? extends JsonStream>) method test.
	 */
	@Test(expected = java.lang.NullPointerException.class)
	public void testSetInputsWithNull() {
		Operator fixture = new ElementaryOperator(new JsonStream[] {});
		List<? extends JsonStream> inputs = null;

		fixture.setInputs(inputs);
	}

	/**
	 * Run the void setInputs(JsonStream[]) method test.
	 */
	@Test
	public void testSetArrayInputsWithNullElement() {
		Operator input1 = new OpImpl(0);
		Operator input2 = new OpImpl(1);
		Operator fixture = new OpImpl(0, input1, input2);

		fixture.setInputs((Operator) null);

		assertEquals(1, fixture.getInputs().size());
		assertEquals(Arrays.asList((Operator) null), fixture.getInputs());
	}

	/**
	 * Run the void setInputs(JsonStream[]) method test.
	 */
	@Test
	public void testSetArrayInputs() {
		Operator input1 = new OpImpl(0);
		Operator input2 = new OpImpl(1);
		Operator fixture = new OpImpl(0, input1, input2);

		Operator newInput = new OpImpl(2);
		fixture.setInputs(newInput);

		assertEquals(1, fixture.getInputs().size());
		assertEquals(Arrays.asList(newInput.getOutput(0)), fixture.getInputs());
	}

	/**
	 * Run the void setName(String) method test.
	 */
	@Test
	public void testChangeName() {
		Operator fixture = new OpImpl(0);
		String name = "";

		fixture.setName(name);

		assertEquals(name, fixture.getName());
	}

	/**
	 * Run the void setName(String) method test.
	 */
	@Test(expected = java.lang.NullPointerException.class)
	public void testSetNameWithNull() {
		Operator fixture = new OpImpl(0);
		String name = null;

		fixture.setName(name);
	}

}