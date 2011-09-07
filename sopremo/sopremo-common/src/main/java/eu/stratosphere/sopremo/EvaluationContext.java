package eu.stratosphere.sopremo;

import java.util.Iterator;
import java.util.LinkedList;

import eu.stratosphere.sopremo.function.FunctionRegistry;

/**
 * Provides additional context to the evaluation of {@link Evaluable}s, such as access to all registered functions.
 * 
 * @author Arvid Heise
 */
public class EvaluationContext implements SerializableSopremoType {
	private static final long serialVersionUID = 7701485388451926506L;

	private final FunctionRegistry functionRegistry;

	private int inputCounter = 0;

	private LinkedList<String> operatorStack = new LinkedList<String>();

	public LinkedList<String> getOperatorStack() {
		return this.operatorStack;
	}

	public String operatorTrace() {
		final Iterator<String> descendingIterator = operatorStack.descendingIterator();
		final StringBuilder builder = new StringBuilder(descendingIterator.next());
		while (descendingIterator.hasNext())
			builder.append("->").append(descendingIterator.next());
		return builder.toString();
	}

	public void pushOperator(String e) {
		this.operatorStack.push(e);
	}

	public String popOperator() {
		return this.operatorStack.pop();
	}

	public EvaluationContext() {
		this.functionRegistry = new FunctionRegistry();
	}

	public EvaluationContext(final EvaluationContext context) {
		this.functionRegistry = context.functionRegistry;
		this.inputCounter = context.inputCounter;
	}

	/**
	 * Returns the {@link FunctionRegistry} containing all registered function in the current evaluation context.
	 * 
	 * @return the FunctionRegistry
	 */
	public FunctionRegistry getFunctionRegistry() {
		return this.functionRegistry;
	}

	public int getInputCounter() {
		return this.inputCounter;
	}

	public void increaseInputCounter() {
		this.inputCounter++;
	}

	private int taskId;

	public int getTaskId() {
		return taskId;
	}

	public void setTaskId(int taskId) {
		this.taskId = taskId;
	}
	
	
}
