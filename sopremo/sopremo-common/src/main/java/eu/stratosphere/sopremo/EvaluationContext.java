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

	private final Bindings bindings = new Bindings();

	private final FunctionRegistry functionRegistry;

	private int inputCounter = 0;

	private final LinkedList<String> operatorStack = new LinkedList<String>();

	public LinkedList<String> getOperatorStack() {
		return this.operatorStack;
	}

	public Object getBinding(String name) {
		return this.bindings.get(name);
	}

	public <T> T getBinding(String name, Class<T> expectedType) {
		return this.bindings.get(name, expectedType);
	}

	public <T> T getNonNullBinding(String name, Class<T> expectedType) {
		return this.bindings.getNonNull(name, expectedType);
	}

	public void addScope() {
		this.bindings.addScope();
	}

	public void removeScope() {
		this.bindings.removeScope();
	}

	public void setBinding(String name, Object binding) {
		this.bindings.set(name, binding);
	}

	public String operatorTrace() {
		final Iterator<String> descendingIterator = this.operatorStack.descendingIterator();
		final StringBuilder builder = new StringBuilder(descendingIterator.next());
		while (descendingIterator.hasNext())
			builder.append("->").append(descendingIterator.next());
		return builder.toString();
	}

	public void pushOperator(final String e) {
		this.operatorStack.push(e);
	}

	public String popOperator() {
		return this.operatorStack.pop();
	}

	public EvaluationContext() {
		this.functionRegistry = new FunctionRegistry(this.bindings);
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
		return this.taskId;
	}

	public void setTaskId(final int taskId) {
		this.taskId = taskId;
	}

}
