package eu.stratosphere.sopremo;

import java.util.Iterator;
import java.util.LinkedList;

import eu.stratosphere.sopremo.function.MethodRegistry;

/**
 * Provides additional context to the evaluation of {@link Evaluable}s, such as access to all registered functions.
 * 
 * @author Arvid Heise
 */
public class EvaluationContext extends AbstractSopremoType implements SerializableSopremoType {
	private static final long serialVersionUID = 7701485388451926506L;

	private final Bindings bindings = new Bindings();

	private final MethodRegistry functionRegistry;

	private int inputCounter = 0;

	private final LinkedList<Operator<?>> operatorStack = new LinkedList<Operator<?>>();

	public LinkedList<Operator<?>> getOperatorStack() {
		return this.operatorStack;
	}

	public Bindings getBindings() {
		return this.bindings;
	}

	public void addScope() {
		this.bindings.addScope();
	}

	public void removeScope() {
		this.bindings.removeScope();
	}

	public String operatorTrace() {
		final Iterator<Operator<?>> descendingIterator = this.operatorStack.descendingIterator();
		final StringBuilder builder = new StringBuilder(descendingIterator.next().getName());
		while (descendingIterator.hasNext())
			builder.append("->").append(descendingIterator.next().getName());
		return builder.toString();
	}

	public Operator<?> getCurrentOperator() {
		return this.operatorStack.peek();
	}
	
	public void pushOperator(final Operator<?> e) {
		this.operatorStack.push(e);
	}

	public Operator<?> popOperator() {
		return this.operatorStack.pop();
	}

	public EvaluationContext() {
		this.functionRegistry = new MethodRegistry(this.bindings);
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
	public MethodRegistry getFunctionRegistry() {
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

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.SopremoType#toString(java.lang.StringBuilder)
	 */
	@Override
	public void toString(StringBuilder builder) {
		builder.append("Context @ ").append(this.operatorStack).append("\n").
			append("Bindings: ");
		this.bindings.toString(builder);
	}
}
