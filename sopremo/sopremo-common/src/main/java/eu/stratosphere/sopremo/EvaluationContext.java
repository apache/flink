package eu.stratosphere.sopremo;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;

import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.function.MethodRegistry;
import eu.stratosphere.sopremo.serialization.ObjectSchema;
import eu.stratosphere.sopremo.serialization.Schema;

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

	private Schema[] inputSchemas, outputSchemas;

	private Schema schema;

	private EvaluationExpression resultProjection = EvaluationExpression.VALUE;

	public LinkedList<Operator<?>> getOperatorStack() {
		return this.operatorStack;
	}

	public EvaluationExpression getResultProjection() {
		return this.resultProjection;
	}

	public void setResultProjection(final EvaluationExpression resultProjection) {
		if (resultProjection == null)
			throw new NullPointerException("resultProjection must not be null");

		this.resultProjection = resultProjection;
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

	/**
	 * Initializes EvaluationContext.
	 */
	public EvaluationContext() {
		this(0, 0);
	}

	public EvaluationContext(final int numInputs, final int numOutputs) {
		this.functionRegistry = new MethodRegistry(this.bindings);
		this.setInputsAndOutputs(numInputs, numOutputs);
	}

	public void setInputsAndOutputs(final int numInputs, final int numOutputs) {
		this.inputSchemas = new Schema[numInputs];
		Arrays.fill(this.inputSchemas, new ObjectSchema());
		this.outputSchemas = new Schema[numOutputs];
		Arrays.fill(this.outputSchemas, new ObjectSchema());
	}

	public EvaluationContext(final EvaluationContext context) {
		this(context.inputSchemas.length, context.outputSchemas.length);
		this.bindings.putAll(context.bindings);
		this.inputCounter = context.inputCounter;
		this.inputSchemas = context.inputSchemas.clone();
		this.outputSchemas = context.outputSchemas.clone();
		this.schema = context.schema;
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

	/**
	 * Returns the inputSchemas.
	 * 
	 * @return the inputSchemas
	 */
	public Schema getInputSchema(@SuppressWarnings("unused") final int index) {
		return this.schema;
		// return this.inputSchemas[index];
	}

	/**
	 * Returns the outputSchemas.
	 * 
	 * @return the outputSchemas
	 */
	public Schema getOutputSchema(@SuppressWarnings("unused") final int index) {
		return this.schema;
		// return this.outputSchemas[index];
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
	public void toString(final StringBuilder builder) {
		builder.append("Context @ ").append(this.operatorStack).append("\n").
			append("Bindings: ");
		this.bindings.toString(builder);
	}

	/**
	 * @param schema
	 */
	public void setSchema(final Schema schema) {
		this.schema = schema;
	}
}
