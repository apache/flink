/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.api.common.operators;

import java.util.List;

import eu.stratosphere.api.common.functions.Function;
import eu.stratosphere.api.common.operators.util.UserCodeWrapper;
import eu.stratosphere.util.Visitor;

/**
 * Abstract superclass for for all operators that have one input like "map" or "reduce".
 */
public abstract class SingleInputOperator<T extends Function> extends AbstractUdfOperator<T> {
	
	/**
	 * The input which produces the data consumed by this operator.
	 */
	protected Operator input;
	
	/**
	 * The positions of the keys in the tuple.
	 */
	private final int[] keyFields;
	
	/**
	 * Semantic properties of the associated function.
	 */
	private SingleInputSemanticProperties semanticProperties;
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Creates a new abstract single-input operator with the given name wrapping the given user function.
	 * 
	 * @param stub The object containing the user function.
	 * @param keyPositions The field positions of the input records that act as keys.
	 * @param name The given name for the operator, used in plans, logs and progress messages.
	 */
	protected SingleInputOperator(UserCodeWrapper<T> stub, int[] keyPositions, String name) {
		super(stub, name);
		this.keyFields = keyPositions;
	}
	
	/**
	 * Creates a new abstract single-input operator with the given name wrapping the given user function.
	 * This constructor is specialized only for operators that require no keys for their processing.
	 * 
	 * @param stub The object containing the user function.
	 * @param name The given name for the operator, used in plans, logs and progress messages.
	 */
	protected SingleInputOperator(UserCodeWrapper<T> stub, String name) {
		super(stub, name);
		this.keyFields = new int[0];
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the input operator or data source, or null, if none is set.
	 * 
	 * @return This operator's input.
	 */
	public Operator getInput() {
		return this.input;
	}
	
	/**
	 * Removes all inputs.
	 */
	public void clearInputs() {
		this.input = null;
	}
	
	/**
	 * Sets the given operator as the input to this operator.
	 * 
	 * @param input The operator to use as the input.
	 */
	public void setInput(Operator input) {
		this.input = input;
	}
	
	/**
	 * Sets the input to the union of the given operators.
	 * 
	 * @param input The operator(s) that form the input.
	 * @deprecated This method will be removed in future versions. Use the {@link Union} operator instead.
	 */
	@Deprecated
	public void setInput(Operator ... input) {
		this.input = Operator.createUnionCascade(null, input);
	}
	
	/**
	 * Sets the input to the union of the given operators.
	 * 
	 * @param inputs The operator(s) that form the input.
	 * @deprecated This method will be removed in future versions. Use the {@link Union} operator instead.
	 */
	@Deprecated
	public void setInputs(List<Operator> inputs) {
		this.input = Operator.createUnionCascade(null, inputs.toArray(new Operator[inputs.size()]));
	}

	/**
	 * Adds to the input the union of the given operators.
	 * 
	 * @param input The operator(s) that form the input.
	 * @deprecated This method will be removed in future versions. Use the {@link Union} operator instead.
	 */
	@Deprecated
	public void addInput(Operator ... input) {
		this.input = Operator.createUnionCascade(this.input, input);
	}
	
	/**
	 * Adds to the input the union of the given operators.
	 * 
	 * @param inputs The operator(s) that form the input.
	 * @deprecated This method will be removed in future versions. Use the {@link Union} operator instead.
	 */
	@Deprecated
	public void addInput(List<Operator> inputs) {
		this.input = Operator.createUnionCascade(this.input, inputs.toArray(new Operator[inputs.size()]));
	}

	// --------------------------------------------------------------------------------------------

	public SingleInputSemanticProperties getSemanticProperties() {
		return this.semanticProperties;
	}
	
	public void setSemanticProperties(SingleInputSemanticProperties semanticProperties) {
		this.semanticProperties = semanticProperties;
	}
	
	// --------------------------------------------------------------------------------------------

	
	@Override
	public final int getNumberOfInputs() {
		return 1;
	}

	@Override
	public int[] getKeyColumns(int inputNum) {
		if (inputNum == 0) {
			return this.keyFields;
		}
		else throw new IndexOutOfBoundsException();
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Accepts the visitor and applies it this instance. The visitors pre-visit method is called and, if returning 
	 * <tt>true</tt>, the visitor is recursively applied on the single input. After the recursion returned,
	 * the post-visit method is called.
	 * 
	 * @param visitor The visitor.
	 *  
	 * @see eu.stratosphere.util.Visitable#accept(eu.stratosphere.util.Visitor)
	 */
	@Override
	public void accept(Visitor<Operator> visitor) {
		if (visitor.preVisit(this)) {
			this.input.accept(visitor);
			for (Operator c : this.broadcastInputs.values()) {
				c.accept(visitor);
			}
			visitor.postVisit(this);
		}
	}
}
