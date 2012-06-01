/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.sopremo.sdaa11.clustering.util;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.ElementarySopremoModule;
import eu.stratosphere.sopremo.SopremoModule;

/**
 * @author skruse
 * 
 */
public class OutputProjection extends CompositeOperator<OutputProjection> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7119989985555069850L;
	private final Operator<?> operator;
	private final int outputIndex;

	protected int getNumInputs() {
		return 1;
	}

	/**
	 * Initializes OutputProjection.
	 * 
	 * @param operator
	 */
	public OutputProjection(final Operator<?> operator, final int outputIndex) {
		this.operator = operator;
		this.outputIndex = outputIndex;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.sopremo.CompositeOperator#asElementaryOperators()
	 */
	@Override
	public ElementarySopremoModule asElementaryOperators() {
		final SopremoModule module = new SopremoModule(this.operator.getName()
				+ "_" + this.outputIndex, 1, 1);
		this.operator.withInputs(module.getInputs());
		module.getOutput(0).setInput(0,
				this.operator.getOutput(this.outputIndex));
		return module.asElementary();
	}

}
