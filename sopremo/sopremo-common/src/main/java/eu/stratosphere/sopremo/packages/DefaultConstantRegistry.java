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
package eu.stratosphere.sopremo.packages;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.sopremo.expressions.EvaluationExpression;

/**
 * Default implementation of {@link IConstantRegistry}.
 * 
 * @author Arvid Heise
 */
public class DefaultConstantRegistry implements IConstantRegistry {
	private Map<String, EvaluationExpression> constants = new HashMap<String, EvaluationExpression>();

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.packages.IConstantRegistry#findConstant(java.lang.String)
	 */
	@Override
	public EvaluationExpression findConstant(String name) {
		return this.constants.get(name);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.packages.IConstantRegistry#registerConstant(java.lang.String,
	 * eu.stratosphere.sopremo.expressions.EvaluationExpression)
	 */
	@Override
	public void registerConstant(String name, EvaluationExpression expression) {
		this.constants.put(name, expression);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.packages.IConstantRegistry#getRegisteredConstants()
	 */
	@Override
	public Map<String, EvaluationExpression> getRegisteredConstants() {
		return Collections.unmodifiableMap(this.constants);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.ISopremoType#toString(java.lang.StringBuilder)
	 */
	@Override
	public void toString(StringBuilder builder) {
		builder.append("Constant registry").append(this.constants);
	}
}
