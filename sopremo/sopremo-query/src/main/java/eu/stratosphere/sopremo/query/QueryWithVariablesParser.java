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
package eu.stratosphere.sopremo.query;

import org.antlr.runtime.RecognizerSharedState;
import org.antlr.runtime.TokenStream;

import eu.stratosphere.sopremo.packages.DefaultRegistry;
import eu.stratosphere.sopremo.packages.IRegistry;

/**
 * @author Arvid Heise
 */
public abstract class QueryWithVariablesParser<VarType> extends AbstractQueryParser {
	private StackedRegistry<VarType, IRegistry<VarType>> variableRegistry =
		new StackedRegistry<VarType, IRegistry<VarType>>(new DefaultRegistry<VarType>());

	public QueryWithVariablesParser(TokenStream input, RecognizerSharedState state) {
		super(input, state);
	}

	public QueryWithVariablesParser(TokenStream input) {
		super(input);
	}

	public void addScope() {
		this.variableRegistry.push(new DefaultRegistry<VarType>());
	}

	public void removeScope() {
		this.variableRegistry.pop();
	}

	/**
	 * Returns the variableRegistry.
	 * 
	 * @return the variableRegistry
	 */
	public StackedRegistry<VarType, IRegistry<VarType>> getVariableRegistry() {
		return this.variableRegistry;
	}
}
