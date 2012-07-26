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
package eu.stratosphere.meteor;

import org.antlr.runtime.RecognizerSharedState;
import org.antlr.runtime.Token;
import org.antlr.runtime.TokenStream;

import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.JsonStreamExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.PathExpression;
import eu.stratosphere.sopremo.query.QueryWithVariablesParser;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.DecimalNode;
import eu.stratosphere.sopremo.type.DoubleNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * @author Arvid Heise
 */
public abstract class MeteorParserBase extends QueryWithVariablesParser<JsonStreamExpression> {

	public MeteorParserBase(TokenStream input, RecognizerSharedState state) {
		super(input, state);
		init();
	}

	public MeteorParserBase(TokenStream input) {
		super(input);
		init();
	}

	private void init() {
		getPackageManager().importPackage("base");
		
		addTypeAlias("int", IntNode.class);
		addTypeAlias("decimal", DecimalNode.class);
		addTypeAlias("string", TextNode.class);
		addTypeAlias("double", DoubleNode.class);
		addTypeAlias("boolean", BooleanNode.class);
		addTypeAlias("bool", BooleanNode.class);
	}

	protected JsonStreamExpression getVariable(Token name) {
		return getVariableRegistry().get(name.getText());
	}

	protected void putVariable(Token name, JsonStreamExpression expression) {
		getVariableRegistry().put(name.getText(), expression);
	}

	protected String getAssignmentName(EvaluationExpression expression) {
		if (expression instanceof PathExpression)
			return getAssignmentName(((PathExpression) expression).getLastFragment());
		if (expression instanceof ObjectAccess)
			return ((ObjectAccess) expression).getField();
		return expression.toString();
	}

}
