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
package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import eu.stratosphere.sopremo.AbstractSopremoType;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SerializableSopremoType;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.PathExpression;
import eu.stratosphere.sopremo.expressions.ObjectCreation.FieldAssignment;
import eu.stratosphere.sopremo.expressions.ObjectCreation.Mapping;

/**
 * @author Arvid Heise
 */
public class RuleManager extends AbstractSopremoType implements SerializableSopremoType {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6117578045521341880L;

	private List<Map.Entry<List<EvaluationExpression>, EvaluationExpression>> rules = new ArrayList<Map.Entry<List<EvaluationExpression>, EvaluationExpression>>();

	public void addRule(EvaluationExpression rule, List<EvaluationExpression> target) {
		this.rules.add(new AbstractMap.SimpleEntry<List<EvaluationExpression>, EvaluationExpression>(target, rule));
	}

	public void addRule(EvaluationExpression rule, EvaluationExpression... target) {
		this.rules.add(new AbstractMap.SimpleEntry<List<EvaluationExpression>, EvaluationExpression>(
			Arrays.asList(target), rule));
	}

	/**
	 * Returns the rules.
	 * 
	 * @return the rules
	 */
	public List<Map.Entry<List<EvaluationExpression>, EvaluationExpression>> getRules() {
		return this.rules;
	}

	public boolean isEmpty() {
		return this.rules.isEmpty();
	}

	public void removeRule(EvaluationExpression rule, List<EvaluationExpression> target) {
		this.rules.remove(new AbstractMap.SimpleEntry<List<EvaluationExpression>, EvaluationExpression>(target, rule));
	}

	public void removeRule(EvaluationExpression rule, EvaluationExpression... target) {
		this.rules.remove(new AbstractMap.SimpleEntry<List<EvaluationExpression>, EvaluationExpression>(
			Arrays.asList(target), rule));
	}

	public void parse(EvaluationExpression expression, Operator<?> operator, RuleFactory ruleFactory) {
		this.parse(expression, operator, ruleFactory, new PathExpression());
	}

	private void parse(EvaluationExpression expression, Operator<?> operator, RuleFactory ruleFactory,
			PathExpression contextPath) {
		if (expression instanceof ObjectCreation) {
			for (Mapping<?> field : ((ObjectCreation) expression).getMappings()) {
				contextPath.add(new ObjectAccess(((FieldAssignment) field).getTarget()));
				this.parse(field.getExpression(), operator, ruleFactory, contextPath);
				contextPath.removeLast();
			}
			return;
		}
		if (expression instanceof ArrayCreation) {
			List<EvaluationExpression> children = ((ArrayCreation) expression).getChildren();
			for (int index = 0, size = children.size(); index < size; index++) {
				// context.push(new ArrayAccess(index));
				this.parse(children.get(index), operator, ruleFactory, contextPath);
				// context.pop();
			}
			return;
		}

		this.addRule(ruleFactory.createRule(expression, operator, contextPath), new ArrayList<EvaluationExpression>(
			contextPath.getFragments()));
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.SopremoType#toString(java.lang.StringBuilder)
	 */
	@Override
	public void toString(StringBuilder builder) {
		builder.append(getRules());
	}
}
