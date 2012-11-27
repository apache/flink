/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.sopremo.serialization;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;

/**
 * This Factory provides the functionality described in {@link SchemaFactory} in an simple way.
 * Only the following conditions are checked:</br>
 * <ul>
 * <li>no key expressions are provided: {@link DirectSchema}</br>
 * <li>all key expressions are {@link ObjectAccess}: {@link ObjectSchema}</br>
 * <li>all key expressions are {@link ArrayAccess}:</br> - startIndex of the first key expression is 0:
 * {@link HeadArraySchema}</br> - else: {@link TailArraySchema}</br>
 * <li>non of the conditions described above are true: {@link GeneralSchema}
 * 
 * @author Arvid Heise
 */
public class NaiveSchemaFactory implements SchemaFactory {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2116538067208466632L;

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.serialization.SchemaFactory#create(java.lang.Iterable)
	 */
	@Override
	public Schema create(final Iterable<EvaluationExpression> keyExpressions) {

		final List<ObjectAccess> objectAccesses = new ArrayList<ObjectAccess>();
		final List<ArrayAccess> arrayAccesses = new ArrayList<ArrayAccess>();
		final List<EvaluationExpression> mappings = new ArrayList<EvaluationExpression>();

		for (final EvaluationExpression evaluationExpression : keyExpressions) {
			mappings.add(evaluationExpression);
			if (evaluationExpression instanceof ObjectAccess)
				objectAccesses.add((ObjectAccess) evaluationExpression);
			if (evaluationExpression instanceof ArrayAccess)
				arrayAccesses.add((ArrayAccess) evaluationExpression);
		}

		if (mappings.isEmpty())
			return new DirectSchema();

		if (objectAccesses.size() == mappings.size())
			// all keyExpressions are ObjectAccesses
			return new ObjectSchema(objectAccesses);
		else if (arrayAccesses.size() == mappings.size()) {
			// all keyExpressions are ArrayAccesses

			final int startIndex = arrayAccesses.get(0).getStartIndex();
			final int endIndex = arrayAccesses.get(arrayAccesses.size() - 1).getEndIndex();

			if (startIndex == 0)
				// want to reduce on first elements of the array -> HeadArraySchema should be used
				return new HeadArraySchema(endIndex + 1);
			return new TailArraySchema(endIndex - startIndex + 1);
		}
		return new GeneralSchema(mappings);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.ISopremoType#toString(java.lang.StringBuilder)
	 */
	@Override
	public void toString(StringBuilder builder) {
		builder.append("naive schema");
	}
}
