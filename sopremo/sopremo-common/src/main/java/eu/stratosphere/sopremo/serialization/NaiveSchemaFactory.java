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
package eu.stratosphere.sopremo.serialization;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;

/**
 * @author Arvid Heise
 */
public class NaiveSchemaFactory implements SchemaFactory {
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.serialization.SchemaFactory#create(java.lang.Iterable)
	 */
	@Override
	public Schema create(Iterable<EvaluationExpression> keyExpressions) {

		List<ObjectAccess> accesses = new ArrayList<ObjectAccess>();
		List<EvaluationExpression> mappings = new ArrayList<EvaluationExpression>();

		for (EvaluationExpression evaluationExpression : keyExpressions) {
			mappings.add(evaluationExpression);
			if (evaluationExpression instanceof ObjectAccess) {
				accesses.add((ObjectAccess) evaluationExpression);
			}
		}

		if (mappings.isEmpty())
			return new DirectSchema();

		if (accesses.size() == mappings.size()) { // all keyExpressions are ObjectAccesses
			ObjectSchema schema = new ObjectSchema();
			schema.setMappingsWithAccesses(accesses);
			return schema;
		} else {
			return new GeneralSchema(mappings);
		}
	}
}
