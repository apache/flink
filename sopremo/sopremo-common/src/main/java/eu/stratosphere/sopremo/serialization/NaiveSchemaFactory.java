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

import eu.stratosphere.sopremo.expressions.ArrayAccess;
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

		List<ObjectAccess> objectAccesses = new ArrayList<ObjectAccess>();
		List<ArrayAccess> arrayAccesses = new ArrayList<ArrayAccess>();
		List<EvaluationExpression> mappings = new ArrayList<EvaluationExpression>();

		for (EvaluationExpression evaluationExpression : keyExpressions) {
			mappings.add(evaluationExpression);
			if (evaluationExpression instanceof ObjectAccess) {
				objectAccesses.add((ObjectAccess) evaluationExpression);
			}
			if (evaluationExpression instanceof ArrayAccess) {
				arrayAccesses.add((ArrayAccess) evaluationExpression);
			}
		}

		if (mappings.isEmpty())
			return new DirectSchema();

		if (objectAccesses.size() == mappings.size()) {
			// all keyExpressions are ObjectAccesses

			ObjectSchema schema = new ObjectSchema();
			schema.setMappingsWithAccesses(objectAccesses);
			return schema;
		} else if (arrayAccesses.size() == mappings.size()) {
			// all keyExpressions are ArrayAccesses

			int startIndex = arrayAccesses.get(0).getStartIndex();
			int endIndex = arrayAccesses.get(arrayAccesses.size()).getEndIndex();

			if (startIndex == 0) {
				// want to reduce on first elements of the array -> HeadArraySchema should be used
				
				HeadArraySchema schema = new HeadArraySchema();
				schema.setHeadSize(endIndex + 1);
				return schema;
			} else {	
				TailArraySchema schema = new TailArraySchema();
				schema.setTailSize(endIndex - startIndex + 1);
				return schema;
			}
		} else {
			// all other schemas doesn't match -> have to use GeneralSchema

			return new GeneralSchema(mappings);
		}
	}
}
