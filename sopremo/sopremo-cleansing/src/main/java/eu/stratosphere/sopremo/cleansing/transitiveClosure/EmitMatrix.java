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
package eu.stratosphere.sopremo.cleansing.transitiveClosure;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.jsondatamodel.NullNode;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.JsonNodeComparator;
import eu.stratosphere.sopremo.pact.SopremoMap;

/**
 * Prepares a {@link BinarySparseMatrix} for output. This is done by simple picking only the lower one of each
 * corresponding pair (a,b) and (b,a).
 */
public class EmitMatrix extends ElementaryOperator<EmitMatrix> {

	/**
	 * 
	 */
	public static final long serialVersionUID = -2384047858154432955L;

	@SuppressWarnings("unused")
	public static class Implementation extends SopremoMap<JsonNode, JsonNode, JsonNode, JsonNode> {

		@Override
		public void map(JsonNode key, JsonNode genMatrix, JsonCollector out) {
			BinarySparseMatrix matrix = (BinarySparseMatrix) genMatrix;
			for (final JsonNode row : matrix.getRows())
				for (final JsonNode column : matrix.get(row))
					if (JsonNodeComparator.INSTANCE.compare(row, column) < 0)
						out.collect(/* key */NullNode.getInstance(),
							JsonUtil.asArray(row, column));
		}
	}
}
