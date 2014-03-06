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

package eu.stratosphere.test.iterative.nephele.customdanglingpagerank;

import eu.stratosphere.api.common.io.DelimitedInputFormat;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.test.iterative.nephele.ConfigUtils;
import eu.stratosphere.test.iterative.nephele.customdanglingpagerank.types.VertexWithRankAndDangling;
import eu.stratosphere.test.iterative.nephele.danglingpagerank.AsciiLongArrayView;

public class CustomImprovedDanglingPageRankInputFormat extends DelimitedInputFormat<VertexWithRankAndDangling> {
	private static final long serialVersionUID = 1L;

	private AsciiLongArrayView arrayView = new AsciiLongArrayView();

	private static final long DANGLING_MARKER = 1l;
	
	private double initialRank;

	@Override
	public void configure(Configuration parameters) {
		long numVertices = ConfigUtils.asLong("pageRank.numVertices", parameters);
		initialRank = 1.0 / numVertices;
		super.configure(parameters);
	}

	@Override
	public VertexWithRankAndDangling readRecord(VertexWithRankAndDangling target, byte[] bytes, int offset, int numBytes) {

		arrayView.set(bytes, offset, numBytes);

		try {
			arrayView.next();
			target.setVertexID(arrayView.element());

			if (arrayView.next()) {
				target.setDangling(arrayView.element() == DANGLING_MARKER);
			} else {
				target.setDangling(false);
			}

		} catch (NumberFormatException e) {
			throw new RuntimeException("Error parsing " + arrayView.toString(), e);
		}

		target.setRank(initialRank);

		return target;
	}
}
