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

package eu.stratosphere.test.iterative.nephele.danglingpagerank;

import eu.stratosphere.api.record.io.TextInputFormat;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.test.iterative.nephele.ConfigUtils;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;

public class ImprovedDanglingPageRankInputFormat extends TextInputFormat {
	private static final long serialVersionUID = 1L;

	private LongValue vertexID = new LongValue();

	private DoubleValue initialRank;

	private BooleanValue isDangling = new BooleanValue();

	private AsciiLongArrayView arrayView = new AsciiLongArrayView();

	private static final long DANGLING_MARKER = 1l;

	@Override
	public void configure(Configuration parameters) {
		long numVertices = ConfigUtils.asLong("pageRank.numVertices", parameters);
		initialRank = new DoubleValue(1 / (double) numVertices);
		super.configure(parameters);
	}

	@Override
	public boolean readRecord(Record target, byte[] bytes, int offset, int numBytes) {

		arrayView.set(bytes, offset, numBytes);

		try {
			arrayView.next();
			vertexID.setValue(arrayView.element());

			if (arrayView.next()) {
				isDangling.set(arrayView.element() == DANGLING_MARKER);
			} else {
				isDangling.set(false);
			}

		} catch (NumberFormatException e) {
			throw new RuntimeException("Error parsing " + arrayView.toString(), e);
		}

		target.clear();
		target.addField(vertexID);
		target.addField(initialRank);
		target.addField(isDangling);

		return true;
	}
}
