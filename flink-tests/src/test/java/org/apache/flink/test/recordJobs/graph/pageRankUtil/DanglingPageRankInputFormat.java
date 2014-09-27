/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.test.recordJobs.graph.pageRankUtil;

import org.apache.flink.api.java.record.io.TextInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.recordJobs.util.ConfigUtils;
import org.apache.flink.types.BooleanValue;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.Record;

public class DanglingPageRankInputFormat extends TextInputFormat {
	private static final long serialVersionUID = 1L;
	
	public static final String NUM_VERTICES_PARAMETER = "pageRank.numVertices";

	private LongValue vertexID = new LongValue();

	private DoubleValue initialRank;

	private BooleanValue isDangling = new BooleanValue();

	private AsciiLongArrayView arrayView = new AsciiLongArrayView();

	private static final long DANGLING_MARKER = 1l;

	@Override
	public void configure(Configuration parameters) {
		long numVertices = ConfigUtils.asLong(NUM_VERTICES_PARAMETER, parameters);
		initialRank = new DoubleValue(1 / (double) numVertices);
		super.configure(parameters);
	}

	@Override
	public Record readRecord(Record target, byte[] bytes, int offset, int numBytes) {

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

		return target;
	}
}
