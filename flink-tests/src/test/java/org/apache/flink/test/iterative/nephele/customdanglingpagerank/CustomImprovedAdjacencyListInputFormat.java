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


package org.apache.flink.test.iterative.nephele.customdanglingpagerank;

import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.apache.flink.test.iterative.nephele.customdanglingpagerank.types.VertexWithAdjacencyList;
import org.apache.flink.test.iterative.nephele.danglingpagerank.AsciiLongArrayView;

public class CustomImprovedAdjacencyListInputFormat extends DelimitedInputFormat<VertexWithAdjacencyList> {
	private static final long serialVersionUID = 1L;

	private final AsciiLongArrayView arrayView = new AsciiLongArrayView();

	@Override
	public VertexWithAdjacencyList readRecord(VertexWithAdjacencyList target, byte[] bytes, int offset, int numBytes) {

		if (numBytes == 0) {
			return null;
		}

		arrayView.set(bytes, offset, numBytes);
		
		long[] list = target.getTargets();

		try {

			int pos = 0;
			while (arrayView.next()) {

				if (pos == 0) {
					target.setVertexID(arrayView.element());
				} else {
					if (list.length <= pos - 1) {
						list = new long[list.length < 16 ? 16 : list.length * 2];
						target.setTargets(list);
					}
					list[pos - 1] = arrayView.element();
				}
				pos++;
			}
			
			target.setNumTargets(pos - 1);
		} catch (RuntimeException e) {
			throw new RuntimeException("Error parsing: " + arrayView.toString(), e);
		}

		return target;
	}
}
