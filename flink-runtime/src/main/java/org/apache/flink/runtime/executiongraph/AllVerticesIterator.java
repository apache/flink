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

package org.apache.flink.runtime.executiongraph;

import java.util.Iterator;
import java.util.NoSuchElementException;

class AllVerticesIterator implements Iterator<ExecutionVertex> {

	private final Iterator<ExecutionJobVertex> jobVertices;
	
	private ExecutionVertex[] currVertices;
	
	private int currPos;
	
	
	public AllVerticesIterator(Iterator<ExecutionJobVertex> jobVertices) {
		this.jobVertices = jobVertices;
	}
	
	
	@Override
	public boolean hasNext() {
		while (true) {
			if (currVertices != null) {
				if (currPos < currVertices.length) {
					return true;
				} else {
					currVertices = null;
				}
			}
			else if (jobVertices.hasNext()) {
				currVertices = jobVertices.next().getTaskVertices();
				currPos = 0;
			}
			else {
				return false;
			}
		}
	}
	
	@Override
	public ExecutionVertex next() {
		if (hasNext()) {
			return currVertices[currPos++];
		} else {
			throw new NoSuchElementException();
		}
	}
	
	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
}
