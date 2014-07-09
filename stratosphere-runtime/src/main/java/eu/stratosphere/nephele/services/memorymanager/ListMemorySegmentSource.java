/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Apache Flink project (http://flink.incubator.apache.org)
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

package eu.stratosphere.nephele.services.memorymanager;

import java.util.List;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentSource;

/**
 * Simple memory segment source that draws segments from a list.
 * 
 */
public class ListMemorySegmentSource implements MemorySegmentSource
{
	private final List<MemorySegment> segments;
	
	public ListMemorySegmentSource(final List<MemorySegment> memorySegments) {
		this.segments = memorySegments;
	}
	

	@Override
	public MemorySegment nextSegment() {
		if (this.segments.size() > 0) {
			return this.segments.remove(this.segments.size() - 1);
		} else {
			return null;
		}
	}
}
