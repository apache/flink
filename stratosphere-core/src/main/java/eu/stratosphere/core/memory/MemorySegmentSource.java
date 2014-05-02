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

package eu.stratosphere.core.memory;


/**
 * Interface describing entities that can provide memory segments.
 */
public interface MemorySegmentSource {
	
	/**
	 * Gets the next memory segment. If no more segments are available, it returns null.
	 * 
	 * @return The next memory segment, or null, if none is available.
	 */
	public MemorySegment nextSegment();
}
