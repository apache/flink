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

package eu.stratosphere.pact.runtime.task.util;

/**
 * Enumeration of all available local processing strategies tasks. 
 */
public enum LocalStrategy
{
	// no special local strategy is applied
	NONE(false, false),
	// the input is sorted
	SORT(true, true),
	// the input is sorted, during sorting a combiner is applied
	COMBININGSORT(true, true);
	
	// --------------------------------------------------------------------------------------------
	
	private final boolean dams;
	
	private boolean requiresComparator;

	private LocalStrategy(boolean dams, boolean requiresComparator) {
		this.dams = dams;
		this.requiresComparator = requiresComparator;
	}
	
	public boolean dams() {
		return this.dams;
	}
	
	public boolean requiresComparator() {
		return this.requiresComparator;
	}
}