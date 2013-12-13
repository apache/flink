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

package eu.stratosphere.pact.runtime.sort;

public interface IndexedSorter {

	/**
	 * Sort the items accessed through the given IndexedSortable over the given
	 * range of logical indices. From the perspective of the sort algorithm,
	 * each index between l (inclusive) and r (exclusive) is an addressable
	 * entry.
	 * 
	 * @see IndexedSortable#compare
	 * @see IndexedSortable#swap
	 */
	void sort(IndexedSortable s, int l, int r);

	void sort(IndexedSortable s);

}
