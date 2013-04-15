/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.common.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 *
 */
public class FieldList extends FieldSet
{
	public FieldList() {
		super();
	}
	
	public FieldList(int columnIndex) {
		this();
		add(columnIndex);
	}
	
	public FieldList(int[] columnIndexes) {
		this();
		addAll(columnIndexes);
	}
	
	// --------------------------------------------------------------------------------------------
	
	public Integer get(int pos) {
		return get().get(pos);
	}
	
	public FieldList toFieldList() {
		return this;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.util.FieldSet#isValidSubset(eu.stratosphere.pact.common.util.FieldSet)
	 */
	@Override
	public boolean isValidSubset(FieldSet set) {
		if (set instanceof FieldList) {
			return (isValidSubset((FieldList) set));
		} else {
			return false;
		}
	}
	
	public boolean isValidSubset(FieldList list) {
		if (list.size() > size()) {
			return false;
		}
		final List<Integer> myList = get();
		final List<Integer> theirList = list.get();
		for (int i = 0; i < theirList.size(); i++) {
			Integer myInt = myList.get(i);
			Integer theirInt = theirList.get(i);
			if (myInt.intValue() != theirInt.intValue()) {
				return false;
			}
		}
		return true;
	}
	
	public boolean isValidUnorderedPrefix(FieldSet set) {
		if (set.size() > size()) {
			return false;
		}
		
		List<Integer> list = get();
		for (int i = 0; i < set.size(); i++) {
			if (!set.contains(list.get(i))) {
				return false;
			}
		}
		return true;
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	protected Collection<Integer> initCollection() {
		return new ArrayList<Integer>();
	}

	@Override
	protected String getDescriptionPrefix() {
		return "[";
	}
	
	@Override
	protected String getDescriptionSuffix() {
		return "]";
	}

	private List<Integer> get() {
		return (List<Integer>) this.collection;
	}
}
