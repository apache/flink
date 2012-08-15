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

package eu.stratosphere.pact.compiler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import eu.stratosphere.pact.common.contract.Ordering;


/**
 *
 */
public class OptimizerFieldList extends OptimizerFieldSet
{
	public OptimizerFieldList() {
		super();
	}
	
	// --------------------------------------------------------------------------------------------
	
	public ColumnWithType get(int pos) {
		return get().get(pos);
	}
	
	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.util.FieldSet#isValidSubset(eu.stratosphere.pact.common.util.FieldSet)
	 */
	@Override
	public boolean isValidSubset(OptimizerFieldSet set) {
		if (set instanceof OptimizerFieldList) {
			return (isValidSubset((OptimizerFieldList) set));
		} else {
			return false;
		}
	}
	
	public boolean isValidSubset(OptimizerFieldList list) {
		if (list.size() > size()) {
			return false;
		}
		final List<ColumnWithType> myList = get();
		final List<ColumnWithType> theirList = list.get();
		for (int i = 0; i < theirList.size(); i++) {
			ColumnWithType myInt = myList.get(i);
			ColumnWithType theirInt = theirList.get(i);
			if (!myInt.equals(theirInt)) {
				return false;
			}
		}
		return true;
	}
	
	public boolean isValidUnorderedPrefix(OptimizerFieldSet set) {
		if (set.size() > size()) {
			return false;
		}
		
		List<ColumnWithType> list = get();
		for (int i = 0; i < set.size(); i++) {
			if (!set.contains(list.get(i))) {
				return false;
			}
		}
		return true;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.util.AbstractFieldSet#initCollection()
	 */
	@Override
	protected Collection<ColumnWithType> initCollection() {
		return new ArrayList<ColumnWithType>();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.util.FieldSet#getDescriptionPrefix()
	 */
	@Override
	protected String getDescriptionPrefix() {
		return "Field List";
	}

	private List<ColumnWithType> get() {
		return (List<ColumnWithType>) this.collection;
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static OptimizerFieldList getFromOrdering(Ordering ordering) {
		final OptimizerFieldList list = new OptimizerFieldList();
		for (int i = 0; i < ordering.getNumberOfFields(); i++) {
			list.add(ordering.getFieldNumber(i), ordering.getType(i));
		}
		return list;
	}
}
