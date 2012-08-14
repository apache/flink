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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;


/**
 *
 */
public class OptimizerFieldSet implements Iterable<ColumnWithType>
{
	protected final Collection<ColumnWithType> collection;

	// --------------------------------------------------------------------------------------------

	public OptimizerFieldSet() {
		this.collection = initCollection();
	}
	
	// --------------------------------------------------------------------------------------------
	
	public void add(ColumnWithType columnIndex) {
		this.collection.add(columnIndex);
	}

	public void addAll(Collection<ColumnWithType> columnIndexes) {
		this.collection.addAll(columnIndexes);
	}
	
	public void addAll(OptimizerFieldSet set) {
		for (ColumnWithType i : set) {
			add(i);
		}
	}
	
	public boolean contains(ColumnWithType columnIndex) {
		return this.collection.contains(columnIndex);
	}
	
	public int size() {
		return this.collection.size();
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Iterable#iterator()
	 */
	@Override
	public Iterator<ColumnWithType> iterator() {
		return this.collection.iterator();
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Checks if the given set of fields is a valid subset of this set of fields. For unordered
	 * sets, this is the case if all of the given set's fields are also part of this field.
	 * <p>
	 * Subclasses that describe field sets where the field order matters must override this method
	 * to implement a field ordering sensitive check.
	 * 
	 * @param set The set that is a candidate subset.
	 * @return True, if the given set is a subset of this set, false otherwise.
	 */
	public boolean isValidSubset(OptimizerFieldSet set) {
		if (set.size() > size()) {
			return false;
		}
		for (ColumnWithType i : set) {
			if (!contains(i)) {
				return false;
			}
		}
		return true;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return this.collection.hashCode();
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (obj instanceof OptimizerFieldSet) {
			return this.collection.equals(((OptimizerFieldSet) obj).collection);
		} else {
			return false;
		}
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder bld = new StringBuilder();
		bld.append(getDescriptionPrefix()).append(' ').append('[');
		for (ColumnWithType i : this.collection) {
			bld.append(i);
			bld.append(',');
			bld.append(' ');
		}
		bld.setLength(bld.length() - 2);
		bld.append(']');
		return bld.toString();
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	public OptimizerFieldSet clone() {
		OptimizerFieldSet set = new OptimizerFieldSet();
		set.addAll(this.collection);
		return set;
	}
	
	// --------------------------------------------------------------------------------------------
	
	protected Collection<ColumnWithType> initCollection() {
		return new HashSet<ColumnWithType>();
	}
	
	protected String getDescriptionPrefix() {
		return "Field Set";
	}
}
