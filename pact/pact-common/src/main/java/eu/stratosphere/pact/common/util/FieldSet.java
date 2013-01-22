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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

/**
 *
 */
public class FieldSet implements Iterable<Integer>
{
	protected final Collection<Integer> collection;

	// --------------------------------------------------------------------------------------------

	public FieldSet() {
		this.collection = initCollection();
	}
	
	public FieldSet(int columnIndex) {
		this();
		add(columnIndex);
	}
	
	public FieldSet(int[] columnIndexes) {
		this();
		addAll(columnIndexes);
	}
	
	public FieldSet(Collection<Integer> o) {
		this();
		addAll(o);
	}
	
	public FieldSet(Collection<Integer> o1, Collection<Integer> o2) {
		this();
		addAll(o1);
		addAll(o2);
	}
	
	public FieldSet(FieldSet toCopy) {
		this();
		addAll(toCopy);
	}
	
	// --------------------------------------------------------------------------------------------
	
	public void add(Integer columnIndex) {
		this.collection.add(columnIndex);
	}

	public void addAll(int[] columnIndexes) {
		for (int i = 0; i < columnIndexes.length; i++) {
			add(columnIndexes[i]);
		}
	}

	public void addAll(Collection<Integer> columnIndexes) {
		this.collection.addAll(columnIndexes);
	}
	
	public void addAll(FieldSet set) {
		for (Integer i : set) {
			add(i);
		}
	}
	
	public boolean contains(Integer columnIndex) {
		return this.collection.contains(columnIndex);
	}
	
	public int size() {
		return this.collection.size();
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Iterable#iterator()
	 */
	@Override
	public Iterator<Integer> iterator() {
		return this.collection.iterator();
	}
	
	public FieldList toFieldList() {
		int[] pos = toArray();
		Arrays.sort(pos);
		return new FieldList(pos);
	}
	
	public int[] toArray() {
		int[] a = new int[this.collection.size()];
		int i = 0;
		for (int col : this.collection) {
			a[i++] = col;
		}
		return a;
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
	public boolean isValidSubset(FieldSet set) {
		if (set.size() > size()) {
			return false;
		}
		for (Integer i : set) {
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
		if (obj instanceof FieldSet) {
			return this.collection.equals(((FieldSet) obj).collection);
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
		for (Integer i : this.collection) {
			bld.append(i);
			bld.append(',');
			bld.append(' ');
		}
		if (this.collection.size() > 0) {
			bld.setLength(bld.length() - 2);
		}
		bld.append(']');
		return bld.toString();
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	public FieldSet clone() {
		FieldSet set = new FieldSet();
		set.addAll(this.collection);
		return set;
	}
	
	// --------------------------------------------------------------------------------------------
	
	protected Collection<Integer> initCollection() {
		return new HashSet<Integer>();
	}
	
	protected String getDescriptionPrefix() {
		return "FieldSet";
	}
}
