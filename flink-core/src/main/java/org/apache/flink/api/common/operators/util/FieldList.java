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

package org.apache.flink.api.common.operators.util;

import org.apache.flink.annotation.Internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Immutable ordered list of fields IDs.
 */
@Internal
public class FieldList extends FieldSet {
	
	public static final FieldList EMPTY_LIST = new FieldList();
	
	
	public FieldList() {
		super(Collections.<Integer>emptyList());
	}
	
	public FieldList(int fieldId) {
		super(Collections.singletonList(fieldId));
	}
	
	public FieldList(Integer fieldId) {
		super(Collections.singletonList(checkNotNull(fieldId, "The fields ID must not be null.")));
	}
	
	public FieldList(int... columnIndexes) {
		super (fromInts(columnIndexes));
	}
	
	private FieldList(List<Integer> fields) {
		super(fields);
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public FieldList addField(Integer fieldID) {
		if (fieldID == null) {
			throw new IllegalArgumentException("Field ID must not be null.");
		}
		
		if (size() == 0) {
			return new FieldList(fieldID);
		} else {
			ArrayList<Integer> list = new ArrayList<Integer>(size() + 1);
			list.addAll(this.collection);
			list.add(fieldID);
			return new FieldList(Collections.unmodifiableList(list));
		}
	}

	@Override
	public FieldList addFields(int... fieldIDs) {
		if (fieldIDs == null || fieldIDs.length == 0) {
			return this;
		}
		if (size() == 0) {
			return new FieldList(fieldIDs);
		} else {
			ArrayList<Integer> list = new ArrayList<Integer>(size() + fieldIDs.length);
			list.addAll(this.collection);
			for (int i = 0; i < fieldIDs.length; i++) {
				list.add(fieldIDs[i]);
			}
			
			return new FieldList(Collections.unmodifiableList(list));
		}
	}
	
	@Override
	public FieldList addFields(FieldSet set) {
		if (set == null) {
			throw new IllegalArgumentException("FieldSet to add must not be null.");
		}
		
		if (set.size() == 0) {
			return this;
		}
		else if (size() == 0 && set instanceof FieldList) {
			return (FieldList) set;
		}
		else {
			ArrayList<Integer> list = new ArrayList<Integer>(size() + set.size());
			list.addAll(this.collection);
			list.addAll(set.collection);
			return new FieldList(Collections.unmodifiableList(list));
		}
	}
	
	public Integer get(int pos) {
		return get().get(pos);
	}
	
	@Override
	public FieldList toFieldList() {
		return this;
	}

	// --------------------------------------------------------------------------------------------
	
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

	public boolean isExactMatch(FieldList list) {
		if (this.size() != list.size()) {
			return false;
		} else {
			for (int i = 0; i < this.size(); i++) {
				if (!this.get(i).equals(list.get(i))) {
					return false;
				}
			}
			return true;
		}
	}
	
	// --------------------------------------------------------------------------------------------

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
	
	private static final List<Integer> fromInts(int... ints) {
		if (ints == null || ints.length == 0) {
			return Collections.emptyList();
		} else {
			ArrayList<Integer> al = new ArrayList<Integer>(ints.length);
			for (int i = 0; i < ints.length; i++) {
				al.add(ints[i]);
			}
			return Collections.unmodifiableList(al);
		}
	}
}
