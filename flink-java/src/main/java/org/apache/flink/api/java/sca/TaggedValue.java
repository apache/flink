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

package org.apache.flink.api.java.sca;

import org.apache.flink.annotation.Internal;

import org.apache.flink.shaded.asm5.org.objectweb.asm.Type;
import org.apache.flink.shaded.asm5.org.objectweb.asm.tree.analysis.BasicValue;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Extension of ASM's BasicValue that allows to assign "tags"
 * to values and add additional information depending on the tag to the Value.
 */
@Internal
public class TaggedValue extends BasicValue {

	/**
	 * Possible tags.
	 */
	public enum Tag {
		REGULAR, // regular object with no special meaning
		THIS, // a special container which is the instance of the UDF
		INPUT, // atomic input field
		COLLECTOR, // collector of UDF
		CONTAINER, // container that contains fields
		INT_CONSTANT, // int constant
		INPUT_1_ITERABLE, INPUT_2_ITERABLE, INPUT_1_ITERATOR, INPUT_2_ITERATOR, // input iterators
		ITERATOR_TRUE_ASSUMPTION, // boolean value that is "true" at least once
		NULL // null
	}

	/**
	 * Distinguishes between inputs in case of two input operators.
	 */
	public enum Input {
		INPUT_1(0), INPUT_2(1);

		private int id;

		private Input(int id) {
			this.id = id;
		}

		public int getId() {
			return id;
		}
	}

	private Tag tag;
	// only inputs can set this to true
	private boolean callByValue = false;

	// for inputs
	private Input input;
	private String flatFieldExpr; // empty string for non-composite types
	private boolean grouped;

	// for input containers & this
	// key=field / value=input or container
	// undefined state => value=null
	private Map<String, TaggedValue> containerMapping;
	private Map<String, ModifiedASMFrame> containerFrameMapping;

	// for int constants
	private int intConstant;

	public TaggedValue(Type type) {
		this(type, Tag.REGULAR);
	}

	public TaggedValue(Type type, Tag tag) {
		super(type);
		this.tag = tag;
	}

	public TaggedValue(Type type, Input input, String flatFieldExpr, boolean grouped, boolean callByValue) {
		super(type);
		tag = Tag.INPUT;
		this.input = input;
		this.flatFieldExpr = flatFieldExpr;
		this.grouped = grouped;
		this.callByValue = callByValue;
	}

	public TaggedValue(Type type, Map<String, TaggedValue> containerMapping) {
		super(type);
		tag = Tag.CONTAINER;
		this.containerMapping = containerMapping;
	}

	public TaggedValue(int constant) {
		super(Type.INT_TYPE);
		tag = Tag.INT_CONSTANT;
		this.intConstant = constant;
	}

	public boolean isInput() {
		return tag == Tag.INPUT;
	}

	public boolean isThis() {
		return tag == Tag.THIS;
	}

	public boolean isContainer() {
		return tag == Tag.CONTAINER;
	}

	public boolean isRegular() {
		return tag == Tag.REGULAR;
	}

	public boolean isIntConstant() {
		return tag == Tag.INT_CONSTANT;
	}

	public boolean isCollector() {
		return tag == Tag.COLLECTOR;
	}

	public boolean isInputIterable() {
		return tag == Tag.INPUT_1_ITERABLE || tag == Tag.INPUT_2_ITERABLE;
	}

	public boolean isInputIterator() {
		return tag == Tag.INPUT_1_ITERATOR || tag == Tag.INPUT_2_ITERATOR;
	}

	public boolean isInput1Iterable() {
		return tag == Tag.INPUT_1_ITERABLE;
	}

	public boolean isInput1Iterator() {
		return tag == Tag.INPUT_1_ITERATOR;
	}

	public boolean isIteratorTrueAssumption() {
		return tag == Tag.ITERATOR_TRUE_ASSUMPTION;
	}

	public boolean isNull() {
		return tag == Tag.NULL;
	}

	public boolean canNotContainInput() {
		return tag != Tag.INPUT && tag != Tag.CONTAINER && tag != Tag.THIS;
	}

	public boolean canContainInput() {
		return tag == Tag.INPUT || tag == Tag.CONTAINER || tag == Tag.THIS;
	}

	public boolean canContainFields() {
		return tag == Tag.CONTAINER || tag == Tag.THIS;
	}

	public boolean isCallByValue() {
		return callByValue;
	}

	public Tag getTag() {
		return tag;
	}

	public void setTag(Tag tag) {
		this.tag = tag;
		if (tag == Tag.CONTAINER || tag == Tag.THIS) {
			input = null;
			flatFieldExpr = null;
		}
		else if (tag == Tag.INPUT) {
			containerMapping = null;
		}
		else {
			input = null;
			containerMapping = null;
			flatFieldExpr = null;
		}
	}

	public String toForwardedFieldsExpression(Input input) {
		// input not relevant
		if (isInput() && this.input != input) {
			return null;
		}
		// equivalent to semantic annotation "*" for non-composite types
		else if (isInput() && flatFieldExpr.length() == 0) {
			return "*";
		}
		// equivalent to "f0.f0->*"
		else if (isInput()) {
			return flatFieldExpr + "->*";
		}
		// equivalent to "f3;f0.f0->f0.f1;f1->f2;..."
		else if (canContainFields() && containerMapping != null) {
			final StringBuilder sb = new StringBuilder();
			traverseContainer(input, containerMapping, sb, "");
			final String returnValue = sb.toString();
			if (returnValue != null && returnValue.length() > 0) {
				return returnValue;
			}
		}
		return null;
	}

	private void traverseContainer(Input input, Map<String, TaggedValue> containerMapping, StringBuilder sb,
			String prefix) {
		for (Map.Entry<String, TaggedValue> entry : containerMapping.entrySet()) {
			// skip undefined states
			if (entry.getValue() == null) {
				continue;
			}
			// input
			else if (entry.getValue().isInput() && entry.getValue().input == input) {
				final String flatFieldExpr = entry.getValue().getFlatFieldExpr();
				if (flatFieldExpr.length() == 0) {
					sb.append("*");
				}
				else {
					sb.append(flatFieldExpr);
				}
				sb.append("->");
				if (prefix.length() > 0) {
					sb.append(prefix);
					sb.append('.');
				}
				sb.append(entry.getKey());
				sb.append(';');
			}
			// input containers
			else if (entry.getValue().canContainFields()) {
				traverseContainer(input, entry.getValue().containerMapping, sb,
						((prefix.length() > 0) ? prefix + "." : "") + entry.getKey());
			}
		}
	}

	@Override
	public boolean equals(Object value) {
		if (!(value instanceof TaggedValue) || !super.equals(value)) {
			return false;
		}
		final TaggedValue other = (TaggedValue) value;
		if (other.tag != tag) {
			return false;
		}

		if (isInput()) {
			return input == other.input && flatFieldExpr.equals(other.flatFieldExpr)
					&& grouped == other.grouped && callByValue == other.callByValue;
		}
		else if (canContainFields()) {
			if ((containerMapping == null && other.containerMapping != null)
					|| (containerMapping != null && other.containerMapping == null)) {
				return false;
			}
			if (containerMapping == null) {
				return true;
			}
			return containerMapping.equals(other.containerMapping);
		}
		return tag == other.tag;
	}

	@Override
	public String toString() {
		if (isInput()) {
			return "TaggedValue(" + tag + ":" + flatFieldExpr + ")";
		}
		else if (canContainFields()) {
			return "TaggedValue(" + tag + ":" + containerMapping + ")";
		}
		else if (isIntConstant()) {
			return "TaggedValue(" + tag + ":" + intConstant + ")";
		}
		return "TaggedValue(" + tag + ")";
	}

	// --------------------------------------------------------------------------------------------
	// Input
	// --------------------------------------------------------------------------------------------

	public Input getInput() {
		return input;
	}

	public String getFlatFieldExpr() {
		return flatFieldExpr;
	}

	public boolean isGrouped() {
		return grouped;
	}

	// --------------------------------------------------------------------------------------------
	// Container & This
	// --------------------------------------------------------------------------------------------

	public Map<String, TaggedValue> getContainerMapping() {
		return containerMapping;
	}

	public boolean containerContains(String field) {
		if (containerMapping == null) {
			return false;
		}
		return containerMapping.containsKey(field);
	}

	public boolean containerHasReferences() {
		if (containerMapping == null) {
			return false;
		}
		for (TaggedValue value : containerMapping.values()) {
			if (value == null || !value.isCallByValue()) {
				return true;
			}
		}
		return false;
	}

	public void addContainerMapping(String field, TaggedValue mapping, ModifiedASMFrame frame) {
		if (containerMapping == null) {
			containerMapping = new HashMap<String, TaggedValue>(4);
		}
		if (containerFrameMapping == null) {
			containerFrameMapping = new HashMap<String, ModifiedASMFrame>(4);
		}
		if (containerMapping.containsKey(field)
				&& containerMapping.get(field) != null
				&& frame == containerFrameMapping.get(field)) {
			containerMapping.put(field, null);
			containerFrameMapping.remove(field);
		}
		else {
			containerMapping.put(field, mapping);
			containerFrameMapping.put(field, frame);
		}
	}

	public void clearContainerMappingMarkedFields() {
		if (containerMapping != null) {
			final Iterator<Entry<String, TaggedValue>> it = containerMapping.entrySet().iterator();
			while (it.hasNext()) {
				final Entry<String, TaggedValue> entry = it.next();
				if (entry.getValue() == null) {
					it.remove();
				}
			}
		}
	}

	public void makeRegular() {
		if (canContainFields() && containerMapping != null) {
			for (TaggedValue value : containerMapping.values()) {
				value.makeRegular();
			}
		}
		setTag(Tag.REGULAR);
	}

	// --------------------------------------------------------------------------------------------
	// IntConstant
	// --------------------------------------------------------------------------------------------

	public int getIntConstant() {
		return intConstant;
	}

	public TaggedValue copy() {
		return copy(getType());
	}

	public TaggedValue copy(Type type) {
		final TaggedValue newValue = new TaggedValue(type);
		newValue.tag = this.tag;
		if (isInput()) {
			newValue.input = this.input;
			newValue.flatFieldExpr = this.flatFieldExpr;
			newValue.grouped = this.grouped;
			newValue.callByValue = this.callByValue;
		}
		else if (canContainFields()) {
			final HashMap<String, TaggedValue> containerMapping = new HashMap<String, TaggedValue>(this.containerMapping.size());
			final HashMap<String, ModifiedASMFrame> containerFrameMapping;
			if (this.containerFrameMapping != null) {
				containerFrameMapping = new HashMap<String, ModifiedASMFrame>(this.containerFrameMapping.size());
			} else {
				containerFrameMapping = null;
			}
			for (Entry<String, TaggedValue> entry : this.containerMapping.entrySet()) {
				if (entry.getValue() != null) {
					containerMapping.put(entry.getKey(), entry.getValue().copy());
					if (containerFrameMapping != null) {
						containerFrameMapping.put(entry.getKey(), this.containerFrameMapping.get(entry.getKey()));
					}
				} else {
					containerMapping.put(entry.getKey(), null);
				}
			}
			newValue.containerMapping = containerMapping;
			newValue.containerFrameMapping = containerFrameMapping;
		}
		else if (isIntConstant()) {
				newValue.intConstant = this.intConstant;
		}
		return newValue;
	}
}
