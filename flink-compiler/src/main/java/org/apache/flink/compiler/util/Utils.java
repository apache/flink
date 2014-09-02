/**
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


package org.apache.flink.compiler.util;

import java.util.Arrays;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.compiler.CompilerException;


/**
 * 
 */
public class Utils
{
	public static final FieldList createOrderedFromSet(FieldSet set) {
		if (set instanceof FieldList) {
			return (FieldList) set;
		} else {
			final int[] cols = set.toArray();
			Arrays.sort(cols);
			return new FieldList(cols);
		}
	}
	
	public static final Ordering createOrdering(FieldList fields, boolean[] directions) {
		final Ordering o = new Ordering();
		for (int i = 0; i < fields.size(); i++) {
			o.appendOrdering(fields.get(i), null, directions == null || directions[i] ? Order.ASCENDING : Order.DESCENDING);
		}
		return o;
	}
	
	public static final Ordering createOrdering(FieldList fields) {
		final Ordering o = new Ordering();
		for (int i = 0; i < fields.size(); i++) {
			o.appendOrdering(fields.get(i), null, Order.ANY);
		}
		return o;
	}
	
	public static boolean[] getDirections(Ordering o, int numFields) {
		final boolean[] dirs = o.getFieldSortDirections();
		if (dirs.length == numFields) {
			return dirs;
		} else if (dirs.length > numFields) {
			final boolean[] subSet = new boolean[numFields];
			System.arraycopy(dirs, 0, subSet, 0, numFields);
			return subSet;
		} else {
			throw new CompilerException();
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * No instantiation.
	 */
	private Utils() {}
}
