/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.range;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.java.typeutils.runtime.NullAwareComparator;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.TypeGetterSetters;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * for extract keys.
 */
public class KeyExtractor implements Serializable {

	private final int[] keyPositions;
	private final InternalType[] types;
	private final NullAwareComparator[] nullAwareComparators;

	public KeyExtractor(int[] keyPositions, boolean[] orders, InternalType[] types,
			TypeComparator[] comparators) {
		this.keyPositions = keyPositions;
		this.types = types;
		this.nullAwareComparators = new NullAwareComparator[comparators.length];
		for (int i = 0; i < nullAwareComparators.length; i++) {
			nullAwareComparators[i] = new NullAwareComparator<>(comparators[i], orders[i]);
		}
	}

	public TypeComparator[] getFlatComparators() {
		List<TypeComparator> flatComparators = new LinkedList<>();
		for (TypeComparator c : nullAwareComparators) {
			Collections.addAll(flatComparators, c.getFlatComparators());
		}
		return flatComparators.toArray(new TypeComparator[0]);
	}

	public int extractKeys(BaseRow record, Object[] target, int index) {
		int len = nullAwareComparators.length;
		int localIndex = index;
		for (int i = 0; i < len; i++) {
			int pos = keyPositions[i];
			Object element = record.isNullAt(pos) ? null : TypeGetterSetters.get(record, pos, types[pos]);
			localIndex += nullAwareComparators[i].extractKeys(element, target, localIndex);
		}
		return localIndex - index;
	}
}
