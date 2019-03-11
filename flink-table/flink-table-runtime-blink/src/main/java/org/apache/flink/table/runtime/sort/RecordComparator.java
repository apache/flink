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

package org.apache.flink.table.runtime.sort;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.dataformat.BaseRow;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Record comparator for {@link BinaryInMemorySortBuffer}.
 * For performance, subclasses are usually implemented through CodeGenerator.
 */
public abstract class RecordComparator implements Comparator<BaseRow>, Serializable {

	private static final long serialVersionUID = 1L;

	protected TypeSerializer[] serializers;
	protected TypeComparator[] comparators;

	public void init(TypeSerializer[] serializers, TypeComparator[] comparators) {
		this.serializers = serializers;
		this.comparators = comparators;
	}

	@Override
	public abstract int compare(BaseRow o1, BaseRow o2);
}
