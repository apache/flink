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
package org.apache.flink.table.plan.logical.rel.util;

import org.apache.calcite.rel.core.Window.Group;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.types.Row;

public class StreamGroupKeySelector implements KeySelector<Object, Tuple> {

	static final long serialVersionUID = 5268295970857329151L;
	WindowAggregateUtil winUtil;
	Group group;

	public StreamGroupKeySelector(Group group) {
		this.group = group;
		winUtil = new WindowAggregateUtil();
	}

	@Override
	public Tuple getKey(Object value) throws Exception {
		Row row = (Row) value;
		Tuple key = Tuple.getTupleClass(
				winUtil.
				getKeysAsArray(group).
				length).
				newInstance();

		for (Integer idx : winUtil.getKeysAsArray(group)) {
			key.setField(row.getField(idx), idx);
		}

		return key;
	}

}
