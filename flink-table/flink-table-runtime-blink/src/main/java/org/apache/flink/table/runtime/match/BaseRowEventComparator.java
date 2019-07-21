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

package org.apache.flink.table.runtime.match;

import org.apache.flink.cep.EventComparator;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.generated.GeneratedRecordComparator;
import org.apache.flink.table.generated.RecordComparator;

/**
 * An implementation of {@link EventComparator} based on a generated {@link RecordComparator}.
 */
public class BaseRowEventComparator implements EventComparator<BaseRow> {
	private static final long serialVersionUID = 1L;

	private final GeneratedRecordComparator generatedComparator;

	private transient RecordComparator comparator;

	public BaseRowEventComparator(GeneratedRecordComparator generatedComparator) {
		this.generatedComparator = generatedComparator;
	}

	@Override
	public int compare(BaseRow row1, BaseRow row2) {
		if (comparator == null) {
			comparator = generatedComparator.newInstance(
				Thread.currentThread().getContextClassLoader());
		}
		return comparator.compare(row1, row2);
	}
}
