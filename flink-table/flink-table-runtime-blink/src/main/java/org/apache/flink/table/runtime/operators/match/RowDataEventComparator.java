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

package org.apache.flink.table.runtime.operators.match;

import org.apache.flink.cep.EventComparator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.RecordComparator;

/**
 * An implementation of {@link EventComparator} based on a generated {@link RecordComparator}.
 */
public class RowDataEventComparator implements EventComparator<RowData> {
	private static final long serialVersionUID = 1L;

	private final GeneratedRecordComparator generatedComparator;

	private transient RecordComparator comparator;

	public RowDataEventComparator(GeneratedRecordComparator generatedComparator) {
		this.generatedComparator = generatedComparator;
	}

	@Override
	public int compare(RowData row1, RowData row2) {
		if (comparator == null) {
			comparator = generatedComparator.newInstance(
				Thread.currentThread().getContextClassLoader());
		}
		return comparator.compare(row1, row2);
	}
}
