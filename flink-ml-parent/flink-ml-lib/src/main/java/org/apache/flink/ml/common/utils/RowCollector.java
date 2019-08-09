/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.common.utils;

import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Collector of Row type data.
 */
public class RowCollector implements Collector<Row> {
	private List<Row> rows;

	public RowCollector() {
		this(null);
	}

	public RowCollector(List<Row> rows) {
		this.rows = rows;
		if (null == this.rows) {
			this.rows = new ArrayList<>();
		}
	}

	/**
	 * Get the collected rows.
	 *
	 * @return list of the collected rows
	 */
	public List<Row> getRows() {
		return rows;
	}

	@Override
	public void collect(Row row) {
		rows.add(row);
	}

	@Override
	public void close() {
		rows.clear();
	}

	/**
	 * Removes all of the rows from this collector.
	 */
	public void clear() {
		rows.clear();
	}
}
