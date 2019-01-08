/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.cassandra;

import org.apache.flink.types.Row;

/**
 * A SinkFunction to write Row records into a Cassandra table.
 */
public class CassandraRowSink extends AbstractCassandraTupleSink<Row> {

	private final int rowArity;

	public CassandraRowSink(int rowArity, String insertQuery, ClusterBuilder builder) {
		super(insertQuery, builder);
		this.rowArity = rowArity;
	}

	@Override
	protected Object[] extract(Row record) {
		Object[] al = new Object[rowArity];
		for (int i = 0; i < rowArity; i++) {
			al[i] = record.getField(i);
		}
		return al;
	}
}
