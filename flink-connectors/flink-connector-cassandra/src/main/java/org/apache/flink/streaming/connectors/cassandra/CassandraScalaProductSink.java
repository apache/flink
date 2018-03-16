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

package org.apache.flink.streaming.connectors.cassandra;

import scala.Product;

/**
 * Sink to write scala tuples and case classes into a Cassandra cluster.
 *
 * @param <IN> Type of the elements emitted by this sink, it must extend {@link Product}
 */
public class CassandraScalaProductSink<IN extends Product> extends AbstractCassandraTupleSink<IN> {
	public CassandraScalaProductSink(String insertQuery, ClusterBuilder builder) {
		super(insertQuery, builder);
	}

	@Override
	protected Object[] extract(IN record) {
		Object[] al = new Object[record.productArity()];
		for (int i = 0; i < record.productArity(); i++) {
			al[i] = record.productElement(i);
		}
		return al;
	}
}
