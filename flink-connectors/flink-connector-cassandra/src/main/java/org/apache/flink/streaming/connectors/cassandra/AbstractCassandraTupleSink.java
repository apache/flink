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

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.flink.configuration.Configuration;

/**
 * Abstract sink to write tuple-like values into a Cassandra cluster.
 *
 * @param <IN> Type of the elements emitted by this sink
 */
public abstract class AbstractCassandraTupleSink<IN> extends CassandraSinkBase<IN, ResultSet> {
	private final String insertQuery;
	private transient PreparedStatement ps;

	public AbstractCassandraTupleSink(String insertQuery, ClusterBuilder builder) {
		super(builder);
		this.insertQuery = insertQuery;
	}

	@Override
	public void open(Configuration configuration) {
		super.open(configuration);
		this.ps = session.prepare(insertQuery);
	}

	@Override
	public ListenableFuture<ResultSet> send(IN value) {
		Object[] fields = extract(value);
		return session.executeAsync(ps.bind(fields));
	}

	protected abstract Object[] extract(IN record);
}
