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

package org.apache.flink.connectors.cassandra.batch;

import java.io.IOException;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.cassandra.streaming.ClusterConfigurator;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

public abstract class CassandraOutputFormat<OUT extends Tuple> extends
		RichOutputFormat<OUT> implements ClusterConfigurator {

	private static final long serialVersionUID = 1L;

	private final String insertQuery;

	private transient Cluster cluster;
	private transient Session session;
	private transient PreparedStatement ps;
	private transient FutureCallback<ResultSet> callback;
	private transient Throwable asyncException = null;

	public CassandraOutputFormat(String insertQuery) {
		if(Strings.isNullOrEmpty(insertQuery)){
			throw new IllegalArgumentException("insertQuery cannot be null or empty");
		}
		this.insertQuery = insertQuery;
	}

	@Override
	public void configure(Configuration parameters) {
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {

		this.cluster = configureCluster(Cluster.builder()).build();
		this.session = cluster.connect();
		this.ps = session.prepare(insertQuery);
		this.callback = new FutureCallback<ResultSet>() {

			@Override
			public void onSuccess(ResultSet ignored) {
			}

			@Override
			public void onFailure(Throwable t) {
				asyncException = t;
			}
		};
	}

	@Override
	public void writeRecord(OUT record) throws IOException {
		checkException();
		Object[] fields = extract(record);
		ResultSetFuture result = session.executeAsync(ps.bind(fields));
		catchException(result);
	}

	protected void checkException() throws IOException {
		if (asyncException != null) {
			throw new IOException("write record failed", asyncException);
		}
	}
	
	protected void catchException(ResultSetFuture result){
		Futures.addCallback(result, callback);
	}

	private Object[] extract(OUT record) {
		Object[] al = new Object[record.getArity()];
		for (int i = 0; i < record.getArity(); i++) {
			al[i] = record.getField(i);
		}
		return al;
	}

	@Override
	public void close() throws IOException {
		session.close();
		cluster.close();
	}
}