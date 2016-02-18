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

package org.apache.flink.connectors.cassandra.streaming;

import java.io.IOException;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 *	BaseCassandraSink is the common abstract class of {@link CassandraMapperSink} and {@link CassandraSink}.
 *
 *	The {@link Cluster} is built via {@link ClusterConfigurator#configureCluster(Cluster.Builder cluster) configureCluster}
 *	inherited by {@link ClusterConfigurator}
 *
 *	The {@link Session} is liable to maintain the connection between our Sink and the Cassandra Cluster.
 *
 * @param <IN> Type of the elements emitted by this sink
 */
public abstract class BaseCassandraSink<IN,V> extends RichSinkFunction<IN> implements ClusterConfigurator {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(BaseCassandraSink.class);

	protected final String createQuery;
	protected transient Cluster cluster;
	protected transient Session session;
	protected transient Throwable asyncException = null;
	protected transient FutureCallback<V> callback;

	public BaseCassandraSink(String createQuery){
		this.createQuery = createQuery;
	}
	
	@Override
	public void open(Configuration configuration) {

		this.cluster = configureCluster(Cluster.builder()).build();
		this.session = cluster.connect();

		this.callback = new FutureCallback<V>() {

			@Override
			public void onSuccess(V ignored) {
			}

			@Override
			public void onFailure(Throwable t) {
				asyncException = t;
			}
		};

		if (LOG.isInfoEnabled()) {
			LOG.info("Cluster connection to Cassandra has been open. State: {} ",session.getState());
		}

		//  execute createQuery with only parallelism of 1
		if(createQuery != null){
			int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
			if(parallelism  == 1) {
				session.execute(createQuery);
			} else {
				throw new RuntimeException("It is allowed to create table only with parallelism of 1");
			}
		}
	}
	
	@Override
	public void invoke(IN value) throws Exception{
		checkException();
		ListenableFuture<V> result = send(value);
		catchException(result);
	}

	public abstract ListenableFuture<V> send(IN value);
	
	@Override
	public void close() {
		try {
			session.close();
		} catch (Exception e) {
			LOG.error("Error while closing session.", e);
		}
		try {
			cluster.close();
		} catch (Exception e) {
			LOG.error("Error while closing cluster.", e);
		}
	}
	
	protected void checkException() throws IOException{
		if(asyncException != null) {
			throw new IOException("invoke() failed", asyncException);
		}
	}
	
	protected void catchException(ListenableFuture<V> future) {
		Futures.addCallback(future, callback);
	}
	
	protected void checkNullOrEmpty(String s, String message){
		if(Strings.isNullOrEmpty(s)){
			throw new IllegalArgumentException(message);
		}
	}
}