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

import org.apache.flink.configuration.Configuration;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.annotations.Table;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nullable;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * Flink Sink to save data into a Cassandra cluster using
 * <a href="http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/mapping/Mapper.html">Mapper</a>,
 * which it uses annotations from
 * <a href="http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/mapping/annotations/package-summary.html">
 * com.datastax.driver.mapping.annotations</a>.
 *
 * @param <IN> Type of the elements emitted by this sink
 */
public class CassandraPojoSink<IN> extends CassandraSinkBase<IN, ResultSet> {

	private static final long serialVersionUID = 1L;
	private String defaultKeyspace;

	protected final Class<IN> clazz;
	private final MapperOptions options;
	protected transient Mapper<IN> mapper;
	protected transient MappingManager mappingManager;

	/**
	 * The main constructor for creating CassandraPojoSink.
	 *
	 * @param clazz Class instance
	 */
	public CassandraPojoSink(Class<IN> clazz, ClusterBuilder builder) {
		this(clazz, builder, null);
	}

	public CassandraPojoSink(Class<IN> clazz, ClusterBuilder builder, @Nullable MapperOptions options) {
		super(builder);
		this.clazz = clazz;
		this.options = options;
	}

	/**
	 * Defines what a keyspace should be used in case the annotated POJO doesn't contain keyspace information.  This can be useful
	 * 		for testing environment where multiple environments may be pointed to single Cassandra, differentiating only by keyspace.
	 * @param defaultKeyspace
	 * @return
	 */
	public CassandraPojoSink defaultKeyspace(String defaultKeyspace) {
		this.defaultKeyspace = defaultKeyspace;
		return this;
	}

	@Override
	public void open(Configuration configuration) {
		super.open(configuration);
		try {
			this.mappingManager = new MappingManager(session);
			if (defaultKeyspace != null && !defaultKeyspace.isEmpty()) {
				applyDefaultKeyspace();
			}
			this.mapper = mappingManager.mapper(clazz);
			if (options != null) {
				Mapper.Option[] optionsArray = options.getMapperOptions();
				if (optionsArray != null) {
					this.mapper.setDefaultSaveOptions(optionsArray);
				}
			}
		} catch (Exception e) {
			throw new RuntimeException("Cannot create CassandraPojoSink with input: " + clazz.getSimpleName(), e);
		}
	}

	@Override
	public ListenableFuture<ResultSet> send(IN value) {
		return session.executeAsync(mapper.saveQuery(value));
	}

	/**
	 * This method examines the value of the "keyspace" portion of the {@link Table} annotation.  If this value is not set, it will be
	 * 		populated with the provided defaultKeyspace property
	 * @throws Exception
	 */
	private void applyDefaultKeyspace() throws Exception {
		Annotation tableAnnotation = clazz.getAnnotation(Table.class);
		Method keyspaceMethod = tableAnnotation.getClass().getDeclaredMethod("keyspace");
		keyspaceMethod.setAccessible(true);
		String keyspaceActualValue = (String) keyspaceMethod.invoke(tableAnnotation);
		if (keyspaceActualValue == null || keyspaceActualValue.isEmpty()) {
			Field overwriteValue = keyspaceActualValue.getClass().getDeclaredField("value");
			overwriteValue.setAccessible(true);
			overwriteValue.set(keyspaceActualValue, defaultKeyspace.toCharArray());
		} else {
			String message = String.format("%s already has a defined keyspace. Did not write to provided default", clazz.getName());
			log.warn(message);
		}
	}
}
