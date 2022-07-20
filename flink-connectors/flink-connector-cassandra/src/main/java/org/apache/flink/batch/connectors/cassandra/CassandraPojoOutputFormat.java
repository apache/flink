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

package org.apache.flink.batch.connectors.cassandra;

import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.streaming.connectors.cassandra.MapperOptions;
import org.apache.flink.util.Preconditions;

import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.util.concurrent.ListenableFuture;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

/**
 * OutputFormat to write data to Apache Cassandra and from a custom Cassandra annotated object.
 * Please read the recommendations in {@linkplain CassandraOutputFormatBase}.
 *
 * @param <OUT> type of outputClass
 */
public class CassandraPojoOutputFormat<OUT> extends CassandraOutputFormatBase<OUT, Void> {

    private static final long serialVersionUID = -1701885135103942460L;

    private final MapperOptions mapperOptions;
    private final Class<OUT> outputClass;
    private transient Mapper<OUT> mapper;

    public CassandraPojoOutputFormat(ClusterBuilder builder, Class<OUT> outputClass) {
        this(builder, outputClass, null);
    }

    public CassandraPojoOutputFormat(
            ClusterBuilder builder, Class<OUT> outputClass, MapperOptions mapperOptions) {
        this(
                builder,
                outputClass,
                mapperOptions,
                Integer.MAX_VALUE,
                Duration.ofMillis(Long.MAX_VALUE));
    }

    public CassandraPojoOutputFormat(
            ClusterBuilder builder,
            Class<OUT> outputClass,
            MapperOptions mapperOptions,
            int maxConcurrentRequests,
            Duration maxConcurrentRequestsTimeout) {
        super(builder, maxConcurrentRequests, maxConcurrentRequestsTimeout);
        Preconditions.checkNotNull(outputClass, "OutputClass cannot be null");
        this.mapperOptions = mapperOptions;
        this.outputClass = outputClass;
    }

    /** Opens a Session to Cassandra and initializes the prepared statement. */
    @Override
    protected void postOpen() {
        super.postOpen();
        MappingManager mappingManager = new MappingManager(session);
        this.mapper = mappingManager.mapper(outputClass);
        if (mapperOptions != null) {
            Mapper.Option[] optionsArray = mapperOptions.getMapperOptions();
            if (optionsArray != null) {
                mapper.setDefaultSaveOptions(optionsArray);
            }
        }
    }

    @Override
    protected CompletionStage<Void> send(OUT record) {
        final ListenableFuture<Void> result = mapper.saveAsync(record);
        return listenableFutureToCompletableFuture(result);
    }

    /** Closes all resources used. */
    @Override
    protected void postClose() {
        super.postClose();
        mapper = null;
    }
}
