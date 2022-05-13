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

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import java.io.IOException;

/**
 * InputFormat to read data from Apache Cassandra and generate ${@link Tuple}.
 *
 * @param <OUT> type of Tuple
 */
public class CassandraInputFormat<OUT extends Tuple> extends CassandraInputFormatBase<OUT> {

    private static final long serialVersionUID = 3642323148032444264L;
    private transient ResultSet resultSet;

    public CassandraInputFormat(String query, ClusterBuilder builder) {
        super(query, builder);
    }

    /**
     * Opens a Session and executes the query.
     *
     * @param ignored because parameter is not parallelizable.
     * @throws IOException
     */
    @Override
    public void open(InputSplit ignored) throws IOException {
        this.session = cluster.connect();
        this.resultSet = session.execute(query);
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return resultSet.isExhausted();
    }

    @Override
    public OUT nextRecord(OUT reuse) throws IOException {
        final Row item = resultSet.one();
        for (int i = 0; i < reuse.getArity(); i++) {
            reuse.setField(item.getObject(i), i);
        }
        return reuse;
    }
}
