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

package org.apache.flink.connector.jdbc.xa.h2;

import org.apache.flink.connector.jdbc.DbMetadata;

import javax.sql.XADataSource;

/** H2DbMetadata. */
public class H2DbMetadata implements DbMetadata {

    private final String schema;

    public H2DbMetadata(String schema) {
        this.schema = schema;
    }

    @Override
    public XADataSource buildXaDataSource() {
        final org.h2.jdbcx.JdbcDataSource ds = new org.h2.jdbcx.JdbcDataSource();
        ds.setUrl(getUrl());
        return new H2XaDsWrapper(ds);
    }

    @Override
    public String getDriverClass() {
        return "org.h2.Driver";
    }

    @Override
    public String getUrl() {
        return String.format("jdbc:h2:mem:%s;INIT=SET SCHEMA %s", schema, schema);
    }

    @Override
    public String getInitUrl() {
        return String.format(
                "jdbc:h2:mem:%s;DB_CLOSE_DELAY=-1;INIT=CREATE SCHEMA IF NOT EXISTS %s\\;SET SCHEMA %s",
                schema, schema, schema);
    }
}
