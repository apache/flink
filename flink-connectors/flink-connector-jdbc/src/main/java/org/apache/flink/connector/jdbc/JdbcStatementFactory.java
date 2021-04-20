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

package org.apache.flink.connector.jdbc;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.util.function.Function;

/**
 * Sets {@link PreparedStatement} table name to use in JDBC Sink.
 *
 * @see JdbcBatchStatementExecutor
 */
@PublicEvolving
public interface JdbcStatementFactory<T> extends Function<T, String>, Serializable {}
