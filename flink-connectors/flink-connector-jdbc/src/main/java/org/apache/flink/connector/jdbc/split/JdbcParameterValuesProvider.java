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

package org.apache.flink.connector.jdbc.split;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.connector.jdbc.JdbcInputFormat;

import java.io.Serializable;

/**
 * This interface is used by the {@link JdbcInputFormat} to compute the list of parallel query to
 * run (i.e. splits). Each query will be parameterized using a row of the matrix provided by each
 * {@link JdbcParameterValuesProvider} implementation.
 */
@Experimental
public interface JdbcParameterValuesProvider {

    /** Returns the necessary parameters array to use for query in parallel a table. */
    Serializable[][] getParameterValues();
}
