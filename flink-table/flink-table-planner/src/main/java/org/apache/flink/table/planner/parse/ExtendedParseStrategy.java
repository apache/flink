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

package org.apache.flink.table.planner.parse;

import org.apache.flink.table.operations.Operation;

/**
 * Strategy to parse statement to {@link Operation}. parsing some special command which can't
 * supported by {@link CalciteParser}, e.g. {@code SET key=value} may contain special characters in
 * key and value.
 */
public interface ExtendedParseStrategy {

    /** Determine whether the input statement is satisfied the strategy. */
    boolean match(String statement);

    /** Convert the input statement to the {@link Operation}. */
    Operation convert(String statement);

    /** Return hints for the given statement. */
    String[] getHints();
}
