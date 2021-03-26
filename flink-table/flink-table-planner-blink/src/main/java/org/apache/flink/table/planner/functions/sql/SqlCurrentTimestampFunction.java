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

package org.apache.flink.table.planner.functions.sql;

import org.apache.calcite.sql.fun.SqlAbstractTimeFunction;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Function that returns current timestamp, the function return type is {@link
 * SqlTypeName#TIMESTAMP_WITH_LOCAL_TIME_ZONE}.
 */
public class SqlCurrentTimestampFunction extends SqlAbstractTimeFunction {

    public SqlCurrentTimestampFunction(String name) {
        // access protected constructor
        super(name, SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }
}
