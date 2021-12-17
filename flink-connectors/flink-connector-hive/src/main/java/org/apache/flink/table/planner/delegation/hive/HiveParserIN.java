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

package org.apache.flink.table.planner.delegation.hive;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.ReturnTypes;

/**
 * The SqlOperator for GenericUDFIn. We cannot use HiveIn because Calcite-1.26 requires only
 * RexSubQuery can have SqlKind.IN.
 */
public class HiveParserIN extends SqlSpecialOperator {

    public static final HiveParserIN INSTANCE = new HiveParserIN();

    private HiveParserIN() {
        super(
                "IN",
                SqlKind.OTHER_FUNCTION,
                30,
                true,
                ReturnTypes.BOOLEAN_NULLABLE,
                InferTypes.FIRST_KNOWN,
                null);
    }
}
