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

package org.apache.flink.table.planner.plan.stream.sql;

import org.apache.flink.connector.file.table.LegacyTableFactory;
import org.apache.flink.table.planner.utils.JavaStreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.Test;

/** Tests for usages of {@link LegacyTableFactory}. */
public class LegacyTableFactoryTest extends TableTestBase {

    private final JavaStreamTableTestUtil util;

    public LegacyTableFactoryTest() {
        util = javaStreamTestUtil();
        util.tableEnv().executeSql("CREATE TABLE T (a INT) WITH ('type'='legacy')");
    }

    @Test
    public void testSelect() {
        util.verifyExecPlan("SELECT * FROM T");
    }

    @Test
    public void testInsert() {
        util.verifyExecPlanInsert("INSERT INTO T VALUES (1)");
    }
}
