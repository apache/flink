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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.utils.JavaStreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for recursive with. */
class RecursiveWithTest extends TableTestBase {
    private final JavaStreamTableTestUtil util = javaStreamTestUtil();

    @Test
    void testRecursive() {
        // so far it is not supported
        assertThatThrownBy(
                        () ->
                                util.verifyExecPlan(
                                        "WITH RECURSIVE a(x) AS\n"
                                                + "  (SELECT 1),\n"
                                                + "               b(y) AS\n"
                                                + "  (SELECT x\n"
                                                + "   FROM a\n"
                                                + "   UNION ALL SELECT y + 1\n"
                                                + "   FROM b\n"
                                                + "   WHERE y < 2),\n"
                                                + "               c(z) AS\n"
                                                + "  (SELECT y\n"
                                                + "   FROM b\n"
                                                + "   UNION ALL SELECT z * 4\n"
                                                + "   FROM c\n"
                                                + "   WHERE z < 4 )\n"
                                                + "SELECT *\n"
                                                + "FROM a,\n"
                                                + "     b,\n"
                                                + "     c"))
                .isInstanceOf(TableException.class);
    }
}
