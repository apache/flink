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

package org.apache.flink.table.planner.calcite;

import org.apache.flink.table.functions.SqlLikeUtils;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for the SqlLikeUtils. */
class FlinkSqlLikeUtilsTest {
    @Test
    void testSqlLike() {
        // ------------------------------------- like ---------------------------------------------

        assertThat(SqlLikeUtils.like("abc", "a.c", "\\")).isEqualTo(false);
        assertThat(SqlLikeUtils.like("a.c", "a.c", "\\")).isEqualTo(true);
        assertThat(SqlLikeUtils.like("abcd", "a.*d", "\\")).isEqualTo(false);
        assertThat(SqlLikeUtils.like("abcde", "%c.e", "\\")).isEqualTo(false);

        // default escape character
        assertThat(SqlLikeUtils.like("a-c", "a\\_c")).isEqualTo(false);
        assertThat(SqlLikeUtils.like("a_c", "a\\_c")).isEqualTo(true);

        // -------------------------------- sqlToRegexLike ----------------------------------------

        assertThat(SqlLikeUtils.sqlToRegexLike(".", "\\")).isEqualTo("\\.");
        assertThat(SqlLikeUtils.sqlToRegexLike("c", "\\")).isEqualTo("c");
        assertThat(SqlLikeUtils.sqlToRegexLike("_", "\\")).isEqualTo(".");
        assertThat(SqlLikeUtils.sqlToRegexLike("%", "\\")).isEqualTo("(?s:.*)");

        // escape
        assertThat(SqlLikeUtils.sqlToRegexLike("\\_", "\\")).isEqualTo("_");
        assertThat(SqlLikeUtils.sqlToRegexLike("\\%", "\\")).isEqualTo("%");
        assertThat(SqlLikeUtils.sqlToRegexLike("\\\\", "\\")).isEqualTo("\\\\");
        assertThat(SqlLikeUtils.sqlToRegexLike("&&", "&")).isEqualTo("&");

        // exception
        assertThatThrownBy(() -> SqlLikeUtils.sqlToRegexLike("\\a", "\\"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Invalid escape");
        assertThatThrownBy(() -> SqlLikeUtils.sqlToRegexLike("\\", "\\"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Invalid escape");

        // --------------------------------- similar ----------------------------------------------

        assertThat(SqlLikeUtils.similar("abc", "a.c", "\\")).isEqualTo(true);
        assertThat(SqlLikeUtils.similar("a.c", "a.c", "\\")).isEqualTo(true);
        assertThat(SqlLikeUtils.similar("abcd", "a.*d", "\\")).isEqualTo(true);
    }
}
