package org.apache.flink.table.planner.calcite;

import org.apache.flink.table.functions.SqlLikeUtils;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for the SqlLikeUtils. */
public class FlinkSqlLikeUtilsTest {
    @Test
    public void testSqlLike() {
        assertThat(SqlLikeUtils.like("abc", "a.c", "\\")).isEqualTo(false);
        assertThat(SqlLikeUtils.like("a.c", "a.c", "\\")).isEqualTo(true);
        assertThat(SqlLikeUtils.like("abcd", "a.*d", "\\")).isEqualTo(false);
        assertThat(SqlLikeUtils.like("abcde", "%c.e", "\\")).isEqualTo(false);

        assertThat(SqlLikeUtils.similar("abc", "a.c", "\\")).isEqualTo(true);
        assertThat(SqlLikeUtils.similar("a.c", "a.c", "\\")).isEqualTo(true);
        assertThat(SqlLikeUtils.similar("abcd", "a.*d", "\\")).isEqualTo(true);
    }
}
