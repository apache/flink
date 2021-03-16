package org.apache.flink.table.planner.runtime.stream.jsonplan;

import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.TestData;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.JsonPlanTestBase;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

/** Test json serialization/deserialization for union. */
public class UnionJsonPlanITCase extends JsonPlanTestBase {
    @Test
    public void testUnion() throws ExecutionException, InterruptedException, IOException {
        createTestValuesSourceTable(
                "MyTable",
                JavaScalaConversionUtil.toJava(TestData.data1()),
                "a int",
                "b varchar",
                "c int");
        createTestValuesSourceTable(
                "MyTable2",
                JavaScalaConversionUtil.toJava(TestData.data1()),
                "a int",
                "b varchar",
                "c int");
        createTestNonInsertOnlyValuesSinkTable("MySink", "a int", "b varchar", "c bigint");

        String dml =
                "INSERT INTO MySink "
                        + "(SELECT * FROM MyTable where a >=3)"
                        + "     union all (select * from MyTable2 where a <= 3)";
        executeSqlWithJsonPlanVerified(dml).await();
        List<String> expected =
                Arrays.asList(
                        "+I[2, a, 6]",
                        "+I[4, b, 8]",
                        "+I[6, c, 10]",
                        "+I[1, a, 5]",
                        "+I[3, b, 7]",
                        "+I[5, c, 9]",
                        "+I[3, b, 7]" // a=3 need to be doubled
                        );
        assertResult(expected, TestValuesTableFactory.getResults("MySink"));
    }
}
