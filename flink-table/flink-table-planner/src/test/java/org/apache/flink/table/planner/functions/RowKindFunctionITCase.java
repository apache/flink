package org.apache.flink.table.planner.functions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.api.Expressions.rowKind;

public class RowKindFunctionITCase extends BuiltInFunctionTestBase{

    @Parameterized.Parameters(name = "{index}: {0}")
    public static List<TestSpec> testData() {
        return Arrays.asList(
                TestSpec.forFunction(BuiltInFunctionDefinitions.ROW_KIND, "get 'RowKind' of data")
                        .onFieldsWithData("none")
                        .andDataTypes(DataTypes.STRING())
                        .testTableApiResult(
                                rowKind(),
                                "+I",
                                DataTypes.CHAR(2).notNull()
                        )
                        .testSqlResult(
                                "ROW_KIND()",
                                "+I",
                                DataTypes.CHAR(2).notNull()
                        )
        );
    }
}
