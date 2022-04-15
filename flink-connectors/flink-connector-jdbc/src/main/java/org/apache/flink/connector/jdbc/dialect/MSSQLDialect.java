package org.apache.flink.connector.jdbc.dialect;


import org.apache.commons.lang3.StringUtils;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.internal.converter.MSSQLRowConverter;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class MSSQLDialect extends AbstractDialect {

    private static final long serialVersionUID = 1L;

    // Define MAX/MIN precision of TIMESTAMP type according to Mysql docs:
    // https://dev.mysql.com/doc/refman/8.0/en/fractional-seconds.html
    private static final int MAX_TIMESTAMP_PRECISION = 6;
    private static final int MIN_TIMESTAMP_PRECISION = 1;

    // Define MAX/MIN precision of DECIMAL type according to Mysql docs:
    // https://dev.mysql.com/doc/refman/8.0/en/fixed-point-types.html
    private static final int MAX_DECIMAL_PRECISION = 65;
    private static final int MIN_DECIMAL_PRECISION = 1;
    @Override
    public String dialectName() {
        return "MSSQL";
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:sqlserver:");
    }

    @Override
    public JdbcRowConverter getRowConverter(RowType rowType) {
        return new MSSQLRowConverter(rowType);
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("com.microsoft.sqlserver.jdbc.SQLServerDriver");
    }

    @Override
    public String getLimitClause(long limit) {
        return "#SELECT TOP " + limit;
    }
    @Override
    public String quoteIdentifier(String identifier) {
        return "[" + identifier + "]";
    }

    @Override
    public Optional<String> getUpsertStatement(
        String tableName, String[] fieldNames, String[] uniqueKeyFields) {

        StringBuilder sb = new StringBuilder();
        sb.append(
            "MERGE INTO "
                + tableName
                + " T1 USING "
                + "("
                + buildDualQueryStatement(fieldNames)
                + ") T2 ON ("
                + buildConnectionConditions(uniqueKeyFields)
                + ") ");

        String updateSql = buildUpdateConnection(fieldNames, uniqueKeyFields, true);

        if (StringUtils.isNotEmpty(updateSql)) {
            sb.append(" WHEN MATCHED THEN UPDATE SET ");
            sb.append(updateSql);
        }

        sb.append(
            " WHEN NOT MATCHED THEN "
                + "INSERT ("
                + Arrays.stream(fieldNames)
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(","))
                + ") VALUES ("
                + Arrays.stream(fieldNames)
                .map(col -> "T2." + quoteIdentifier(col))
                .collect(Collectors.joining(","))
                + ")");
        sb.append(";");
        return Optional.of(sb.toString());
    }

    /**
     * build T1."A"=T2."A" or T1."A"=nvl(T2."A",T1."A")
     *
     * @param fieldNames
     * @param uniqueKeyFields
     * @param allReplace
     * @return
     */
    private String buildUpdateConnection(
        String[] fieldNames, String[] uniqueKeyFields, boolean allReplace) {
        List<String> uniqueKeyList = Arrays.asList(uniqueKeyFields);
        return Arrays.stream(fieldNames)
            .filter(col -> !uniqueKeyList.contains(col))
            .map(
                col -> {
                    return allReplace
                        ? quoteIdentifier("T1")
                        + "."
                        + quoteIdentifier(col)
                        + " ="
                        + quoteIdentifier("T2")
                        + "."
                        + quoteIdentifier(col)
                        : quoteIdentifier("T1")
                        + "."
                        + quoteIdentifier(col)
                        + " =ISNULL("
                        + quoteIdentifier("T2")
                        + "."
                        + quoteIdentifier(col)
                        + ","
                        + quoteIdentifier("T1")
                        + "."
                        + quoteIdentifier(col)
                        + ")";
                })
            .collect(Collectors.joining(","));
    }

    private String buildConnectionConditions(String[] uniqueKeyFields) {
        return Arrays.stream(uniqueKeyFields)
            .map(col -> "T1." + quoteIdentifier(col) + "=T2." + quoteIdentifier(col))
            .collect(Collectors.joining(","));
    }

    /**
     * build select sql , such as (SELECT ? "A",? "B" FROM DUAL)
     *
     * @param column destination column
     * @return
     */
    public String buildDualQueryStatement(String[] column) {
        StringBuilder sb = new StringBuilder("SELECT ");
        String collect =
            Arrays.stream(column)
                .map(col -> ":" + quoteIdentifier(col) + " " + quoteIdentifier(col))
                .collect(Collectors.joining(", "));
        sb.append(collect);
        return sb.toString();
    }

    @Override
    public int maxDecimalPrecision() {
        return MAX_DECIMAL_PRECISION;
    }

    @Override
    public int minDecimalPrecision() {
        return MIN_DECIMAL_PRECISION;
    }

    @Override
    public int maxTimestampPrecision() {
        return MAX_TIMESTAMP_PRECISION;
    }

    @Override
    public int minTimestampPrecision() {
        return MIN_TIMESTAMP_PRECISION;
    }

    @Override
    public List<LogicalTypeRoot> unsupportedTypes() {
        return Arrays.asList(
            LogicalTypeRoot.BINARY,
            LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
            LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE,
            LogicalTypeRoot.INTERVAL_YEAR_MONTH,
            LogicalTypeRoot.INTERVAL_DAY_TIME,
            LogicalTypeRoot.ARRAY,
            LogicalTypeRoot.MULTISET,
            LogicalTypeRoot.MAP,
            LogicalTypeRoot.ROW,
            LogicalTypeRoot.DISTINCT_TYPE,
            LogicalTypeRoot.STRUCTURED_TYPE,
            LogicalTypeRoot.NULL,
            LogicalTypeRoot.RAW,
            LogicalTypeRoot.SYMBOL,
            LogicalTypeRoot.UNRESOLVED);
    }
}
