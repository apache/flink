package org.apache.flink.connector.jdbc;

import org.apache.flink.util.StringUtils;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;

public class OracleTablePath {


    private static final String DEFAULT_ORACLE_SCHEMA_NAME = "public";

    private final String oracleSchemaName;
    private final String oracleTableName;

    public OracleTablePath(String pgSchemaName, String pgTableName) {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(pgSchemaName));
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(pgTableName));

        this.oracleSchemaName = pgSchemaName;
        this.oracleTableName = pgTableName;
    }

    public static OracleTablePath fromFlinkTableName(String flinkTableName) {
        if (flinkTableName.contains(".")) {
            String[] path = flinkTableName.split("\\.");

            checkArgument(
                    path != null && path.length == 2,
                    String.format(
                            "Table name '%s' is not valid. The parsed length is %d",
                            flinkTableName, path.length));

            return new OracleTablePath(path[0], path[1]);
        } else {
            return new OracleTablePath(DEFAULT_ORACLE_SCHEMA_NAME, flinkTableName);
        }
    }

    public static String toFlinkTableName(String schema, String table) {
        return new org.apache.flink.connector.jdbc.catalog.PostgresTablePath(schema, table).getFullPath();
    }

    public String getFullPath() {
        return String.format("%s.%s", oracleSchemaName, oracleTableName);
    }

    public String getOracleTableName() {
        return oracleTableName;
    }

    public String getOracleSchemaName() {
        return oracleSchemaName;
    }

    @Override
    public String toString() {
        return getFullPath();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        OracleTablePath that = (OracleTablePath) o;
        return Objects.equals(oracleSchemaName,that.oracleSchemaName )
                && Objects.equals(oracleTableName, that.oracleTableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(oracleSchemaName, oracleTableName);
    }

}
