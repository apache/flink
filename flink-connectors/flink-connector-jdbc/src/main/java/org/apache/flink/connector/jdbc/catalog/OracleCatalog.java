package org.apache.flink.connector.jdbc.catalog;

import org.apache.commons.lang3.StringUtils;

import org.apache.flink.connector.jdbc.dialect.JdbcDialectTypeMapper;

import org.apache.flink.connector.jdbc.dialect.oracle.OracleTypeMapper;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import org.apache.flink.util.TemporaryClassLoaderContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class OracleCatalog extends AbstractJdbcCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(OracleCatalog.class);

    private final JdbcDialectTypeMapper dialectTypeMapper;
    private static final Set<String> builtinDatabases = new HashSet<String>() {
        {
            add("SCOTT");
            add("ANONYMOUS");
            add("XS$NULL");
            add("DIP");
            add("SPATIAL_WFS_ADMIN_USR");
            add("SPATIAL_CSW_ADMIN_USR");
            add("APEX_PUBLIC_USER");
            add("ORACLE_OCM");
            add("MDDATA");
        }
    };

    public OracleCatalog(ClassLoader userClassLoader,

                         String catalogName,

                         String defaultDatabase,

                         String username,

                         String pwd,

                         String baseUrl) {

        super(userClassLoader, catalogName, defaultDatabase, username, pwd, baseUrl);

        String driverVersion = Preconditions.checkNotNull(getDriverVersion(), "Driver version must not be null.");

        String databaseVersion = Preconditions.checkNotNull(getDatabaseVersion(), "Database version must not be null.");

        LOG.info("Driver version: {}, database version: {}", driverVersion, databaseVersion);

        this.dialectTypeMapper = new OracleTypeMapper(databaseVersion, driverVersion);

    }

    private String getDatabaseVersion() {
        try (TemporaryClassLoaderContext ignored =
                     TemporaryClassLoaderContext.of(userClassLoader)) {
            try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
                return conn.getMetaData().getDatabaseProductVersion();
            } catch (Exception e) {
                throw new CatalogException(
                        String.format("Failed in getting Oracle version by %s.", defaultUrl), e);
            }
        }
    }

    private String getDriverVersion() {

        try (TemporaryClassLoaderContext ignored =
                     TemporaryClassLoaderContext.of(userClassLoader)) {
            try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
                String driverVersion = conn.getMetaData().getDriverVersion();
                Pattern regexp = Pattern.compile("\\d+?\\.\\d+?\\.\\d+");
                Matcher matcher = regexp.matcher(driverVersion);
                return matcher.find() ? matcher.group(0) : null;
            } catch (Exception e) {
                throw new CatalogException(
                        String.format("Failed in getting Oracle driver version by %s.", defaultUrl), e);
            }
        }
    }

    @Override
    public List<String> listDatabases() throws CatalogException {

        return extractColumnValuesBySQL(this.defaultUrl,
                "select username from sys.dba_users " +
                        "where DEFAULT_TABLESPACE <> 'SYSTEM' and DEFAULT_TABLESPACE <> 'SYSAUX' " +
                        " order by username",
                1,
                dbName -> !builtinDatabases.contains(dbName));
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        Preconditions.checkState(StringUtils.isNotBlank(databaseName), "Database name must not be blank.");
        if (!databaseExists(databaseName)){// 注意这个值是 oracle 实例名称
            throw new DatabaseNotExistException(getName(), databaseName);
        }
        List<String> listDatabases = listDatabases().stream().map(username -> "'" + username + "'").collect(Collectors.toList());
        return extractColumnValuesBySQL(
                this.defaultUrl,
                "SELECT OWNER||'.'||TABLE_NAME AS schemaTableName FROM sys.all_tables WHERE OWNER IN (" + String.join(",", listDatabases) + ")"+
                        "ORDER BY OWNER,TABLE_NAME",1, null, null);
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        String[] schemaTableNames = getSchemaTableName(tablePath).split("\\.");
        return !extractColumnValuesBySQL( defaultUrl,
                "SELECT table_name FROM sys.all_tables where OWNER = ? and table_name = ?",
                1, null,
                schemaTableNames[0], schemaTableNames[1])
                .isEmpty();
    }

    /**

     * Converts Oracle type to Flink {@link DataType}.

     */
    @Override
    protected DataType fromJDBCType(ObjectPath tablePath, ResultSetMetaData metadata, int colIndex) throws SQLException {
        return dialectTypeMapper.mapping(tablePath, metadata, colIndex);
    }

    @Override

    protected String getTableName(ObjectPath tablePath) {
        return PostgresTablePath.fromFlinkTableName(tablePath.getObjectName()).getPgTableName();
    }

    @Override
    protected String getSchemaName(ObjectPath tablePath) {
        return PostgresTablePath.fromFlinkTableName(tablePath.getObjectName()).getPgSchemaName();
    }

    @Override
    protected String getSchemaTableName(ObjectPath tablePath) {
        return PostgresTablePath.fromFlinkTableName(tablePath.getObjectName()).getFullPath();
    }

}
