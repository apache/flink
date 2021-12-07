package org.apache.flink.connector.jdbc.dialect.oracle;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.utility.DockerImageName;

/** {@link OracleContainer}. */
public class OracleContainer extends JdbcDatabaseContainer<OracleContainer> {
    private static final String DEFAULT_TAG = "18.4.0-slim";
    private static final String IMAGE = "gvenzl/oracle-xe";
    private static final DockerImageName ORACLE_IMAGE =
            DockerImageName.parse(IMAGE).withTag(DEFAULT_TAG);

    private static final int ORACLE_PORT = 1521;
    private static final int APEX_HTTP_PORT = 8080;

    private static final int DEFAULT_STARTUP_TIMEOUT_SECONDS = 240;
    private static final int DEFAULT_CONNECT_TIMEOUT_SECONDS = 120;

    private String username = "system";
    private String password = "oracle";

    public OracleContainer() {
        super(ORACLE_IMAGE);
        preconfigure();
    }

    private void preconfigure() {
        withStartupTimeoutSeconds(DEFAULT_STARTUP_TIMEOUT_SECONDS);
        withConnectTimeoutSeconds(DEFAULT_CONNECT_TIMEOUT_SECONDS);
        withEnv("ORACLE_PASSWORD", password);
        addExposedPorts(ORACLE_PORT, APEX_HTTP_PORT);
    }

    @Override
    public String getDriverClassName() {
        return "oracle.jdbc.OracleDriver";
    }

    @Override
    public String getJdbcUrl() {
        return "jdbc:oracle:thin:"
                + getUsername()
                + "/"
                + getPassword()
                + "@"
                + getHost()
                + ":"
                + getOraclePort()
                + ":"
                + getSid();
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @SuppressWarnings("SameReturnValue")
    public String getSid() {
        return "xe";
    }

    public Integer getOraclePort() {
        return getMappedPort(ORACLE_PORT);
    }

    @SuppressWarnings("unused")
    public Integer getWebPort() {
        return getMappedPort(APEX_HTTP_PORT);
    }

    @Override
    public String getTestQueryString() {
        return "SELECT 1 FROM DUAL";
    }
}
