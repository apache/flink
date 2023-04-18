package org.apache.flink.table.jdbc;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.client.gateway.SingleSessionManager;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestEndpointExtension;
import org.apache.flink.table.gateway.service.utils.SqlGatewayServiceExtension;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Properties;

public class FlinkJdbcDriverTestBase {
    @RegisterExtension
    @Order(1)
    private static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .setNumberSlotsPerTaskManager(4)
                            .build());

    @RegisterExtension
    @Order(2)
    private static final SqlGatewayServiceExtension SQL_GATEWAY_SERVICE_EXTENSION =
            new SqlGatewayServiceExtension(
                    MINI_CLUSTER_RESOURCE::getClientConfiguration, SingleSessionManager::new);

    @RegisterExtension
    @Order(3)
    private static final SqlGatewayRestEndpointExtension SQL_GATEWAY_REST_ENDPOINT_EXTENSION =
            new SqlGatewayRestEndpointExtension(SQL_GATEWAY_SERVICE_EXTENSION::getService);

    protected DriverUri getDriverUri() throws Exception {
        return getDriverUri("jdbc:flink://%s:%s", new Properties());
    }

    protected DriverUri getDriverUri(String url, Properties properties) throws Exception {
        return DriverUri.create(
                String.format(
                        url,
                        SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetAddress(),
                        SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetPort()),
                properties);
    }
}
