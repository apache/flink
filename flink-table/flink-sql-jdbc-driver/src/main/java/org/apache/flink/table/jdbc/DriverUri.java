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

package org.apache.flink.table.jdbc;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.flink.table.jdbc.utils.DriverUtils.checkNotNull;
import static org.apache.flink.table.jdbc.utils.DriverUtils.fromProperties;
import static org.apache.flink.table.jdbc.utils.DriverUtils.isNullOrWhitespaceOnly;

/** Parse catalog, table and connection properties from uri for {@link FlinkDriver}. */
public class DriverUri {
    private static final String URL_PREFIX = "jdbc:";
    private static final String URL_START = URL_PREFIX + "flink:";
    private final InetSocketAddress address;
    private final URI uri;

    private final Properties properties;

    private Optional<String> catalog = Optional.empty();
    private Optional<String> database = Optional.empty();

    private DriverUri(String url, Properties driverProperties) throws SQLException {
        this(parseDriverUrl(url), driverProperties);
    }

    private DriverUri(URI uri, Properties driverProperties) throws SQLException {
        this.uri = checkNotNull(uri, "uri is null");
        this.address = new InetSocketAddress(uri.getHost(), uri.getPort());
        this.properties = mergeDynamicProperties(uri, driverProperties);

        initCatalogAndSchema();
    }

    public InetSocketAddress getAddress() {
        return address;
    }

    public Properties getProperties() {
        return properties;
    }

    public Optional<String> getCatalog() {
        return catalog;
    }

    public Optional<String> getDatabase() {
        return database;
    }

    public String getURL() {
        return String.format("%s%s", URL_PREFIX, uri);
    }

    private void initCatalogAndSchema() throws SQLException {
        String path = uri.getPath();
        if (isNullOrWhitespaceOnly(uri.getPath()) || path.equals("/")) {
            return;
        }

        // remove first slash
        if (!path.startsWith("/")) {
            throw new SQLException("Path in uri does not start with a slash: " + uri);
        }
        path = path.substring(1);

        List<String> parts = Arrays.stream(path.split("/")).collect(Collectors.toList());
        // remove last item due to a trailing slash
        if (parts.get(parts.size() - 1).isEmpty()) {
            parts = parts.subList(0, parts.size() - 1);
        }

        if (parts.size() > 2) {
            throw new SQLException("Invalid path segments in URL: " + uri);
        }

        if (parts.get(0).isEmpty()) {
            throw new SQLException("Catalog name in URL is empty: " + uri);
        }

        catalog = Optional.ofNullable(parts.get(0));

        if (parts.size() > 1) {
            if (parts.get(1).isEmpty()) {
                throw new SQLException("Database name in URL is empty: " + uri);
            }

            database = Optional.ofNullable(parts.get(1));
        }
    }

    private static Properties mergeDynamicProperties(URI uri, Properties driverProperties)
            throws SQLException {
        Map<String, String> urlProperties = parseUriParameters(uri.getQuery());
        Map<String, String> suppliedProperties = fromProperties(driverProperties);

        for (String key : urlProperties.keySet()) {
            if (suppliedProperties.containsKey(key)) {
                throw new SQLException(
                        format("Connection property '%s' is both in the URL and an argument", key));
            }
        }

        Properties result = new Properties();
        addMapToProperties(result, urlProperties);
        addMapToProperties(result, suppliedProperties);
        return result;
    }

    private static void addMapToProperties(Properties properties, Map<String, String> values) {
        for (Map.Entry<String, String> entry : values.entrySet()) {
            properties.setProperty(entry.getKey(), entry.getValue());
        }
    }

    private static Map<String, String> parseUriParameters(String query) throws SQLException {
        Map<String, String> result = new HashMap<>();

        if (query != null) {
            String[] queryArgs = query.split("&");
            for (String queryArg : queryArgs) {
                String[] parts = queryArg.split("=");
                if (parts.length != 2) {
                    throw new SQLException(
                            format(
                                    "Connection property in uri must be key=val format: '%s'",
                                    queryArg));
                }
                if (result.put(parts[0], parts[1]) != null) {
                    throw new SQLException(
                            format("Connection property '%s' is in URL multiple times", parts[0]));
                }
            }
        }

        return result;
    }

    private static URI parseDriverUrl(String url) throws SQLException {
        if (!url.startsWith(URL_START)) {
            throw new SQLException(
                    String.format("Flink JDBC URL[%s] must start with [%s]", url, URL_START));
        }

        if (url.equals(URL_START)) {
            throw new SQLException(String.format("Empty Flink JDBC URL: %s", url));
        }

        URI uri;
        try {
            uri = new URI(url.substring(URL_PREFIX.length()));
        } catch (URISyntaxException e) {
            throw new SQLException(String.format("Invalid Flink JDBC URL: %s", url), e);
        }

        if (isNullOrWhitespaceOnly(uri.getHost())) {
            throw new SQLException(String.format("No host specified in uri: %s", url));
        }
        if (uri.getPort() == -1) {
            throw new SQLException(String.format("No port specified in uri: %s", url));
        }
        if ((uri.getPort() < 1) || (uri.getPort() > 65535)) {
            throw new SQLException(String.format("Port must be [1, 65535] in uri: %s", url));
        }
        return uri;
    }

    public static boolean acceptsURL(String url) {
        return url.startsWith(URL_START);
    }

    public static DriverUri create(String url, Properties properties) throws SQLException {
        return new DriverUri(url, properties == null ? new Properties() : properties);
    }
}
