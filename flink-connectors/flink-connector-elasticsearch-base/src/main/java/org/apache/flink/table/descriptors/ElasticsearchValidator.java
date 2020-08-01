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

package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.Host;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.StringUtils;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.flink.table.descriptors.DescriptorProperties.noValidation;

/**
 * The validator for {@link Elasticsearch}.
 */
@Internal
public class ElasticsearchValidator extends ConnectorDescriptorValidator {

	public static final String CONNECTOR_TYPE_VALUE_ELASTICSEARCH = "elasticsearch";
	public static final String CONNECTOR_VERSION_VALUE_6 = "6";
	public static final String CONNECTOR_VERSION_VALUE_7 = "7";
	public static final String CONNECTOR_HOSTS = "connector.hosts";
	public static final String CONNECTOR_HOSTS_HOSTNAME = "hostname";
	public static final String CONNECTOR_HOSTS_PORT = "port";
	public static final String CONNECTOR_HOSTS_PROTOCOL = "protocol";
	public static final String CONNECTOR_INDEX = "connector.index";
	public static final String CONNECTOR_DOCUMENT_TYPE = "connector.document-type";
	public static final String CONNECTOR_KEY_DELIMITER = "connector.key-delimiter";
	public static final String CONNECTOR_KEY_NULL_LITERAL = "connector.key-null-literal";
	public static final String CONNECTOR_FAILURE_HANDLER = "connector.failure-handler";
	public static final String CONNECTOR_FAILURE_HANDLER_VALUE_FAIL = "fail";
	public static final String CONNECTOR_FAILURE_HANDLER_VALUE_IGNORE = "ignore";
	public static final String CONNECTOR_FAILURE_HANDLER_VALUE_RETRY = "retry-rejected";
	public static final String CONNECTOR_FAILURE_HANDLER_VALUE_CUSTOM = "custom";
	public static final String CONNECTOR_FAILURE_HANDLER_CLASS = "connector.failure-handler-class";
	public static final String CONNECTOR_FLUSH_ON_CHECKPOINT = "connector.flush-on-checkpoint";
	public static final String CONNECTOR_BULK_FLUSH_MAX_ACTIONS = "connector.bulk-flush.max-actions";
	public static final String CONNECTOR_BULK_FLUSH_MAX_SIZE = "connector.bulk-flush.max-size";
	public static final String CONNECTOR_BULK_FLUSH_INTERVAL = "connector.bulk-flush.interval";
	public static final String CONNECTOR_BULK_FLUSH_BACKOFF_TYPE = "connector.bulk-flush.backoff.type";
	public static final String CONNECTOR_BULK_FLUSH_BACKOFF_TYPE_VALUE_DISABLED = "disabled";
	public static final String CONNECTOR_BULK_FLUSH_BACKOFF_TYPE_VALUE_CONSTANT = "constant";
	public static final String CONNECTOR_BULK_FLUSH_BACKOFF_TYPE_VALUE_EXPONENTIAL = "exponential";
	public static final String CONNECTOR_BULK_FLUSH_BACKOFF_MAX_RETRIES = "connector.bulk-flush.backoff.max-retries";
	public static final String CONNECTOR_BULK_FLUSH_BACKOFF_DELAY = "connector.bulk-flush.backoff.delay";
	public static final String CONNECTOR_CONNECTION_MAX_RETRY_TIMEOUT = "connector.connection-max-retry-timeout";
	public static final String CONNECTOR_CONNECTION_PATH_PREFIX = "connector.connection-path-prefix";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		properties.validateValue(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_ELASTICSEARCH, false);
		validateVersion(properties);
		validateHosts(properties);
		validateGeneralProperties(properties);
		validateFailureHandler(properties);
		validateBulkFlush(properties);
		validateConnectionProperties(properties);
	}

	private void validateVersion(DescriptorProperties properties) {
		properties.validateEnumValues(
			CONNECTOR_VERSION,
			false,
			Arrays.asList(CONNECTOR_VERSION_VALUE_6, CONNECTOR_VERSION_VALUE_7));
	}

	private void validateHosts(DescriptorProperties properties) {
		if (properties.containsKey(CONNECTOR_HOSTS)) {
			validateAndParseHostsString(properties);
		} else {
			final Map<String, Consumer<String>> hostsValidators = new HashMap<>();
			hostsValidators.put(CONNECTOR_HOSTS_HOSTNAME, (key) -> properties.validateString(key, false, 1));
			hostsValidators.put(CONNECTOR_HOSTS_PORT, (key) -> properties.validateInt(key, false, 0, 65535));
			hostsValidators.put(CONNECTOR_HOSTS_PROTOCOL, (key) -> properties.validateString(key, false, 1));

			properties.validateFixedIndexedProperties(CONNECTOR_HOSTS, false, hostsValidators);
		}
	}

	private void validateGeneralProperties(DescriptorProperties properties) {
		properties.validateString(CONNECTOR_INDEX, false, 1);
		properties.validateString(CONNECTOR_DOCUMENT_TYPE, false, 1);
		properties.validateString(CONNECTOR_KEY_DELIMITER, true);
		properties.validateString(CONNECTOR_KEY_NULL_LITERAL, true);
	}

	private void validateFailureHandler(DescriptorProperties properties) {
		final Map<String, Consumer<String>> failureHandlerValidators = new HashMap<>();
		failureHandlerValidators.put(CONNECTOR_FAILURE_HANDLER_VALUE_FAIL, noValidation());
		failureHandlerValidators.put(CONNECTOR_FAILURE_HANDLER_VALUE_IGNORE, noValidation());
		failureHandlerValidators.put(CONNECTOR_FAILURE_HANDLER_VALUE_RETRY, noValidation());
		failureHandlerValidators.put(CONNECTOR_FAILURE_HANDLER_VALUE_CUSTOM,
			key -> properties.validateString(CONNECTOR_FAILURE_HANDLER_CLASS, false, 1));
		properties.validateEnum(CONNECTOR_FAILURE_HANDLER, true, failureHandlerValidators);
	}

	private void validateBulkFlush(DescriptorProperties properties) {
		properties.validateBoolean(CONNECTOR_FLUSH_ON_CHECKPOINT, true);
		properties.validateInt(CONNECTOR_BULK_FLUSH_MAX_ACTIONS, true, 1);
		properties.validateMemorySize(CONNECTOR_BULK_FLUSH_MAX_SIZE, true, 1024 * 1024); // only allow MB precision
		properties.validateLong(CONNECTOR_BULK_FLUSH_INTERVAL, true, 0);
		properties.validateEnumValues(CONNECTOR_BULK_FLUSH_BACKOFF_TYPE,
			true,
			Arrays.asList(
				CONNECTOR_BULK_FLUSH_BACKOFF_TYPE_VALUE_DISABLED,
				CONNECTOR_BULK_FLUSH_BACKOFF_TYPE_VALUE_CONSTANT,
				CONNECTOR_BULK_FLUSH_BACKOFF_TYPE_VALUE_EXPONENTIAL));
		properties.validateInt(CONNECTOR_BULK_FLUSH_BACKOFF_MAX_RETRIES, true, 1);
		properties.validateLong(CONNECTOR_BULK_FLUSH_BACKOFF_DELAY, true, 0);
	}

	private void validateConnectionProperties(DescriptorProperties properties) {
		properties.validateInt(CONNECTOR_CONNECTION_MAX_RETRY_TIMEOUT, true, 1);
		properties.validateString(CONNECTOR_CONNECTION_PATH_PREFIX, true);
	}

	/**
	 * Parse Hosts String to list.
	 *
	 * <p>Hosts String format was given as following:
	 *
	 * <pre>
	 *     connector.hosts = http://host_name:9092;http://host_name:9093
	 * </pre>
	 */
	public static List<Host> validateAndParseHostsString(DescriptorProperties descriptorProperties) {
		final List<Host> hostList = new ArrayList<>();

		descriptorProperties.validateString(CONNECTOR_HOSTS, false, 1);
		final String hostsStr = descriptorProperties.getString(CONNECTOR_HOSTS);

		final String[] hosts = hostsStr.split(";");
		final String validationExceptionMessage = "Properties '" + CONNECTOR_HOSTS + "' format should " +
			"follow the format 'http://host_name:port', but is '" + hostsStr + "'.";

		if (hosts.length == 0) {
			throw new ValidationException(validationExceptionMessage);
		}
		for (String host : hosts) {
			try {
				final URL url = new URL(host);
				final String protocol = url.getProtocol();
				final String hostName = url.getHost();
				final int hostPort = url.getPort();

				if (StringUtils.isNullOrWhitespaceOnly(protocol) ||
					StringUtils.isNullOrWhitespaceOnly(hostName) ||
					-1 == hostPort) {
					throw new ValidationException(validationExceptionMessage);
				}

				hostList.add(new Host(hostName, hostPort, protocol));
			} catch (MalformedURLException e) {
				throw new ValidationException(validationExceptionMessage, e);
			}
		}
		return hostList;
	}
}
