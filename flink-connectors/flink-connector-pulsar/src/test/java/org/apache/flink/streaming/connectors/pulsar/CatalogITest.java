/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.pulsar;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.commons.cli.Options;
import org.apache.flink.client.cli.DefaultCLI;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.pulsar.testutils.EnvironmentFileUtil;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.pulsar.PulsarCatalog;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.local.ExecutionContext;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CatalogITest extends PulsarTestBaseWithFlink {

	private static final String CATALOGS_ENVIRONMENT_FILE = "test-sql-client-pulsar-catalog.yaml";
	private static final String CATALOGS_ENVIRONMENT_FILE_START = "test-sql-client-pulsar-start-catalog.yaml";

	@Test
	public void testCatalogs() throws Exception {
		String inmemoryCatalog = "inmemorycatalog";
		String pulsarCatalog1 = "pulsarcatalog1";
		String pulsarCatalog2 = "pulsarcatalog2";

		ExecutionContext context = createExecutionContext(CATALOGS_ENVIRONMENT_FILE, getStreamingConfs());
		TableEnvironment tableEnv = context.getTableEnvironment();

		assertEquals(tableEnv.getCurrentCatalog(), inmemoryCatalog);
		assertEquals(tableEnv.getCurrentDatabase(), "mydatabase");

		Catalog catalog = tableEnv.getCatalog(pulsarCatalog1).orElse(null);
		assertNotNull(catalog);
		assertTrue(catalog instanceof PulsarCatalog);
		tableEnv.useCatalog(pulsarCatalog1);
		assertEquals(tableEnv.getCurrentDatabase(), "public/default");

		catalog = tableEnv.getCatalog(pulsarCatalog2).orElse(null);
		assertNotNull(catalog);
		assertTrue(catalog instanceof PulsarCatalog);
		tableEnv.useCatalog(pulsarCatalog2);
		assertEquals(tableEnv.getCurrentDatabase(), "tn/ns");
	}

	@Test
	public void testDatabases() throws Exception {
		String pulsarCatalog1 = "pulsarcatalog1";
		List<String> namespaces = Arrays.asList("tn1/ns1", "tn1/ns2");
		List<String> topics = Arrays.asList("tp1", "tp2");
		List<String> topicsFullName = topics.stream().map(a -> "tn1/ns1/" + a).collect(Collectors.toList());
		List<String> partitionedTopics = Arrays.asList("ptp1", "ptp2");
		List<String> partitionedTopicsFullName = partitionedTopics.stream().map(a -> "tn1/ns1/" + a).collect(Collectors.toList());

		ExecutionContext context = createExecutionContext(CATALOGS_ENVIRONMENT_FILE, getStreamingConfs());
		TableEnvironment tableEnv = context.getTableEnvironment();

		tableEnv.useCatalog(pulsarCatalog1);
		assertEquals(tableEnv.getCurrentDatabase(), "public/default");

		try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(getAdminUrl()).build()) {
			admin.tenants().createTenant("tn1",
				new TenantInfo(Sets.newHashSet(), Sets.newHashSet("standalone")));
			for (String ns : namespaces) {
				admin.namespaces().createNamespace(ns);
			}

			for (String tp : topicsFullName) {
				admin.topics().createNonPartitionedTopic(tp);
			}

			for (String tp : partitionedTopicsFullName) {
				admin.topics().createPartitionedTopic(tp, 5);
			}

			assertTrue(Sets.newHashSet(tableEnv.listDatabases()).containsAll(namespaces));

			tableEnv.useDatabase("tn1/ns1");

			assertTrue(
				Sets.symmetricDifference(
					Sets.newHashSet(tableEnv.listTables()),
					Sets.newHashSet(Iterables.concat(topics, partitionedTopics)))
					.isEmpty());

			for (String tp : topicsFullName) {
				admin.topics().delete(tp);
			}

			for (String tp : partitionedTopicsFullName) {
				admin.topics().deletePartitionedTopic(tp);
			}

			for (String ns : namespaces) {
				admin.namespaces().deleteNamespace(ns);
			}
		}
	}

	private <T> ExecutionContext<T> createExecutionContext(String file, Map<String, String> replaceVars) throws Exception {
		final Environment env = EnvironmentFileUtil.parseModified(
			file,
			replaceVars);
		final Configuration flinkConfig = new Configuration();
		return new ExecutionContext<>(
			env,
			new SessionContext("test-session", new Environment()),
			Collections.emptyList(),
			flinkConfig,
			new Options(),
			Collections.singletonList(new DefaultCLI(flinkConfig)));
	}

	private Map<String, String> getStreamingConfs() {
		Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_EXECUTION_TYPE", "streaming");
		replaceVars.put("$VAR_RESULT_MODE", "changelog");
		replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
		replaceVars.put("$VAR_MAX_ROWS", "100");
		replaceVars.put("$VAR_SERVICEURL", getServiceUrl());
		replaceVars.put("$VAR_ADMINURL", getAdminUrl());
		return replaceVars;
	}
}
