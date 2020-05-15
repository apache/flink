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

package org.apache.flink.streaming.connectors.elasticsearch.testutils;

import org.apache.flink.streaming.connectors.elasticsearch.EmbeddedElasticsearchNodeEnvironment;
import org.apache.flink.util.InstantiationUtil;

import org.elasticsearch.client.Client;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A resource that starts an embedded elasticsearch cluster.
 */
public class ElasticsearchResource extends ExternalResource {
	private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchResource.class);
	private EmbeddedElasticsearchNodeEnvironment embeddedNodeEnv;
	private final TemporaryFolder tempFolder = new TemporaryFolder();

	private final String clusterName;

	public ElasticsearchResource(String clusterName) {
		this.clusterName = clusterName;
	}

	@Override
	protected void before() throws Throwable {

		LOG.info("-------------------------------------------------------------------------");
		LOG.info("    Starting embedded Elasticsearch node ");
		LOG.info("-------------------------------------------------------------------------");

		// dynamically load version-specific implementation of the Elasticsearch embedded node environment
		Class<?> clazz = Class.forName(
			"org.apache.flink.streaming.connectors.elasticsearch.EmbeddedElasticsearchNodeEnvironmentImpl");
		embeddedNodeEnv = (EmbeddedElasticsearchNodeEnvironment) InstantiationUtil.instantiate(clazz);

		tempFolder.create();
		embeddedNodeEnv.start(tempFolder.newFolder(), clusterName);
	}

	@Override
	protected void after() {

		LOG.info("-------------------------------------------------------------------------");
		LOG.info("    Shutting down embedded Elasticsearch node ");
		LOG.info("-------------------------------------------------------------------------");

		try {
			embeddedNodeEnv.close();
			tempFolder.delete();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public Client getClient() {
		return embeddedNodeEnv.getClient();
	}
}
