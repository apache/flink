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

package org.apache.flink.tests.util.flink;

import org.apache.flink.tests.util.parameters.ParameterProperty;
import org.apache.flink.tests.util.ssh.SshTool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.tests.util.flink.FlinkResourceFactoryUtils.getDistributionDirectory;
import static org.apache.flink.tests.util.flink.FlinkResourceFactoryUtils.getLogBackupDirectory;

/**
 * A {@link FlinkResourceFactory} for the {@link DistributedStandaloneFlinkResource}.
 */
public final class DistributedStandaloneFlinkResourceFactory implements FlinkResourceFactory {
	private static final Logger LOG = LoggerFactory.getLogger(DistributedStandaloneFlinkResourceFactory.class);

	private static final ParameterProperty<List<String>> JM_HOSTS = new ParameterProperty<>("jm_hosts", value -> Arrays.asList(value.split(";")));
	private static final ParameterProperty<List<String>> TM_HOSTS = new ParameterProperty<>("tm_hosts", value -> Arrays.asList(value.split(";")));

	@Override
	public Optional<FlinkResource> create(FlinkResourceSetup setup) {
		Optional<List<String>> jmHosts = JM_HOSTS.get();
		if (jmHosts.isEmpty()) {
			LOG.debug("Not loading {} because {} was not set.", DistributedStandaloneFlinkResource.class, JM_HOSTS.getPropertyName());
			return Optional.empty();
		}
		Optional<List<String>> tmHosts = TM_HOSTS.get();
		if (tmHosts.isEmpty()) {
			LOG.debug("Not loading {} because {} was not set.", DistributedStandaloneFlinkResource.class, TM_HOSTS.getPropertyName());
			return Optional.empty();
		}
		LOG.info("Created {}.", DistributedStandaloneFlinkResource.class.getSimpleName());
		return getDistributionDirectory(LOG).map(distributionDirectory ->
			new DistributedStandaloneFlinkResource(
				distributionDirectory,
				getLogBackupDirectory(LOG),
				jmHosts.get(),
				tmHosts.get(),
				SshTool.fromSystemProperties(),
				setup));
	}
}
