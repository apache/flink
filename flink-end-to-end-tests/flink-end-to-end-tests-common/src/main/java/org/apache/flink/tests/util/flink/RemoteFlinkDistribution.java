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

import org.apache.flink.annotation.Internal;
import org.apache.flink.tests.util.ssh.SshTool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * A wrapper around a remote Flink distribution.
 */
public final class RemoteFlinkDistribution {

	private static final Logger LOG = LoggerFactory.getLogger(RemoteFlinkDistribution.class);

	private final String host;
	private final Path remoteDistributionDir;
	private final SshTool sshTool;

	@Internal
	public RemoteFlinkDistribution(String host, Path remoteDistributionDir, SshTool sshTool) {
		this.host = host;
		this.remoteDistributionDir = remoteDistributionDir;
		this.sshTool = sshTool;
	}

	public String getHost() {
		return host;
	}

	public void stopFlinkCluster() throws IOException {
		// TODO: implement shutdown over SSH
	}

	public Stream<String> searchAllLogs(Pattern pattern, Function<Matcher, String> matchProcessor) throws IOException {
		// TODO: implement log search; either fetch the file and run locally or use pattern.pattern() and run grep over SSH
		return Stream.empty();
	}

	public void copyLogsTo(Path targetDirectory) throws IOException {
		// TODO: implement copy to the local target directory; SshTool#copyDirectoryFrom may be useful
	}
}
