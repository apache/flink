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

package org.apache.flink.tests.util.ssh;

import org.apache.flink.tests.util.parameters.ParameterProperty;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.connection.channel.direct.Session;

import java.io.IOException;
import java.nio.file.Path;

/**
 * SSH test utilities.
 */
// TODO: maybe introduce interface so we can test things locally, pretending to copy things over the network
public class SshTool {

	// TODO: check whether username/password are actually necessary; start-cluster.sh requires passwordless SSH to be setup
	private static final ParameterProperty<String> SSH_USERNAME = new ParameterProperty<>("ssh_user", value -> value);
	private static final ParameterProperty<String> SSH_PASSWORD = new ParameterProperty<>("ssh_pass", value -> value);
	private static final ParameterProperty<Integer> SSH_PORT = new ParameterProperty<>("ssh_port", Integer::valueOf);

	private final String username;
	private final String password;
	private final int port;

	private SshTool(String username, String password, int port) {
		this.username = username;
		this.password = password;
		this.port = port;
	}

	public void copyDirectoryTo(Path localDirectory, Path remoteDirectory, String host) throws IOException {
		try (SSHClient ssh = new SSHClient()) {
			ssh.connect(host, this.port);
			ssh.authPassword(this.username, this.password);
			ssh.newSCPFileTransfer()
				.upload(localDirectory.toAbsolutePath().toString(), remoteDirectory.toAbsolutePath().toString());
		}
	}

	public void copyDirectoryFrom(Path remoteDirectory, Path localDirectory, String host) throws IOException {
		try (SSHClient ssh = new SSHClient()) {
			ssh.connect(host, this.port);
			ssh.authPassword(this.username, this.password);
			ssh.newSCPFileTransfer()
				.download(remoteDirectory.toAbsolutePath().toString(), localDirectory.toAbsolutePath().toString());
		}
	}

	public void runBlocking(String host, String... commands) throws IOException {
		try (SSHClient ssh = new SSHClient()) {
			ssh.connect(host, this.port);
			ssh.authPassword(this.username, this.password);
			try (Session session = ssh.startSession()) {
				session.exec(String.join(" ", commands)).join();
			}
		}
	}

	public static SshTool fromSystemProperties() {
		String username = SSH_USERNAME.get().orElseThrow(() -> new IllegalStateException(SSH_USERNAME.getPropertyName() + " was not configured."));
		String password = SSH_PASSWORD.get().orElseThrow(() -> new IllegalStateException(SSH_PASSWORD.getPropertyName() + " was not configured."));
		int port = SSH_PORT.get(2020);

		return new SshTool(username, password, port);
	}
}
