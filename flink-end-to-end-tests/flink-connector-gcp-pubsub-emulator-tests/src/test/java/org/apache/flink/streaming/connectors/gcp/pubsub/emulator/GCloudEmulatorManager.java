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

package org.apache.flink.streaming.connectors.gcp.pubsub.emulator;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.LogStream;
import com.spotify.docker.client.exceptions.ContainerNotFoundException;
import com.spotify.docker.client.exceptions.DockerCertificateException;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.exceptions.ImageNotFoundException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerInfo;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.PortBinding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * The class that handles the starting and stopping of the emulator docker image.
 */
public class GCloudEmulatorManager {

	private static final Logger LOG = LoggerFactory.getLogger(GCloudEmulatorManager.class);

	private static DockerClient docker;

	private static String dockerIpAddress = "127.0.0.1";

	public static final String INTERNAL_PUBSUB_PORT = "22222";
	// TODO: use :latest again once https://github.com/GoogleCloudPlatform/cloud-sdk-docker/issues/225 is resolved.
	public static final String DOCKER_IMAGE_NAME = "google/cloud-sdk:313.0.0";

	private static String pubsubPort;

	public static String getDockerIpAddress() {
		if (dockerIpAddress == null) {
			throw new IllegalStateException("The docker has not yet been started (yet) so you cannot get the IP address yet.");
		}
		return dockerIpAddress;
	}

	public static String getDockerPubSubPort() {
		if (pubsubPort == null) {
			throw new IllegalStateException("The docker has not yet been started (yet) so you cannot get the port information yet.");
		}
		return pubsubPort;
	}

	public static final String UNITTEST_PROJECT_ID = "running-from-junit-for-flink";
	private static final String CONTAINER_NAME_JUNIT = (DOCKER_IMAGE_NAME + "_" + UNITTEST_PROJECT_ID).replaceAll("[^a-zA-Z0-9_]", "_");

	public static void launchDocker() throws DockerException, InterruptedException, DockerCertificateException {
		// Create a client based on DOCKER_HOST and DOCKER_CERT_PATH env vars
		docker = DefaultDockerClient.fromEnv().build();

		terminateAndDiscardAnyExistingContainers(true);

		LOG.info("");
		LOG.info("/===========================================");
		LOG.info("| GCloud Emulator");

		ContainerInfo containerInfo;
		String id;

		try {
			docker.inspectImage(DOCKER_IMAGE_NAME);
		} catch (ImageNotFoundException e) {
			// No such image so we must download it first.
			LOG.info("| - Getting docker image \"{}\"", DOCKER_IMAGE_NAME);
			docker.pull(DOCKER_IMAGE_NAME, message -> {
				if (message.id() != null && message.progress() != null) {
					LOG.info("| - Downloading > {} : {}", message.id(), message.progress());
				}
			});
		}

		// No such container. Good, we create one!
		LOG.info("| - Creating new container");

		// Bind container ports to host ports
		final Map<String, List<PortBinding>> portBindings = new HashMap<>();
		portBindings.put(INTERNAL_PUBSUB_PORT, Collections.singletonList(PortBinding.randomPort("0.0.0.0")));

		final HostConfig hostConfig = HostConfig.builder().portBindings(portBindings).build();

		// Create new container with exposed ports
		final ContainerConfig containerConfig = ContainerConfig.builder()
			.hostConfig(hostConfig)
			.exposedPorts(INTERNAL_PUBSUB_PORT)
			.image(DOCKER_IMAGE_NAME)
			.cmd("sh", "-c", "mkdir -p /opt/data/pubsub ; gcloud beta emulators pubsub start --data-dir=/opt/data/pubsub --host-port=0.0.0.0:" + INTERNAL_PUBSUB_PORT)
			.build();

		LOG.debug("Launching container with configuration {}", containerConfig);
		final ContainerCreation creation = docker.createContainer(containerConfig, CONTAINER_NAME_JUNIT);
		id = creation.id();

		containerInfo = docker.inspectContainer(id);

		if (!containerInfo.state().running()) {
			LOG.warn("| - Starting it up ....");
			docker.startContainer(id);
			Thread.sleep(1000);
		}

		containerInfo = docker.inspectContainer(id);

		dockerIpAddress = "127.0.0.1";

		Map<String, List<PortBinding>> ports = containerInfo.networkSettings().ports();

		assertNotNull("Unable to retrieve the ports where to connect to the emulators", ports);
		assertEquals("We expect 1 port to be mapped", 1, ports.size());

		pubsubPort = getPort(ports, INTERNAL_PUBSUB_PORT, "PubSub");

		LOG.info("| Waiting for the emulators to be running");

		// PubSub exposes an "Ok" at the root url when running.
		if (!waitForOkStatus("PubSub", pubsubPort)) {
			// Oops, we did not get an "Ok" within 10 seconds
			startHasFailedKillEverything();
		}
		LOG.info("\\===========================================");
		LOG.info("");
	}

	private static void startHasFailedKillEverything() throws DockerException, InterruptedException {
		LOG.error("|");
		LOG.error("| ==================== ");
		LOG.error("| YOUR TESTS WILL FAIL ");
		LOG.error("| ==================== ");
		LOG.error("|");

		// Kill this container and wipe all connection information
		dockerIpAddress = null;
		pubsubPort = null;
		terminateAndDiscardAnyExistingContainers(false);
	}

	private static final long MAX_RETRY_TIMEOUT = 10000; // Milliseconds

	private static boolean waitForOkStatus(String label, String port) {
		long start = System.currentTimeMillis();
		while (true) {
			try {
				URL url = new URL("http://" + dockerIpAddress + ":" + port + "/");
				HttpURLConnection con = (HttpURLConnection) url.openConnection();
				con.setRequestMethod("GET");
				con.setConnectTimeout(50);
				con.setReadTimeout(50);

				BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
				String inputLine;
				StringBuilder content = new StringBuilder();
				while ((inputLine = in.readLine()) != null) {
					content.append(inputLine);
				}
				in.close();
				con.disconnect();
				if (content.toString().contains("Ok")) {
					LOG.info("| - {} Emulator is running at {}:{}", label, dockerIpAddress, port);
					return true;
				}
			} catch (IOException e) {
				long now = System.currentTimeMillis();
				if (now - start > MAX_RETRY_TIMEOUT) {
					LOG.error("| - PubSub Emulator at {}:{} FAILED to return an Ok status within {} ms ", dockerIpAddress, port, MAX_RETRY_TIMEOUT);
					return false;
				}
				try {
					Thread.sleep(100); // Sleep a very short time
				} catch (InterruptedException e1) {
					// Ignore
				}
			}
		}
	}

	private static String getPort(Map<String, List<PortBinding>> ports, String internalTCPPort, String label) {
		List<PortBinding> portMappings = ports.get(internalTCPPort + "/tcp");
		if (portMappings == null || portMappings.isEmpty()) {
			LOG.info("| {} Emulator --> NOTHING CONNECTED TO {}/tcp", label, internalTCPPort);
			return null;
		}

		return portMappings.get(0).hostPort();
	}

	private static void terminateAndDiscardAnyExistingContainers(boolean warnAboutExisting) throws DockerException, InterruptedException {
		ContainerInfo containerInfo;
		try {
			containerInfo = docker.inspectContainer(CONTAINER_NAME_JUNIT);
			// Already have this container running.

			assertNotNull("We should either get a containerInfo or we get an exception", containerInfo);

			LOG.info("");
			LOG.info("/===========================================");
			if (warnAboutExisting) {
				LOG.warn("|    >>> FOUND OLD EMULATOR INSTANCE RUNNING <<< ");
				LOG.warn("| Destroying that one to keep tests running smoothly.");
			}
			LOG.info("| Cleanup of GCloud Emulator. Log output of container: ");

			if (LOG.isInfoEnabled()) {
				try (LogStream stream = docker.logs(
					containerInfo.id(),
					DockerClient.LogsParam.stdout(),
					DockerClient.LogsParam.stderr())) {
					LOG.info("| > {}", stream.readFully());
				}
			}

			// We REQUIRE 100% accurate side effect free unit tests
			// So we completely discard this one.

			String id = containerInfo.id();
			// Kill container
			if (containerInfo.state().running()) {
				docker.killContainer(id);
				LOG.info("| - Killed");
			}

			// Remove container
			docker.removeContainer(id);

			LOG.info("| - Removed");
			LOG.info("\\===========================================");
			LOG.info("");

		} catch (ContainerNotFoundException cnfe) {
			// No such container. Good !
		}
	}

	public static void terminateDocker() throws DockerException, InterruptedException {
		terminateAndDiscardAnyExistingContainers(false);

		// Close the docker client
		docker.close();
	}

	// ====================================================================================

}
