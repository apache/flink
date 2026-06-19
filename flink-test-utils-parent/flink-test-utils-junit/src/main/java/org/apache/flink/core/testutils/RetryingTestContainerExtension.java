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

package org.apache.flink.core.testutils;

import com.github.dockerjava.api.command.PullImageResultCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

import javax.annotation.Nullable;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * A {@link CustomExtension} that manages a {@link GenericContainer} with retry logic, including
 * re-pulling the Docker image on failure. This handles transient Docker image pull/build failures
 * that can occur in CI environments.
 *
 * @param <T> The {@link GenericContainer} that shall be managed.
 */
public class RetryingTestContainerExtension<T extends GenericContainer<T>>
        implements CustomExtension {

    private static final Logger LOG = LoggerFactory.getLogger(RetryingTestContainerExtension.class);
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final long DEFAULT_RETRY_DELAY_MS = 2000;
    private static final long IMAGE_PULL_TIMEOUT_MINUTES = 2;

    @Nullable private T testContainer;

    private final Supplier<T> testContainerCreator;
    private final int maxRetries;
    private final long retryDelayMs;

    public RetryingTestContainerExtension(Supplier<T> testContainerCreator) {
        this(testContainerCreator, DEFAULT_MAX_RETRIES, DEFAULT_RETRY_DELAY_MS);
    }

    public RetryingTestContainerExtension(
            Supplier<T> testContainerCreator, int maxRetries, long retryDelayMs) {
        this.testContainerCreator = testContainerCreator;
        this.maxRetries = maxRetries;
        this.retryDelayMs = retryDelayMs;
    }

    public T getTestContainer() {
        assert testContainer != null;
        return testContainer;
    }

    private void terminateTestContainer() {
        if (testContainer != null) {
            testContainer.stop();
            testContainer = null;
        }
    }

    private void instantiateTestContainer() {
        assert testContainer == null;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                testContainer = testContainerCreator.get();
                testContainer.start();
                return;
            } catch (Exception e) {
                LOG.warn(
                        "Container start attempt {}/{} failed: {}",
                        attempt,
                        maxRetries,
                        e.getMessage());
                testContainer = null;
                if (attempt == maxRetries) {
                    throw e;
                }
                pullImage();
                try {
                    Thread.sleep(retryDelayMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted during container start retry", ie);
                }
            }
        }
    }

    private void pullImage() {
        try {
            T tempContainer = testContainerCreator.get();
            String imageName = tempContainer.getDockerImageName();
            LOG.info("Re-pulling image {} before retry...", imageName);
            DockerClientFactory.instance()
                    .client()
                    .pullImageCmd(imageName)
                    .exec(new PullImageResultCallback())
                    .awaitCompletion(IMAGE_PULL_TIMEOUT_MINUTES, TimeUnit.MINUTES);
            LOG.info("Image {} pulled successfully", imageName);
        } catch (Exception e) {
            LOG.warn("Failed to pull image: {}", e.getMessage());
        }
    }

    @Override
    public void after(ExtensionContext context) throws Exception {
        terminateTestContainer();
    }

    @Override
    public void before(ExtensionContext context) throws Exception {
        terminateTestContainer();
        instantiateTestContainer();
    }
}
