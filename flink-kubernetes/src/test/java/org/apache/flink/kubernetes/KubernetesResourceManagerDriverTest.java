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

package org.apache.flink.kubernetes;

import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesResourceManagerDriverConfiguration;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient.WatchCallbackHandler;
import org.apache.flink.kubernetes.kubeclient.TestingFlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesTooOldResourceVersionException;
import org.apache.flink.kubernetes.kubeclient.resources.TestingKubernetesPod;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.runtime.resourcemanager.active.ResourceManagerDriver;
import org.apache.flink.runtime.resourcemanager.active.ResourceManagerDriverTestBase;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.util.concurrent.FutureUtils;

import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.client.KubernetesClientException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link KubernetesResourceManagerDriver}. */
class KubernetesResourceManagerDriverTest
        extends ResourceManagerDriverTestBase<KubernetesWorkerNode> {

    private static final String CLUSTER_ID = "testing-flink-cluster";
    private static final KubernetesResourceManagerDriverConfiguration
            KUBERNETES_RESOURCE_MANAGER_CONFIGURATION =
                    new KubernetesResourceManagerDriverConfiguration(CLUSTER_ID, "localhost:9000");

    @Test
    void testCancelRequestedResource() throws Exception {
        new Context() {
            {
                final CompletableFuture<KubernetesPod> createPodFuture = new CompletableFuture<>();
                final CompletableFuture<Void> createTaskManagerPodFuture =
                        new CompletableFuture<>();
                final CompletableFuture<String> stopPodFuture = new CompletableFuture<>();

                flinkKubeClientBuilder
                        .setCreateTaskManagerPodFunction(
                                (pod) -> {
                                    createPodFuture.complete(pod);
                                    return createTaskManagerPodFuture;
                                })
                        .setStopPodFunction(
                                (podName) -> {
                                    stopPodFuture.complete(podName);
                                    return FutureUtils.completedVoidFuture();
                                });

                runTest(
                        () -> {
                            // request new pod and then cancel it.
                            runInMainThread(
                                    () -> {
                                        CompletableFuture<KubernetesWorkerNode> requestFuture =
                                                getDriver()
                                                        .requestResource(
                                                                TASK_EXECUTOR_PROCESS_SPEC);
                                        requestFuture.cancel(true);
                                    });

                            final KubernetesPod pod =
                                    new TestingKubernetesPod(
                                            createPodFuture
                                                    .get(TIMEOUT_SEC, TimeUnit.SECONDS)
                                                    .getName(),
                                            false,
                                            true);

                            assertThat(stopPodFuture.isDone()).isFalse();
                            runInMainThread(() -> createTaskManagerPodFuture.complete(null));
                            // pod should be stopped when create pod rpc finished.
                            final CompletableFuture<Void> validationFuture =
                                    stopPodFuture.thenAccept(
                                            (podName) ->
                                                    assertThat(podName).isEqualTo(pod.getName()));
                            validationFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS);
                        });
            }
        };
    }

    @Test
    void testOnPodAdded() throws Exception {
        new Context() {
            {
                final CompletableFuture<KubernetesPod> createPodFuture = new CompletableFuture<>();
                final CompletableFuture<KubernetesWorkerNode> requestResourceFuture =
                        new CompletableFuture<>();

                flinkKubeClientBuilder.setCreateTaskManagerPodFunction(
                        (pod) -> {
                            createPodFuture.complete(pod);
                            return FutureUtils.completedVoidFuture();
                        });

                runTest(
                        () -> {
                            // request new pod
                            runInMainThread(
                                    () ->
                                            getDriver()
                                                    .requestResource(TASK_EXECUTOR_PROCESS_SPEC)
                                                    .thenAccept(requestResourceFuture::complete));
                            final KubernetesPod pod =
                                    new TestingKubernetesPod(
                                            createPodFuture
                                                    .get(TIMEOUT_SEC, TimeUnit.SECONDS)
                                                    .getName(),
                                            true,
                                            false);

                            // prepare validation:
                            // - complete requestResourceFuture in main thread with correct
                            // KubernetesWorkerNode
                            final CompletableFuture<Void> validationFuture =
                                    requestResourceFuture.thenAccept(
                                            (workerNode) -> {
                                                validateInMainThread();
                                                assertThat(workerNode.getResourceID())
                                                        .asString()
                                                        .isEqualTo(pod.getName());
                                            });

                            // send onAdded event
                            getPodCallbackHandler().onAdded(Collections.singletonList(pod));

                            // make sure finishing validation
                            validationFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS);
                        });
            }
        };
    }

    @Test
    void testOnPodModified() throws Exception {
        new Context() {
            {
                testOnPodTerminated((pod) -> getPodCallbackHandler().onModified(pod));
            }
        };
    }

    @Test
    void testOnPodDeleted() throws Exception {
        new Context() {
            {
                testOnPodTerminated((pod) -> getPodCallbackHandler().onDeleted(pod));
            }
        };
    }

    @Test
    void testOnPodDeletedBeforeScheduled() throws Exception {
        new Context() {
            {
                // If the pod is deleted during the pending phase, we cannot detect the pod is
                // terminated because its status won't be updated, but should handle the deleted
                // event
                testOnPodTerminated((pod) -> getPodCallbackHandler().onDeleted(pod), false, false);
            }
        };
    }

    @Test
    void testOnError() throws Exception {
        new Context() {
            {
                testOnPodTerminated((pod) -> getPodCallbackHandler().onError(pod));
            }
        };
    }

    @Test
    void testFatalHandleError() throws Exception {
        new Context() {
            {
                final CompletableFuture<Throwable> onErrorFuture = new CompletableFuture<>();
                resourceEventHandlerBuilder.setOnErrorConsumer(onErrorFuture::complete);

                runTest(
                        () -> {
                            final Throwable testingError = new Throwable("testing error");
                            getPodCallbackHandler().handleError(testingError);
                            assertThat(onErrorFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS))
                                    .isEqualTo(testingError);
                        });
            }
        };
    }

    @Test
    void testKubernetesExceptionHandling() throws Exception {
        new Context() {
            {
                runTest(
                        () ->
                                FlinkAssertions.assertThatFuture(
                                                runInMainThread(
                                                        () -> {
                                                            getDriver()
                                                                    .requestResource(
                                                                            TASK_EXECUTOR_PROCESS_SPEC)
                                                                    .completeExceptionally(
                                                                            new CompletionException(
                                                                                    new KubernetesClientException(
                                                                                            "test")));
                                                        }))
                                        .eventuallySucceeds());
            }
        };
    }

    @Test
    void testRecoverPreviousAttemptWorkersPodTerminated() throws Exception {
        new Context() {
            {
                final KubernetesPod previousAttemptPod =
                        new TestingKubernetesPod(CLUSTER_ID + "-taskmanager-1-1", true, true);
                final CompletableFuture<String> stopPodFuture = new CompletableFuture<>();
                final CompletableFuture<Collection<KubernetesWorkerNode>> recoveredWorkersFuture =
                        new CompletableFuture<>();

                flinkKubeClientBuilder
                        .setGetPodsWithLabelsFunction(
                                (ignore) -> Collections.singletonList(previousAttemptPod))
                        .setStopPodFunction(
                                (podName) -> {
                                    stopPodFuture.complete(podName);
                                    return FutureUtils.completedVoidFuture();
                                });

                resourceEventHandlerBuilder.setOnPreviousAttemptWorkersRecoveredConsumer(
                        recoveredWorkersFuture::complete);

                runTest(
                        () -> {
                            // validate the terminated pod from previous attempt is not recovered
                            // and is removed
                            assertThat(recoveredWorkersFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS))
                                    .isEmpty();
                            assertThat(stopPodFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS))
                                    .isEqualTo(previousAttemptPod.getName());
                        });
            }
        };
    }

    @Test
    void testNewWatchCreationWhenKubernetesTooOldResourceVersionException() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            getPodCallbackHandler()
                                    .handleError(
                                            new KubernetesTooOldResourceVersionException(
                                                    new Exception("too old resource version")));
                            // Verify the old watch is closed and a new one is created
                            CommonTestUtils.waitUntilCondition(() -> getPodsWatches().size() == 2);
                            assertThat(getPodsWatches().get(0).isClosed()).isTrue();
                            assertThat(getPodsWatches().get(1).isClosed()).isFalse();
                        });
            }
        };
    }

    @Test
    public void testThrowExceptionWhenWatchPodsFailInInitializing() throws Exception {
        new Context() {
            {
                assertThatThrownBy(
                                () -> {
                                    flinkKubeClientBuilder.setWatchPodsAndDoCallbackFunction(
                                            (ignore1, ignore2) -> {
                                                throw new ExpectedTestException();
                                            });
                                    runTest(() -> {});
                                })
                        .isInstanceOf(ExpectedTestException.class);
            }
        };
    }

    @Test
    void testThrowExceptionWhenWatchPodsFailInHandlingError() throws Exception {
        new Context() {
            {
                final CompletableFuture<Throwable> onErrorFuture = new CompletableFuture<>();
                resourceEventHandlerBuilder.setOnErrorConsumer(onErrorFuture::complete);
                final CompletableFuture<Void> initWatchFuture = new CompletableFuture<>();
                final ExpectedTestException testingError = new ExpectedTestException();
                flinkKubeClientBuilder.setWatchPodsAndDoCallbackFunction(
                        (ignore, handler) -> {
                            if (initWatchFuture.isDone()) {
                                throw testingError;
                            } else {
                                initWatchFuture.complete(null);
                                getSetWatchPodsAndDoCallbackFuture().complete(handler);
                                return new TestingFlinkKubeClient.MockKubernetesWatch();
                            }
                        });
                runTest(
                        () -> {
                            getPodCallbackHandler().handleError(testingError);
                            assertThat(onErrorFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS))
                                    .isEqualTo(testingError);
                        });
            }
        };
    }

    @Override
    protected ResourceManagerDriverTestBase<KubernetesWorkerNode>.Context createContext() {
        return new Context();
    }

    private class Context extends ResourceManagerDriverTestBase<KubernetesWorkerNode>.Context {
        private final KubernetesPod previousAttemptPod =
                new TestingKubernetesPod(CLUSTER_ID + "-taskmanager-1-1");

        private final CompletableFuture<WatchCallbackHandler<KubernetesPod>>
                setWatchPodsAndDoCallbackFuture = new CompletableFuture<>();
        private final CompletableFuture<Void> closeKubernetesWatchFuture =
                new CompletableFuture<>();

        private final List<TestingFlinkKubeClient.MockKubernetesWatch> podsWatches =
                new ArrayList<>();
        private final CompletableFuture<String> stopAndCleanupClusterFuture =
                new CompletableFuture<>();
        private final CompletableFuture<KubernetesPod> createTaskManagerPodFuture =
                new CompletableFuture<>();
        private final CompletableFuture<String> stopPodFuture = new CompletableFuture<>();

        final TestingFlinkKubeClient.Builder flinkKubeClientBuilder =
                TestingFlinkKubeClient.builder()
                        .setWatchPodsAndDoCallbackFunction(
                                (ignore, handler) -> {
                                    setWatchPodsAndDoCallbackFuture.complete(handler);
                                    final TestingFlinkKubeClient.MockKubernetesWatch watch =
                                            new TestingFlinkKubeClient.MockKubernetesWatch() {
                                                @Override
                                                public void close() {
                                                    super.close();
                                                    closeKubernetesWatchFuture.complete(null);
                                                }
                                            };
                                    podsWatches.add(watch);
                                    return watch;
                                })
                        .setStopAndCleanupClusterConsumer(stopAndCleanupClusterFuture::complete)
                        .setCreateTaskManagerPodFunction(
                                (pod) -> {
                                    createTaskManagerPodFuture.complete(pod);
                                    getPodCallbackHandler()
                                            .onAdded(
                                                    Collections.singletonList(
                                                            new TestingKubernetesPod(
                                                                    pod.getName(), true, false)));
                                    return FutureUtils.completedVoidFuture();
                                })
                        .setStopPodFunction(
                                (podName) -> {
                                    stopPodFuture.complete(podName);
                                    return FutureUtils.completedVoidFuture();
                                });

        private TestingFlinkKubeClient flinkKubeClient;

        FlinkKubeClient.WatchCallbackHandler<KubernetesPod> getPodCallbackHandler() {
            try {
                return setWatchPodsAndDoCallbackFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS);
            } catch (Exception e) {
                fail("Cannot get WatchCallbackHandler, cause: " + e.getMessage());
            }
            return null;
        }

        List<TestingFlinkKubeClient.MockKubernetesWatch> getPodsWatches() {
            return podsWatches;
        }

        CompletableFuture<WatchCallbackHandler<KubernetesPod>>
                getSetWatchPodsAndDoCallbackFuture() {
            return setWatchPodsAndDoCallbackFuture;
        }

        @Override
        protected void prepareRunTest() {
            flinkConfig.setString(KubernetesConfigOptions.CLUSTER_ID, CLUSTER_ID);
            flinkConfig.setString(
                    TaskManagerOptions.RPC_PORT, String.valueOf(Constants.TASK_MANAGER_RPC_PORT));

            flinkKubeClient = flinkKubeClientBuilder.build();
        }

        @Override
        protected void preparePreviousAttemptWorkers() {
            flinkKubeClientBuilder.setGetPodsWithLabelsFunction(
                    (ignore) -> Collections.singletonList(previousAttemptPod));
        }

        @Override
        protected ResourceManagerDriver<KubernetesWorkerNode> createResourceManagerDriver() {
            return new KubernetesResourceManagerDriver(
                    flinkConfig, flinkKubeClient, KUBERNETES_RESOURCE_MANAGER_CONFIGURATION);
        }

        @Override
        protected void validateInitialization() throws Exception {
            assertThat(getPodCallbackHandler()).isNotNull();
        }

        @Override
        protected void validateWorkersRecoveredFromPreviousAttempt(
                Collection<KubernetesWorkerNode> workers) {
            assertThat(workers).hasSize(1);

            final ResourceID resourceId = workers.iterator().next().getResourceID();
            assertThat(resourceId).asString().isEqualTo(previousAttemptPod.getName());
        }

        @Override
        protected void validateTermination() throws Exception {
            closeKubernetesWatchFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS);
        }

        @Override
        protected void validateDeregisterApplication() throws Exception {
            assertThat(stopAndCleanupClusterFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS))
                    .isEqualTo(CLUSTER_ID);
        }

        @Override
        protected void validateRequestedResources(
                Collection<TaskExecutorProcessSpec> taskExecutorProcessSpecs) throws Exception {
            assertThat(taskExecutorProcessSpecs).hasSize(1);

            final TaskExecutorProcessSpec taskExecutorProcessSpec =
                    taskExecutorProcessSpecs.iterator().next();
            final KubernetesPod pod = createTaskManagerPodFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS);
            final ResourceRequirements resourceRequirements =
                    pod.getInternalResource().getSpec().getContainers().get(0).getResources();

            assertThat(
                            resourceRequirements
                                    .getRequests()
                                    .get(Constants.RESOURCE_NAME_MEMORY)
                                    .getAmount())
                    .isEqualTo(
                            String.valueOf(
                                    taskExecutorProcessSpec
                                            .getTotalProcessMemorySize()
                                            .getMebiBytes()));
            assertThat(
                            resourceRequirements
                                    .getRequests()
                                    .get(Constants.RESOURCE_NAME_CPU)
                                    .getAmount())
                    .isEqualTo(
                            String.valueOf(
                                    taskExecutorProcessSpec
                                            .getCpuCores()
                                            .getValue()
                                            .doubleValue()));

            assertThat(
                            resourceRequirements
                                    .getLimits()
                                    .get(Constants.RESOURCE_NAME_MEMORY)
                                    .getAmount())
                    .isEqualTo(
                            String.valueOf(
                                    taskExecutorProcessSpec
                                            .getTotalProcessMemorySize()
                                            .getMebiBytes()));
            assertThat(
                            resourceRequirements
                                    .getLimits()
                                    .get(Constants.RESOURCE_NAME_CPU)
                                    .getAmount())
                    .isEqualTo(
                            String.valueOf(
                                    taskExecutorProcessSpec
                                            .getCpuCores()
                                            .getValue()
                                            .doubleValue()));
        }

        @Override
        protected void validateReleaseResources(Collection<KubernetesWorkerNode> workerNodes)
                throws Exception {
            assertThat(workerNodes).hasSize(1);

            final ResourceID resourceId = workerNodes.iterator().next().getResourceID();
            assertThat(stopPodFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS))
                    .isEqualTo(resourceId.toString());
        }

        void testOnPodTerminated(Consumer<List<KubernetesPod>> sendPodTerminatedEvent)
                throws Exception {
            testOnPodTerminated(sendPodTerminatedEvent, true, true);
        }

        void testOnPodTerminated(
                Consumer<List<KubernetesPod>> sendPodTerminatedEvent,
                boolean isPodScheduled,
                boolean isPodTerminated)
                throws Exception {
            final CompletableFuture<KubernetesWorkerNode> requestResourceFuture =
                    new CompletableFuture<>();
            final CompletableFuture<ResourceID> onWorkerTerminatedConsumer =
                    new CompletableFuture<>();

            resourceEventHandlerBuilder.setOnWorkerTerminatedConsumer(
                    (resourceId, ignore) -> onWorkerTerminatedConsumer.complete(resourceId));

            runTest(
                    () -> {
                        // request new pod and send onAdded event
                        runInMainThread(
                                () ->
                                        getDriver()
                                                .requestResource(TASK_EXECUTOR_PROCESS_SPEC)
                                                .thenAccept(requestResourceFuture::complete));
                        final KubernetesPod pod =
                                createTaskManagerPodFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS);

                        // prepare validation
                        // - pod removed
                        // - onTerminated is called in main thread with correct resource id
                        final CompletableFuture<Void> validationFuture =
                                CompletableFuture.allOf(
                                        stopPodFuture.thenAccept(
                                                (podname) ->
                                                        assertThat(podname)
                                                                .isEqualTo(pod.getName())),
                                        onWorkerTerminatedConsumer.thenAccept(
                                                (resourceId) -> {
                                                    validateInMainThread();
                                                    assertThat(resourceId)
                                                            .asString()
                                                            .isEqualTo(pod.getName());
                                                }));

                        sendPodTerminatedEvent.accept(
                                Collections.singletonList(
                                        new TestingKubernetesPod(
                                                pod.getName(), isPodScheduled, isPodTerminated)));

                        // make sure finishing validation
                        validationFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS);
                    });
        }
    }
}
