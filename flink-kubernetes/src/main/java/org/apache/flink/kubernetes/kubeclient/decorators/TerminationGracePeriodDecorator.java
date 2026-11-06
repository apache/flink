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

package org.apache.flink.kubernetes.kubeclient.decorators;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ExecActionBuilder;
import io.fabric8.kubernetes.api.model.PodBuilder;

import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Sets {@code terminationGracePeriodSeconds} and a {@code preStop} lifecycle hook on a pod, gated
 * by a termination-grace-period {@link ConfigOption} (see {@code
 * KubernetesConfigOptions#TASK_MANAGER_TERMINATION_GRACE_PERIOD}). Only wired in for the
 * TaskManager today; the {@link ConfigOption} is taken as a constructor argument so the same
 * decorator can be reused for the JobManager once it has an equivalent {@code SIGUSR2} handler.
 *
 * <p>The {@code preStop} hook sends {@code SIGUSR2} to the main container process and then sleeps
 * for most of the remaining grace period before returning - Kubernetes only sends {@code SIGTERM}
 * once {@code preStop} returns, so without that sleep the grace period configured here would never
 * actually be available to already-running tasks; {@code SIGTERM} would follow the signal almost
 * immediately. {@code TaskManagerRunner} interprets {@code SIGUSR2} as a request to prepare for
 * graceful termination: it disconnects from the ResourceManager immediately instead of waiting to
 * be timed out, so the ResourceManager frees this TaskExecutor's slots right away rather than after
 * the heartbeat timeout elapses, while currently-running tasks keep running for the rest of the
 * sleep. A {@code SIGUSR2} handler must exist on the receiving side before this decorator is wired
 * in for a given pod type - Kubernetes' and the JVM's default disposition for an unhandled {@code
 * SIGUSR2} is to terminate the process, which would make termination less graceful, not more.
 *
 * <p>Does nothing if the corresponding option is unset, and never overwrites a {@code
 * terminationGracePeriodSeconds} or {@code preStop} hook already present in the user's pod
 * template.
 */
public class TerminationGracePeriodDecorator extends AbstractKubernetesStepDecorator {

    /**
     * How much of the grace period to reserve for the JVM's own shutdown sequence to run after
     * {@code SIGTERM}, once the {@code preStop} hook returns and Kubernetes sends it.
     */
    private static final long SAFETY_MARGIN_SECONDS = 5;

    private final AbstractKubernetesParameters kubernetesParameters;
    private final ConfigOption<Duration> terminationGracePeriodOption;

    public TerminationGracePeriodDecorator(
            AbstractKubernetesParameters kubernetesParameters,
            ConfigOption<Duration> terminationGracePeriodOption) {
        this.kubernetesParameters = checkNotNull(kubernetesParameters);
        this.terminationGracePeriodOption = checkNotNull(terminationGracePeriodOption);
    }

    @Override
    public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
        final Configuration flinkConfig = kubernetesParameters.getFlinkConfiguration();
        if (!flinkConfig.contains(terminationGracePeriodOption)) {
            return flinkPod;
        }
        final Duration terminationGracePeriod = flinkConfig.get(terminationGracePeriodOption);

        final PodBuilder podBuilder = new PodBuilder(flinkPod.getPodWithoutMainContainer());
        if (flinkPod.getPodWithoutMainContainer().getSpec().getTerminationGracePeriodSeconds()
                == null) {
            podBuilder
                    .editOrNewSpec()
                    .withTerminationGracePeriodSeconds(terminationGracePeriod.getSeconds())
                    .endSpec();
        } else {
            logger.info(
                    "Not overwriting terminationGracePeriodSeconds already set by the pod "
                            + "template; '{}' has no effect on the grace period.",
                    terminationGracePeriodOption.key());
        }

        final Container mainContainer = flinkPod.getMainContainer();
        final Container decoratedMainContainer;
        if (mainContainer.getLifecycle() != null
                && mainContainer.getLifecycle().getPreStop() != null) {
            logger.info(
                    "Not overwriting the preStop hook already set by the pod template; '{}' has "
                            + "no effect on the container lifecycle.",
                    terminationGracePeriodOption.key());
            decoratedMainContainer = mainContainer;
        } else {
            decoratedMainContainer =
                    new ContainerBuilder(mainContainer)
                            .editOrNewLifecycle()
                            .withNewPreStop()
                            .withExec(
                                    new ExecActionBuilder()
                                            .withCommand(
                                                    "sh",
                                                    "-c",
                                                    buildGracefulTerminationCommand(
                                                            terminationGracePeriod))
                                            .build())
                            .endPreStop()
                            .endLifecycle()
                            .build();
        }

        return new FlinkPod.Builder(flinkPod)
                .withPod(podBuilder.build())
                .withMainContainer(decoratedMainContainer)
                .build();
    }

    /**
     * {@code 1} is the main container process because Flink's docker-entrypoint.sh execs it
     * directly rather than running it as a child of the shell. The sleep keeps {@code preStop} from
     * returning - and therefore Kubernetes from sending {@code SIGTERM} - until most of the grace
     * period has elapsed, leaving {@link #SAFETY_MARGIN_SECONDS} for the JVM's own shutdown
     * sequence to run afterwards.
     */
    private static String buildGracefulTerminationCommand(Duration terminationGracePeriod) {
        final long sleepSeconds =
                Math.max(0, terminationGracePeriod.getSeconds() - SAFETY_MARGIN_SECONDS);
        return String.format("kill -USR2 1 2>/dev/null || true; sleep %d", sleepSeconds);
    }
}
