package org.apache.flink.kubernetes.kubeclient.decorators;

import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;

/**
 * Mounts resources on the JobManager or TaskManager pod. Resource can be PVC, Secret or ConfigMap for Task Manager.
 */
public class FlinkTaskManagerEnvVarDecorator extends FlinkEnvironmentVariablesDecorator {
	public FlinkTaskManagerEnvVarDecorator(
		AbstractKubernetesParameters kubernetesComponentConf) {
		super(kubernetesComponentConf, KubernetesConfigOptions.TASKMANAGER_ENV_VARIABLES);
	}
}
