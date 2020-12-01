package org.apache.flink.kubernetes.kubeclient.decorators;

import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;

/**
 * Mounts resources on the JobManager or TaskManager pod. Resource can be PVC, Secret or ConfigMap for Job Manager.
 */
public class TaskManagerConfigurationMountDecorator extends ConfigurationMountDecorator {
	public TaskManagerConfigurationMountDecorator(
		AbstractKubernetesParameters kubernetesComponentConf) {
		super(kubernetesComponentConf, KubernetesConfigOptions.TASKMANAGER_CONFIGURATION_MOUNT);
	}
}
