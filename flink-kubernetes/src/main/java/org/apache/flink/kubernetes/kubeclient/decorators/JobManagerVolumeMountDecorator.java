package org.apache.flink.kubernetes.kubeclient.decorators;

import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;

/**
 * Mounts resources on the JobManager or TaskManager pod. Resource can be PVC, Secret or ConfigMap for Job Manager.
 */
public class JobManagerVolumeMountDecorator extends VolumeMountDecorator {
	public JobManagerVolumeMountDecorator(
		AbstractKubernetesParameters kubernetesComponentConf) {
		super(kubernetesComponentConf, KubernetesConfigOptions.JOBMANAGER_VOLUME_MOUNT);
	}
}
