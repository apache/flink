package org.apache.flink.kubernetes.kubeclient.decorators.extended.volcano;

import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.decorators.extended.ExtPluginDecorator;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesTaskManagerParameters;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.volcano.scheduling.v1beta1.PodGroupBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The interface class which should be implemented by plugin decorators. */
public class VolcanoStepDecorator implements ExtPluginDecorator {
    private AbstractKubernetesParameters kubernetesComponentConf = null;
    private Boolean isTaskManager = Boolean.FALSE;
    private static final String DEFAULT_SCHEDULER_NAME = "default-scheduler";
    private static final String VOLCANO_SCHEDULER_NAME = "volcano";
    private static final String priorityClassName = "priorityClassName";
    private static final String minMember = "minMember";
    private static final String queue = "queue";

    public VolcanoStepDecorator() {}

    @Override
    public void configure(AbstractKubernetesParameters kubernetesComponentConf) {
        this.kubernetesComponentConf = checkNotNull(kubernetesComponentConf);
        if (this.kubernetesComponentConf instanceof KubernetesTaskManagerParameters) {
            this.isTaskManager = Boolean.TRUE;
        }
    }

    @Override
    public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
        String configuredSchedulerName = kubernetesComponentConf.getPodSchedulerName();
        if (configuredSchedulerName == null
                || configuredSchedulerName.equals(DEFAULT_SCHEDULER_NAME)) {
            return flinkPod;
        }
        final PodBuilder basicPodBuilder = new PodBuilder(flinkPod.getPodWithoutMainContainer());
        String fakename = this.kubernetesComponentConf.getClusterId();
        basicPodBuilder
                .editOrNewMetadata()
                .withAnnotations(
                        Collections.singletonMap("scheduling.k8s.io/group-name", "pg-" + fakename))
                .endMetadata();
        return new FlinkPod.Builder(flinkPod).withPod(basicPodBuilder.build()).build();
    }

    @Override
    public List<HasMetadata> buildAccompanyingKubernetesResources() throws IOException {
        return Collections.emptyList();
    }

    @Override
    public List<HasMetadata> buildPrePreparedResources() {
        String configuredSchedulerName = kubernetesComponentConf.getPodSchedulerName();
        if (!this.isTaskManager && configuredSchedulerName.equals(VOLCANO_SCHEDULER_NAME)) {
            String fakename = this.kubernetesComponentConf.getClusterId();
            PodGroupBuilder podGroupBuilder = new PodGroupBuilder();
            podGroupBuilder.editOrNewMetadata().withName("pg-" + fakename).endMetadata();
            Map<String, String> podGroupConfig = kubernetesComponentConf.getPodGroupConfig();
            for (Map.Entry<String, String> stringStringEntry : podGroupConfig.entrySet()) {
                switch (stringStringEntry.getKey()) {
                    case priorityClassName:
                        podGroupBuilder
                                .editOrNewSpec()
                                .withPriorityClassName(stringStringEntry.getValue())
                                .endSpec();
                        break;
                    case minMember:
                        podGroupBuilder
                                .editOrNewSpec()
                                .withMinMember(Integer.parseInt(stringStringEntry.getValue()))
                                .endSpec();
                        break;
                    case queue:
                        podGroupBuilder
                                .editOrNewSpec()
                                .withQueue(stringStringEntry.getValue())
                                .endSpec();
                        break;
                }
            }
            return Collections.singletonList(podGroupBuilder.build());
        }
        return Collections.emptyList();
    }
}
