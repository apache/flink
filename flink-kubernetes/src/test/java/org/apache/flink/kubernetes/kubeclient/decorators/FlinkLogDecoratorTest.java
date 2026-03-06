package org.apache.flink.kubernetes.kubeclient.decorators;


import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FlinkLogDecoratorTest {

    private static final String VOLUME_MOUNT_PATH = "/apps/log/flink";
    private static final String VOLUME_LOGS = "/opt/flink/log";

    @Mock
    private AbstractKubernetesParameters kubernetesComponentConf;

    @InjectMocks
    private FlinkLogDecorator flinkLogDecorator;

    @Before
    public void setUp() {
        when(kubernetesComponentConf.getVolumeMountPath()).thenReturn(VOLUME_MOUNT_PATH);
        when(kubernetesComponentConf.getVolumeLogs()).thenReturn(VOLUME_LOGS);
    }

    @Test
    public void decorateFlinkPod_ShouldAddVolumeAndVolumeMount() {
        FlinkPod originalFlinkPod = new FlinkPod.Builder()
            .withPod(new PodBuilder().build())
            .withMainContainer(new ContainerBuilder().build())
            .build();

        FlinkPod decoratedFlinkPod = flinkLogDecorator.decorateFlinkPod(originalFlinkPod);

        Pod decoratedPod = decoratedFlinkPod.getPodWithoutMainContainer();
        Container decoratedContainer = decoratedFlinkPod.getMainContainer();

        assertEquals(1, decoratedPod.getSpec().getVolumes().size());
        Volume volume = decoratedPod.getSpec().getVolumes().get(0);
        assertEquals("flink-log", volume.getName());
        assertEquals(VOLUME_LOGS, volume.getHostPath().getPath());
        assertEquals("DirectoryOrCreate", volume.getHostPath().getType());

        assertEquals(1, decoratedContainer.getVolumeMounts().size());
        VolumeMount volumeMount = decoratedContainer.getVolumeMounts().get(0);
        assertEquals("flink-log", volumeMount.getName());
        assertEquals(VOLUME_MOUNT_PATH, volumeMount.getMountPath());
    }
}
