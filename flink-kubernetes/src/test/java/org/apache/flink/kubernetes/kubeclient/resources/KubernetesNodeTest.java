package org.apache.flink.kubernetes.kubeclient.resources;

import org.apache.flink.kubernetes.kubeclient.resources.KubernetesNode.NodeConditionStatus;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesNode.NodeConditions;

import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeBuilder;
import io.fabric8.kubernetes.api.model.NodeConditionBuilder;
import io.fabric8.kubernetes.api.model.NodeStatusBuilder;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KubernetesNode}. */
public class KubernetesNodeTest {
    @Test
    void testIsNodeNotReady() {
        final Node node = new NodeBuilder().build();
        node.setStatus(
                new NodeStatusBuilder()
                        .withConditions(
                                new NodeConditionBuilder()
                                        .withType(NodeConditions.Ready.name())
                                        .withMessage("kubelet is posting ready status")
                                        .withStatus(NodeConditionStatus.True.name())
                                        .withReason("KubeletReady")
                                        .build())
                        .build());
        assertThat(new KubernetesNode(node).isNodeNotReady()).isFalse();

        node.setStatus(
                new NodeStatusBuilder()
                        .withConditions(
                                new NodeConditionBuilder()
                                        .withType(NodeConditions.Ready.name())
                                        .withMessage("kubelet is posting not ready status")
                                        .withStatus(NodeConditionStatus.False.name())
                                        .withReason("KubeletNotReady")
                                        .build())
                        .build());
        assertThat(new KubernetesNode(node).isNodeNotReady()).isTrue();

        node.setStatus(
                new NodeStatusBuilder()
                        .withConditions(
                                new NodeConditionBuilder()
                                        .withType(NodeConditions.Ready.name())
                                        .withMessage("kubelet is posting unknown status")
                                        .withStatus(NodeConditionStatus.Unknown.name())
                                        .withReason("KubeletUnknown")
                                        .build())
                        .build());
        assertThat(new KubernetesNode(node).isNodeNotReady()).isTrue();
    }
}
