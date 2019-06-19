package org.apache.flink.kubernetes;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.flink.kubernetes.kubeclient.fabric8.Fabric8FlinkKubeClient;
import org.apache.flink.util.TestLogger;
import org.junit.Rule;

import java.util.Arrays;
import java.util.UUID;

/**
 * Created by niki.lj on 2019/6/18.
 */
public class KubernetesTestBase extends TestLogger {
	@Rule
	public MixedMockKubernetesServer server = new MixedMockKubernetesServer(false, true);

	protected Fabric8FlinkKubeClient getClient(FlinkKubernetesOptions options){
		KubernetesClient client = server.getClient();
		if (options.getServiceUUID() == null) {
			options.setServiceUUID(UUID.randomUUID().toString());
		}
		//only support namespace test
		options.setNameSpace("test");
		Fabric8FlinkKubeClient flinkKubeClient = new Fabric8FlinkKubeClient(options, client);
		flinkKubeClient.initialize();
		return flinkKubeClient;
	}

	protected void setActionWatcher(String resourceName) {

		ObjectMeta meta = new ObjectMeta();
		meta.setResourceVersion("1");
		meta.setNamespace(server.getClient().getNamespace());
		meta.setName(resourceName);

		Service serviceWithIngress = new ServiceBuilder()
			.withMetadata(meta)
			.withNewStatus()
			.withLoadBalancer(new LoadBalancerStatus(Arrays.asList(new LoadBalancerIngress("test-service", "192.168.0.1"))))
			.and()
			.build();

		String path = "/api/v1/namespaces/test/services?fieldSelector=metadata.name%3D" + resourceName + "&watch=true";
		server.expect()
			.withPath(path)
			.andUpgradeToWebSocket()
			.open()
			.waitFor(1000)
			.andEmit(new WatchEvent(serviceWithIngress, "ADDED"))
			.done()
			.once();

	}
}
