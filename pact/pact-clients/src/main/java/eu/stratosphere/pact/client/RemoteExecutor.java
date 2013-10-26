package eu.stratosphere.pact.client;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;

import eu.stratosphere.pact.client.nephele.api.Client;
import eu.stratosphere.pact.client.nephele.api.PlanWithJars;
import eu.stratosphere.pact.common.plan.Plan;

public class RemoteExecutor implements PlanExecutor {
	
	private Client client;
	private List<String> jarFiles;
	
	public RemoteExecutor(String hostname, int port, List<String> jarFiles) {
		this.client = new Client(new InetSocketAddress(hostname, port));
		this.jarFiles = jarFiles;

	}

	public RemoteExecutor(String hostname, int port, String jarFile) {
		this(hostname, port, Collections.singletonList(jarFile));
	}

	@Override
	public long executePlan(Plan plan) throws Exception {
		PlanWithJars p = new PlanWithJars(plan, jarFiles);
		client.run(p, true);
		return 0;
	}
}
