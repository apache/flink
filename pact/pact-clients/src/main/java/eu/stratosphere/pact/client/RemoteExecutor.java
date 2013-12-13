package eu.stratosphere.pact.client;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;

import eu.stratosphere.nephele.client.JobExecutionResult;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.client.nephele.api.Client;
import eu.stratosphere.pact.client.nephele.api.PlanWithJars;
import eu.stratosphere.pact.common.plan.Plan;

public class RemoteExecutor implements PlanExecutor {

	private Client client;

	private List<String> jarFiles;

	public RemoteExecutor(InetSocketAddress inet, List<String> jarFiles) {
		this.client = new Client(inet, new Configuration());
		this.jarFiles = jarFiles;
	}
	
	public RemoteExecutor(String hostname, int port, List<String> jarFiles) {
		this(new InetSocketAddress(hostname, port), jarFiles);
	}

	public RemoteExecutor(String hostname, int port, String jarFile) {
		this(hostname, port, Collections.singletonList(jarFile));
	}
	
	public RemoteExecutor(String hostport, String jarFile) {
		this(getInetFromHostport(hostport), Collections.singletonList(jarFile));
	}
	
	public static InetSocketAddress getInetFromHostport(String hostport) {
		// from http://stackoverflow.com/questions/2345063/java-common-way-to-validate-and-convert-hostport-to-inetsocketaddress
		URI uri;
		try {
			uri = new URI("my://" + hostport);
		} catch (URISyntaxException e) {
			throw new RuntimeException("Could not identify hostname and port", e);
		}
		String host = uri.getHost();
		int port = uri.getPort();
		if (host == null || port == -1) {
			throw new RuntimeException("Could not identify hostname and port");
		}
		return new InetSocketAddress(host, port);
	}

	public JobExecutionResult executePlanWithJars(PlanWithJars p) throws Exception {
		return this.client.run(p, true);
	}
	
	@Override
	public JobExecutionResult executePlan(Plan plan) throws Exception {
		PlanWithJars p = new PlanWithJars(plan, this.jarFiles);
		return this.client.run(p, true);
	}
}
