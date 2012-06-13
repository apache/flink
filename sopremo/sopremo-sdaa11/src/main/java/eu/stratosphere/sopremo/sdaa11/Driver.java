package eu.stratosphere.sopremo.sdaa11;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.client.nephele.api.Client;
import eu.stratosphere.pact.client.nephele.api.ErrorInPlanAssemblerException;
import eu.stratosphere.pact.client.nephele.api.PactProgram;
import eu.stratosphere.pact.client.nephele.api.ProgramInvocationException;

public class Driver {

	public static void main(final String[] args)
			throws ProgramInvocationException, ErrorInPlanAssemblerException,
			IOException, URISyntaxException {
		System.out.println("Sending initial plan");
		final long startTime = System.currentTimeMillis();
		final PactProgram program = new PactProgram(new File(
				"sopremo-sdaa11-0.0.1-SNAPSHOT.jar"),
				Sdaa11PlanAssembler.class.getCanonicalName(), args);
		final Client client = new Client(new Configuration());
		client.run(program, true);
		System.out.println("Done");
		final long elapsedTime = System.currentTimeMillis() - startTime;
		System.out.format(
				"External job time: %1$tH:%1$tM:%1$tS:%1$tL (%1$d)\n",
				elapsedTime);
	}

}
