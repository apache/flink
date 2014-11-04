package org.apache.flink.mesos;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;

import java.io.IOException;

public class MesosController {
	public static void main(String[] args) throws IOException {
		System.out.println("Starting MesosController");
		Protos.FrameworkInfo framework = Protos.FrameworkInfo.newBuilder()
				.setUser("") // Have Mesos fill in the current user.
				.setName("Flink Test")
				.setPrincipal("Flink")
				.build();

		System.out.println("Creating Driver");
		System.out.println("Parameters: " + args[0] + " " + args[1]);
		MesosSchedulerDriver driver = new MesosSchedulerDriver(
				new FlinkMesosSched(args[1], args[2]),
				framework,
				args[0]);

		Protos.Status result = driver.run();
		System.exit(result.getNumber());
	}
}
