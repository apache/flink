package develop;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;

import java.io.File;
import java.io.IOException;

/**
 * Created by sebastian on 10/7/14.
 */
public class MesosController {
	public static void main(String[] args) throws IOException {

		String uri = new File("./exec").getCanonicalPath();


		Protos.ExecutorInfo executor = Protos.ExecutorInfo.newBuilder()
				.setExecutorId(Protos.ExecutorID.newBuilder().setValue("default"))
				.setCommand(Protos.CommandInfo.newBuilder().setValue(uri))
				//.setCommand(Protos.CommandInfo.newBuilder().)
				.setName("Flink Executor (Java)")
				.build();
		System.out.println(new File(".").getAbsolutePath());
		Protos.FrameworkInfo framework = Protos.FrameworkInfo.newBuilder()
				.setUser("") // Have Mesos fill in the current user.
				.setName("Flink Test")
				.setPrincipal("Flink")
				.build();

		MesosSchedulerDriver driver = new MesosSchedulerDriver(
				new FlinkMesosSched(executor),
				framework,
				args[0]);

		Protos.Status result = driver.run();

		System.exit(result.getNumber());
	}
}
