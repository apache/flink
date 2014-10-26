package develop;

import com.google.protobuf.ByteString;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;

import java.io.File;
import java.io.IOException;

public class MesosController {
	public static void main(String[] args) throws IOException {
		Protos.FrameworkInfo framework = Protos.FrameworkInfo.newBuilder()
				.setUser("") // Have Mesos fill in the current user.
				.setName("Flink Test")
				.setPrincipal("Flink")
				.build();

		MesosSchedulerDriver driver = new MesosSchedulerDriver(
				new FlinkMesosSched(),
				framework,
				args[0]);

		Protos.Status result = driver.run();

		System.exit(result.getNumber());
	}
}
