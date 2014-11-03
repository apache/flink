package develop;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.ExecutionMode;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.InetAddress;
import java.net.Socket;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.FileHandler;

/**
 * Created by sebastian on 10/7/14.
 */
public class FlinkMesosEx implements Executor {

	@Override
	public void registered(ExecutorDriver executorDriver, Protos.ExecutorInfo executorInfo, Protos.FrameworkInfo frameworkInfo, Protos.SlaveInfo slaveInfo) {
		System.out.println("Registered Executor");
	}

	@Override
	public void reregistered(ExecutorDriver executorDriver, Protos.SlaveInfo slaveInfo) {

	}

	@Override
	public void disconnected(ExecutorDriver executorDriver) {

	}

	public File writeConfig() {
		Writer output = null;
		File result = null;
		try {
			output = new BufferedWriter(new FileWriter("./flink-conf-modified.yaml"));
			// just to make sure.
			output.append(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY + ": localhost\n");
			output.append(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY + ": 6123\n"); // already offsetted here.

			output.append(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY + ": 8090\n");
			output.flush();
			output.close();
			result = new File("./flink-conf-modified.yaml");
		} catch (IOException e) {
			e.printStackTrace();

		}
		return result;
	}

	@Override
	public void launchTask(final ExecutorDriver executorDriver, final Protos.TaskInfo taskInfo) {
		System.out.println(taskInfo.getData().toStringUtf8());
		setStatus(executorDriver, taskInfo, Protos.TaskState.TASK_RUNNING);

		new Thread() {
			@Override
			public void run() {
				JobManager jobManager = null;
				try {
					System.out.println("JobManager about to start");
					File config = writeConfig();
					String[] args = {"-executionMode","local", "-configDir", config.getCanonicalPath()};
					GlobalConfiguration.loadConfiguration(config.getCanonicalPath());
					jobManager = new JobManager(ExecutionMode.LOCAL);
					jobManager.initialize(args);

					jobManager.startInfoServer();
					jobManager.shutdown();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}.run();

		setStatus(executorDriver, taskInfo, Protos.TaskState.TASK_FINISHED);
	}

	@Override
	public void killTask(ExecutorDriver executorDriver, Protos.TaskID taskID) {

	}

	@Override
	public void frameworkMessage(ExecutorDriver executorDriver, byte[] bytes) {

	}

	@Override
	public void shutdown(ExecutorDriver executorDriver) {

	}

	@Override
	public void error(ExecutorDriver executorDriver, String s) {

	}

	private void setStatus(ExecutorDriver ex, Protos.TaskInfo taskInfo, Protos.TaskState newState) {
		Protos.TaskStatus status = Protos.TaskStatus.newBuilder()
				.setTaskId(taskInfo.getTaskId())
				.setState(newState).build();
		ex.sendStatusUpdate(status);
	}

	public static void main(String[] args) throws Exception {
		MesosExecutorDriver driver = new MesosExecutorDriver(new FlinkMesosEx());
		System.exit(driver.run() == Protos.Status.DRIVER_STOPPED ? 0 : 1);
	}
}
