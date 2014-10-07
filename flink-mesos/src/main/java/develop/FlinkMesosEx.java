package develop;

import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;

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

	@Override
	public void launchTask(final ExecutorDriver executorDriver, final Protos.TaskInfo taskInfo) {
		new Thread() { public void run() {
			try {
				Protos.TaskStatus status = Protos.TaskStatus.newBuilder()
						.setTaskId(taskInfo.getTaskId())
						.setState(Protos.TaskState.TASK_RUNNING).build();

				executorDriver.sendStatusUpdate(status);

				System.out.println("Running task " + taskInfo.getTaskId());

				// This is where one would perform the requested task.

				status = Protos.TaskStatus.newBuilder()
						.setTaskId(taskInfo.getTaskId())
						.setState(Protos.TaskState.TASK_FINISHED).build();

				executorDriver.sendStatusUpdate(status);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}}.start();
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

	public static void main(String[] args) throws Exception {
		MesosExecutorDriver driver = new MesosExecutorDriver(new FlinkMesosEx());
		System.exit(driver.run() == Protos.Status.DRIVER_STOPPED ? 0 : 1);
	}
}
