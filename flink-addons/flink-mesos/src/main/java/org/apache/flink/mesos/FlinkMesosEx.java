package org.apache.flink.mesos;

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

public class FlinkMesosEx implements Executor {

	String FLINK_CONF_DIR = null;

	public FlinkMesosEx(String FLINK_CONF_DIR) {
		this.FLINK_CONF_DIR = FLINK_CONF_DIR;
	}

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
		System.out.println(taskInfo.getData().toStringUtf8());
		setStatus(executorDriver, taskInfo, Protos.TaskState.TASK_RUNNING);

		new Thread() {
			@Override
			public void run() {
				JobManager jobManager;
				try {
					String[] args = {"-configDir", FLINK_CONF_DIR};
					jobManager = JobManager.initialize(args);
					// Start info server for jobmanager
					jobManager.startInfoServer();
				}
				catch (Exception e) {
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
		MesosExecutorDriver driver = new MesosExecutorDriver(new FlinkMesosEx(args[0]));
		System.exit(driver.run() == Protos.Status.DRIVER_STOPPED ? 0 : 1);
	}
}
