package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskRunner;
import org.apache.hadoop.mapred.TaskTracker;
import org.apache.hadoop.mapred.TaskTracker.RunningJob;
import org.apache.hadoop.mapred.TaskTracker.TaskInProgress;
import org.apache.hadoop.mapred.TaskUmbilicalProtocol;

public class ReaderTask extends Task {

	@Override
	public void run(JobConf job, TaskUmbilicalProtocol umbilical)
			throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public TaskRunner createRunner(TaskTracker tracker, TaskInProgress tip,
			RunningJob rjob) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isMapTask() {
		// TODO Auto-generated method stub
		return false;
	}

	

}
