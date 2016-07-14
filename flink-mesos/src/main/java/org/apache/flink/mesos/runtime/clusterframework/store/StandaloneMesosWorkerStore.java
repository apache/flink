package org.apache.flink.mesos.runtime.clusterframework.store;

import com.google.common.collect.ImmutableList;
import org.apache.mesos.Protos;
import scala.Option;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A standalone Mesos worker store.
 */
public class StandaloneMesosWorkerStore implements MesosWorkerStore {

	private Option<Protos.FrameworkID> frameworkID = Option.empty();

	private int taskCount = 0;

	private Map<Protos.TaskID, Worker> storedWorkers = new LinkedHashMap<>();

	public StandaloneMesosWorkerStore() {
	}

	@Override
	public void start() throws Exception {

	}

	@Override
	public void stop() throws Exception {

	}

	@Override
	public Option<Protos.FrameworkID> getFrameworkID() throws Exception {
		return frameworkID;
	}

	@Override
	public void setFrameworkID(Option<Protos.FrameworkID> frameworkID) throws Exception {
		this.frameworkID = frameworkID;
	}

	@Override
	public List<Worker> recoverWorkers() throws Exception {
		return ImmutableList.copyOf(storedWorkers.values());
	}

	@Override
	public Protos.TaskID newTaskID() throws Exception {
		Protos.TaskID taskID = Protos.TaskID.newBuilder().setValue(TASKID_FORMAT.format(++taskCount)).build();
		return taskID;
	}

	@Override
	public void putWorker(Worker worker) throws Exception {
		storedWorkers.put(worker.taskID(), worker);
	}

	@Override
	public void removeWorker(Protos.TaskID taskID) throws Exception {
		storedWorkers.remove(taskID);
	}

	@Override
	public void cleanup() throws Exception {
	}
}
