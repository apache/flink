/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.hadoopcompatibility.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.ReduceGroupOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.types.TypeInformation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobQueueInfo;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;

/**
 * The user's view of a Hadoop Job executed on a Flink cluster.
 */
public class FlinkHadoopJobClient extends JobClient {

	private final static int TASK_SLOTS = GlobalConfiguration.getInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, -1);
	private static final Log LOG = LogFactory.getLog(FlinkHadoopJobClient.class);


	private ExecutionEnvironment environment;
	private Configuration hadoopConf;

	public FlinkHadoopJobClient() throws IOException {
		this(new Configuration());
	}

	public FlinkHadoopJobClient(JobConf jobConf) throws IOException {
		this(new Configuration(jobConf));
	}

	public FlinkHadoopJobClient(Configuration hadoopConf) throws IOException{
		this(hadoopConf, (ExecutionEnvironment.getExecutionEnvironment()));
	}

	public FlinkHadoopJobClient(Configuration hadoopConf, ExecutionEnvironment environment) throws IOException {
		this.hadoopConf = hadoopConf;
		this.environment = environment;
	}

	public void init(JobConf conf) throws IOException {
		this.hadoopConf = conf;
	}

	/**
	 * Submits a Hadoop job to Flink (as described by the JobConf) and returns after the job has been completed.
	 */
	public static RunningJob runJob(JobConf hadoopJobConf) throws IOException{
		final FlinkHadoopJobClient jobClient = new FlinkHadoopJobClient(hadoopJobConf);
		final RunningJob job = jobClient.submitJob(hadoopJobConf);
		job.waitForCompletion();
		return job;
	}

	/**
	 * Submits a job to Flink and returns a RunningJob instance which can be scheduled and monitored
	 * without blocking by default. Use waitForCompletion() to block until the job is finished.
	 */
	@Override
	@SuppressWarnings("unchecked")
	public RunningJob submitJob(JobConf hadoopJobConf) throws IOException{

		final int mapParallelism = getMapParallelism(hadoopJobConf);
		final int reduceParallelism = getReduceParallelism(hadoopJobConf);

		//setting up the inputFormat for the job
		final DataSource<?> input = environment.createInput(getFlinkInputFormat(hadoopJobConf));
		input.setParallelism(mapParallelism);

		final FlatMapOperator mapped = input.flatMap(new HadoopMapFunction(hadoopJobConf));
		mapped.setParallelism(getMapParallelism(hadoopJobConf));

		//Partitioning
		final Class partitionerClass = hadoopJobConf.getPartitionerClass();
		if (! partitionerClass.equals(HashPartitioner.class)) {
			throw new UnsupportedOperationException("Custom partitioners are not supported yet.");
		}
		final UnsortedGrouping<?> grouping = mapped.groupBy(0);

		final ReduceGroupOperator reduceOp = grouping.reduceGroup(new HadoopReduceFunction(hadoopJobConf));
		final Class<? extends Reducer> combinerClass = hadoopJobConf.getCombinerClass();
		if (combinerClass != null) {
			reduceOp.setCombinable(true);
		}
		reduceOp.setParallelism(reduceParallelism);

		//Wrapping the output format.
		final OutputFormat<?> outputFormat = new HadoopOutputFormat(hadoopJobConf.getOutputFormat() ,hadoopJobConf);
		reduceOp.output(outputFormat).setParallelism(reduceParallelism);

		return new DummyFlinkRunningJob(hadoopJobConf.getJobName());
	}

	@SuppressWarnings("unchecked")
	private HadoopInputFormat<?,?> getFlinkInputFormat(JobConf jobConf) throws IOException{
		final InputFormat inputFormat = jobConf.getInputFormat();
		final Class inputFormatClass = inputFormat.getClass();

		final TypeInformation keyTypeInfo = TypeExtractor.createTypeInfo(InputFormat.class, inputFormatClass,
				0, null, null);
		final Class keyClass = keyTypeInfo.getTypeClass();

		final TypeInformation valueTypeInfo = TypeExtractor.createTypeInfo(InputFormat.class, inputFormatClass,
				1, null, null);
		final Class valueClass = valueTypeInfo.getTypeClass();

		return new HadoopInputFormat(inputFormat, keyClass, valueClass, jobConf);
	}

	/**
	 * The number of map tasks that can be run in parallel is the number of inputSplits.
	 * The usage of JobConf.setNumMapTasks() is deprecated.
	 * The upper bound for the number of parallel map tasks is the number of task slots.
	 */
	private int getMapParallelism(JobConf conf) throws IOException{
		final int mapTasks = conf.getInputFormat().getSplits(conf, 0).length;
		return mapTasks > TASK_SLOTS ? TASK_SLOTS : mapTasks;
	}

	/**
	 * The number of reduce tasks that can be run in parallel is set by JobConf.setNumReduceTasks().
	 * The upper bound for the number of parallel reduce tasks is the number of task slots.
	 */
	private int getReduceParallelism(JobConf conf) {
		final int reduceTasks = conf.getNumReduceTasks();
		if (reduceTasks < TASK_SLOTS) {
			return reduceTasks;
		}
		else {
			LOG.warn("The number of reduce tasks (" + reduceTasks + ") exceeds the number of available Flink slots ("
					+ TASK_SLOTS + "). " + TASK_SLOTS + "tasks will be run.");
			return TASK_SLOTS;
		}
	}
	
	@Override
	public void setConf(Configuration conf) {
		this.hadoopConf = conf;
	}
	
	@Override
	public Configuration getConf() {
		return this.hadoopConf;
	}

	private class DummyFlinkRunningJob implements RunningJob {

		private final String jobName;

		public DummyFlinkRunningJob( String jobName) {
			this.jobName = jobName;
		}

		@Override
		public JobID getID() {
			throw new UnsupportedOperationException();
		}

		@Override
		public String getJobID() {
			throw new UnsupportedOperationException();
		}

		@Override
		public String getJobName() {
			throw new UnsupportedOperationException();
		}

		@Override
		public String getJobFile() {
			throw new UnsupportedOperationException();
		}

		@Override
		public String getTrackingURL() {
			throw new UnsupportedOperationException();
		}

		@Override
		public float mapProgress() throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public float reduceProgress() throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public float cleanupProgress() throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public float setupProgress() throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean isComplete() throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean isSuccessful() throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public void waitForCompletion() throws IOException {
			try {
				environment.execute(jobName);
			}
			catch (Exception e) {
				throw new IOException("An error has occurred.", e);
			}
		}

		@Override
		public int getJobState() throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public JobStatus getJobStatus() throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public void killJob() throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public void setJobPriority(final String s) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public TaskCompletionEvent[] getTaskCompletionEvents(final int i) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public void killTask(final TaskAttemptID taskAttemptID, final boolean b) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public void killTask(final String s, final boolean b) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public Counters getCounters() throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public String getFailureInfo() throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public String[] getTaskDiagnostics(final TaskAttemptID taskAttemptID) throws IOException {
			throw new UnsupportedOperationException();
		}

		//Hadoop 2.2 methods.
		public boolean isRetired() throws IOException { throw new UnsupportedOperationException(); }

		public String getHistoryUrl() throws IOException {throw new UnsupportedOperationException(); }

		public Configuration getConfiguration() { return getConf(); }
	}

	public void setEnvironment(ExecutionEnvironment environment) {
		this.environment = environment;
	}


	@Override
	public synchronized void close() throws IOException { throw new UnsupportedOperationException(); }

	@Override
	public synchronized FileSystem getFs() throws IOException { throw new UnsupportedOperationException(); }

	@Override
	public RunningJob submitJob(String jobFile) throws IOException { throw new UnsupportedOperationException(); }

	@Override
	public RunningJob submitJobInternal(JobConf job) throws IOException { throw new UnsupportedOperationException(); }

	@Override
	public RunningJob getJob(JobID jobid) throws IOException { throw new UnsupportedOperationException(); }

	@Override
	public RunningJob getJob(String jobid) throws IOException { throw new UnsupportedOperationException(); }

	@Override
	public TaskReport[] getMapTaskReports(JobID jobId) throws IOException { throw new UnsupportedOperationException(); }

	@Override
	public TaskReport[] getMapTaskReports(String jobId) throws IOException { throw new UnsupportedOperationException(); }

	@Override
	public TaskReport[] getReduceTaskReports(JobID jobId) throws IOException { throw new UnsupportedOperationException(); }

	@Override
	public TaskReport[] getCleanupTaskReports(JobID jobId) throws IOException { throw new UnsupportedOperationException(); }

	@Override
	public TaskReport[] getSetupTaskReports(JobID jobId) throws IOException { throw new UnsupportedOperationException(); }

	@Override
	public TaskReport[] getReduceTaskReports(String jobId) throws IOException { throw new UnsupportedOperationException(); }

	@Override
	public void displayTasks(JobID jobId, String type, String state) throws IOException { throw new UnsupportedOperationException(); }

	@Override
	public ClusterStatus getClusterStatus() throws IOException { throw new UnsupportedOperationException(); }

	@Override
	public ClusterStatus getClusterStatus(boolean detailed) throws IOException { throw new UnsupportedOperationException(); }

	@Override
	public org.apache.hadoop.fs.Path getStagingAreaDir() throws IOException { throw new UnsupportedOperationException(); }

	public JobStatus[] jobsToComplete() throws IOException { throw new UnsupportedOperationException(); }

	@Override
	public JobStatus[] getAllJobs() throws IOException { throw new UnsupportedOperationException(); }

	@Override
	public boolean monitorAndPrintJob(JobConf conf, RunningJob job) throws IOException, InterruptedException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setTaskOutputFilter(JobClient.TaskStatusFilter newValue) { throw new UnsupportedOperationException(); }

	@Override
	public JobClient.TaskStatusFilter getTaskOutputFilter() { throw new UnsupportedOperationException(); }

	@Override
	public int run(java.lang.String[] argv) throws Exception { throw new UnsupportedOperationException(); }

	@Override
	public int getDefaultMaps() throws IOException { throw new UnsupportedOperationException(); }

	@Override
	public int getDefaultReduces() throws IOException { throw new UnsupportedOperationException(); }

	@Override
	public Path getSystemDir() { throw new UnsupportedOperationException(); }

	@Override
	public JobQueueInfo[] getQueues() throws IOException { throw new UnsupportedOperationException(); }

	@Override
	public JobStatus[] getJobsFromQueue(String queueName) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public JobQueueInfo getQueueInfo(String queueName) throws IOException { throw new UnsupportedOperationException(); }

	@Override
	public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer) throws IOException, InterruptedException {
		throw new UnsupportedOperationException();
	}

	@Override
	public long renewDelegationToken(Token<DelegationTokenIdentifier> token) throws IOException, InterruptedException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void cancelDelegationToken(Token<DelegationTokenIdentifier> token) throws IOException, InterruptedException {
		throw new UnsupportedOperationException();
	}
}
