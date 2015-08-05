package org.apache.flink.streaming.api.functions;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.Accumulator;
//import org.apache.flink.api.common.functions.RuntimeContext;
//import org.apache.flink.api.common.functions.util.RuntimeUDFContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memorymanager.MemoryManager;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.runtime.tasks.StreamingRuntimeContext;

import static org.junit.Assert.*;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * Mock context that collects elements in a List.
 *
 * @param <T> Type of the collected elements.
 */
public class PrintSinkFunctionTest<IN> extends RichSinkFunction<IN> {

	private Environment envForPrefixNull = new Environment() {
		@Override
		public JobID getJobID() {
			return null;
		}

		@Override
		public JobVertexID getJobVertexId() {
			return null;
		}

		@Override
		public ExecutionAttemptID getExecutionId() {
			return null;
		}

		@Override
		public Configuration getTaskConfiguration() {
			return null;
		}

		@Override
		public Configuration getTaskManagerConfiguration() {
			return null;
		}

		@Override
		public String getHostname() {
			return null;
		}

		@Override
		public Configuration getJobConfiguration() {
			return null;
		}

		@Override
		public int getNumberOfSubtasks() {
			return 0;
		}

		@Override
		public int getIndexInSubtaskGroup() {
			return 0;
		}

		@Override
		public InputSplitProvider getInputSplitProvider() {
			return null;
		}

		@Override
		public IOManager getIOManager() {
			return null;
		}

		@Override
		public MemoryManager getMemoryManager() {
			return null;
		}

		@Override
		public String getTaskName() {
			return null;
		}

		@Override
		public String getTaskNameWithSubtasks() {
			return null;
		}

		@Override
		public ClassLoader getUserClassLoader() {
			return null;
		}

		@Override
		public Map<String, Future<Path>> getDistributedCacheEntries() {
			return null;
		}

		@Override
		public BroadcastVariableManager getBroadcastVariableManager() {
			return null;
		}

		@Override
		public AccumulatorRegistry getAccumulatorRegistry() {
			return null;
		}

		@Override
		public void acknowledgeCheckpoint(long checkpointId) {

		}

		@Override
		public void acknowledgeCheckpoint(long checkpointId, StateHandle<?> state) {

		}

		@Override
		public ResultPartitionWriter getWriter(int index) {
			return null;
		}

		@Override
		public ResultPartitionWriter[] getAllWriters() {
			return new ResultPartitionWriter[0];
		}

		@Override
		public InputGate getInputGate(int index) {
			return null;
		}

		@Override
		public InputGate[] getAllInputGates() {
			return new InputGate[0];
		}
	};

	@Test
	public void testPrintSinkStdOut(){

		ExecutionConfig executionConfig = new ExecutionConfig();
		final HashMap<String, Accumulator<?, ?>> accumulators = new HashMap<String, Accumulator<?, ?>>();
		final StreamingRuntimeContext ctx = new StreamingRuntimeContext(
				"Test Print Sink",
				this.envForPrefixNull,
				null,
				executionConfig,
				null,
				null,
				accumulators
		);

		PrintSinkFunction<String> printSink = new PrintSinkFunction<String>();
		printSink.setRuntimeContext(ctx);
		try {
			printSink.open(new Configuration());
		} catch (Exception e) {
			e.printStackTrace();
		}
		printSink.setTargetToStandardOut();
		printSink.invoke("hello world!");

		assertEquals("Print to System.out", printSink.toString());

		printSink.close();
	}

	@Test
	public void testPrintSinkStdErr(){
		ExecutionConfig executionConfig = new ExecutionConfig();
		final HashMap<String, Accumulator<?, ?>> accumulators = new HashMap<String, Accumulator<?, ?>>();
		final StreamingRuntimeContext ctx = new StreamingRuntimeContext(
				"Test Print Sink",
				this.envForPrefixNull,
				null,
				executionConfig,
				null,
				null,
				accumulators
		);

		PrintSinkFunction<String> printSink = new PrintSinkFunction<String>();
		printSink.setRuntimeContext(ctx);
		try {
			printSink.open(new Configuration());
		} catch (Exception e) {
			e.printStackTrace();
		}
		printSink.setTargetToStandardErr();
		printSink.invoke("hello world!");

		assertEquals("Print to System.err", printSink.toString());

		printSink.close();
	}

	@Override
	public void invoke(IN record) {

	}
}