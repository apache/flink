package org.apache.flink.state.api;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.Operator;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.state.api.functions.BroadcastStateBootstrapFunction;
import org.apache.flink.state.api.functions.StateBootstrapFunction;
import org.apache.flink.state.api.output.TaggedOperatorSubtaskState;
import org.apache.flink.state.api.runtime.metadata.NewSavepointMetadata;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for bootstrap transformations.
 */
public class BootstrapTransformationTest extends AbstractTestBase {

	@Test
	public void testBroadcastStateTransformationParallelism() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(10);

		DataSet<Integer> input = env.fromElements(0);

		BootstrapTransformation<Integer> transformation = OperatorTransformation
			.bootstrapWith(input)
			.transform(new ExampleBroadcastStateBootstrapFunction());

		DataSet<TaggedOperatorSubtaskState> result = transformation.getOperatorSubtaskStates(
			"uid",
			new MemoryStateBackend(),
			new NewSavepointMetadata(4),
			new Path()
		);

		Assert.assertEquals("Broadcast transformations should always be run at parallelism 1",
			1,
			getParallelism(result));
	}

	@Test
	public void testDefaultParallelismRespectedWhenLessThanMaxParallelism() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(4);

		DataSource<Integer> input = env.fromElements(0);

		BootstrapTransformation<Integer> transformation = OperatorTransformation
			.bootstrapWith(input)
			.transform(new ExampleStateBootstrapFunction());

		DataSet<TaggedOperatorSubtaskState> result = transformation.getOperatorSubtaskStates(
			"uid",
			new MemoryStateBackend(),
			new NewSavepointMetadata(10),
			new Path()
		);

		Assert.assertEquals(
			"The parallelism of a data set should not change when less than the max parallelism of the savepoint",
			ExecutionConfig.PARALLELISM_DEFAULT,
			getParallelism(result));
	}

	@Test
	public void testMaxParallelismRespected() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(10);

		DataSource<Integer> input = env.fromElements(0);

		BootstrapTransformation<Integer> transformation = OperatorTransformation
			.bootstrapWith(input)
			.transform(new ExampleStateBootstrapFunction());

		DataSet<TaggedOperatorSubtaskState> result = transformation.getOperatorSubtaskStates(
			"uid",
			new MemoryStateBackend(),
			new NewSavepointMetadata(4),
			new Path()
		);

		Assert.assertEquals(
			"The parallelism of a data set should be constrained my the savepoint max parallelism",
			4,
			getParallelism(result));
	}

	@Test
	public void testOperatorSpecificMaxParallelismRespected() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(4);

		DataSource<Integer> input = env.fromElements(0);

		BootstrapTransformation<Integer> transformation = OperatorTransformation
			.bootstrapWith(input)
			.setMaxParallelism(1)
			.transform(new ExampleStateBootstrapFunction());

		DataSet<TaggedOperatorSubtaskState> result = transformation.getOperatorSubtaskStates(
			"uid",
			new MemoryStateBackend(),
			new NewSavepointMetadata(4),
			new Path()
		);

		Assert.assertEquals("The parallelism of a data set should be constrained my the savepoint max parallelism", 1, getParallelism(result));
	}

	private static <T> int getParallelism(DataSet<T> dataSet) {
		//All concrete implementations of DataSet are operators so this should always be safe.
		return ((Operator) dataSet).getParallelism();
	}

	private static class ExampleBroadcastStateBootstrapFunction extends BroadcastStateBootstrapFunction<Integer> {

		@Override
		public void processElement(Integer value, Context ctx) throws Exception {
		}
	}

	private static class ExampleStateBootstrapFunction extends StateBootstrapFunction<Integer> {

		@Override
		public void processElement(Integer value, Context ctx) throws Exception {
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
		}
	}
}
