package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/** ApproximateFailover Manual Test.*/
public class ApproximateFailover {
	public static void main(String[] args) throws Exception {
		final int failAfterElements = 150;

		// Checking input parameters
		final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		// get input data
		System.out.println("Executing Failover Manual Test.");

		env
			.setParallelism(1)
			.setBufferTimeout(0)
			.setMaxParallelism(128)
			.disableOperatorChaining()
			.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
		env.getCheckpointConfig().enableApproximateLocalRecovery(true);

		DataStream<InvertedKeyTuple>  event = env.addSource(new AppSourceFunction())
			.slotSharingGroup("source")
			.map(new FailingMapper<>(failAfterElements))
			.slotSharingGroup("map");

		FailingMapper.failedBefore = false;

		// emit result
		System.out.println("Printing result to stdout. Use --output to specify output path.");
		event.print();

		// execute program
		env.execute("Streaming WordCount");
	}

	private static class AppSourceFunction extends RichParallelSourceFunction<InvertedKeyTuple> {
		private final String shortString = "I am a very long string to test partial records hohoho hahaha ";
		private final String longOrShortString;
		private final int maxParallelism;
		private final int numberOfChannels;
		private final int[] keys;
		private int index = 0;
		private volatile boolean running = true;

		// short-length string
		AppSourceFunction() {
			this.longOrShortString = shortString;
			this.maxParallelism = 128;
			this.numberOfChannels = 1;
			this.keys = initKeys(numberOfChannels);
		}

		@Override
		public void run(SourceContext<InvertedKeyTuple> ctx) throws Exception{
			while (running) {
				synchronized (ctx.getCheckpointLock()) {
					if (index % 100 == 0) {
						Thread.sleep(50);
					}
					int selectedChannel = index % numberOfChannels;
					int key = keys[selectedChannel];
					ctx.collect(new InvertedKeyTuple(index, key, selectedChannel, longOrShortString));
				}
				index++;
			}
		}

		@Override
		public void cancel() {
			running = false;
		}

		// Find the first key which falls into the key group of each downstream channel
		private int[] initKeys(int numberOfChannels) {
			int[] keys = new int[numberOfChannels];

			for (int i = 0; i < numberOfChannels; i++) {
				int key = 0;
				while (key < 1000 && selectedChannel(key) != i) {
					key++;
				}
				assert key < 1000 : "Can not find a key within number 1000";
				keys[i] = key;
			}

			return keys;
		}

		private int selectedChannel(int key) {
			return KeyGroupRangeAssignment.assignKeyToParallelOperator(key, maxParallelism, numberOfChannels);
		}
	}

	/** Fails once the first map instance reaches failCount. */
	private static class FailingMapper<T> extends RichMapFunction<T, T> {
		private static final long serialVersionUID = 6334389850158703L;

		private static volatile boolean failedBefore;

		private final int failCount;
		private int numElementsTotal;

		private boolean failer;

		FailingMapper(int failCount) {
			this.failCount = failCount;
		}

		@Override
		public void open(Configuration parameters) {
			failer = getRuntimeContext().getIndexOfThisSubtask() == 0;
		}

		@Override
		public T map(T value) throws Exception {
			numElementsTotal++;

			if (!failedBefore) {
				Thread.sleep(10);

				if (failer && numElementsTotal >= failCount) {
					failedBefore = true;
					throw new Exception("Artificial Test Failure");
				}
			}

			return value;
		}
	}

	private static class InvertedKeyTuple {
		int index;
		/** Key based on which the tuple is partitioned. */
		int key;
		/** The selected channel of this tuple after key-partitioned. */
		int selectedChannel;
		String longOrShortString;

		InvertedKeyTuple(int index, int key, int selectedChannel, String longOrShortString) {
			this.index = index;
			this.key = key;
			this.selectedChannel = selectedChannel;
			this.longOrShortString = longOrShortString;
		}

		@Override
		public String toString() {
			return String.format("index:%d; key:%d; selectedChannel:%d; longOrShortString:%s",
				index, key, selectedChannel, longOrShortString);
		}
	}
}
