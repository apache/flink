package org.apache.flink.streaming.examples.bloomfilter;

import org.apache.flink.api.common.state.PartitionedBloomFilterDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.operators.ElasticBloomFilter;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * Created by zsh on 11/02/2018.
 */
public class BloomFilterExample {

	private static class SimpleSource implements SourceFunction<Integer> {
		private static final long serialVersionUID = 1L;

		private volatile boolean isRunning = true;

		private long ts = -1L;

		public SimpleSource() {
		}

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			if (ts == -1L) {
				ts = System.currentTimeMillis();
			}
			while (isRunning) {
				int num = new Random().nextInt(1000);
				ctx.collect(num);
				Thread.sleep(1000L);

				if (System.currentTimeMillis() - ts >= 30000) {
					throw new RuntimeException("trigger failover.");
				}
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}

	public static void main(String[] args) throws Exception {

		// obtain execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setStateBackend(new MemoryStateBackend());
		env.enableCheckpointing(15000L, CheckpointingMode.EXACTLY_ONCE);
		env.setParallelism(1);
		env.setMaxParallelism(8);

		// create input stream of an single integer
		env.addSource(new SimpleSource())
			.keyBy(new KeySelector<Integer, Integer>() {
				@Override
				public Integer getKey(Integer value) throws Exception {
					return value;
				}
			})
			.timeWindow(Time.milliseconds(1000))
			.apply(new RichWindowFunction<Integer, Integer, Integer, TimeWindow>() {
				private ElasticBloomFilter bf1;
				private ElasticBloomFilter bf2;

				@Override
				public void open(Configuration parameters) throws Exception {
					super.open(parameters);
					StreamingRuntimeContext runtimeContext = (StreamingRuntimeContext) this.getRuntimeContext();
					bf1 = runtimeContext.getPartitionedBloomFilter(new PartitionedBloomFilterDescriptor(
						"0.01",
						TypeInformation.of(String.class),
						60000,
						0.01));

					bf2 = runtimeContext.getPartitionedBloomFilter(new PartitionedBloomFilterDescriptor(
						"0.02",
						TypeInformation.of(String.class),
						60000,
						0.01));
				}

				@Override
				public void apply(Integer integer, TimeWindow window, Iterable<Integer> input, Collector<Integer> out) throws Exception {
					for (Integer ele : input) {
						if (!bf1.contains(String.valueOf(ele))) {
							System.out.println("write:" + ele);
							bf2.add(String.valueOf(ele));
							out.collect(ele);
						}

						if (!bf2.contains(String.valueOf(ele) + "a")) {
							System.out.println("write:" + ele);
							bf2.add(String.valueOf(ele) + "a");
							out.collect(ele);
						}
					}
				}
			}).print();

		// execute the program
		env.execute("BloomFilter Example");
	}
}
