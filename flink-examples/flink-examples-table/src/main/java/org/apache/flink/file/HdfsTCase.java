package org.apache.flink.file;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.fs.table.descriptors.Bucket;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Schema;

public class HdfsTCase {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(5000);
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
		DataStream<Tuple3<String, String, Long>> dataStream = env.addSource(new MySource()).returns(org.apache.flink.api.common.typeinfo.Types.TUPLE(org.apache.flink.api.common.typeinfo.Types.STRING, org.apache.flink.api.common.typeinfo.Types.STRING, org.apache.flink.api.common.typeinfo.Types.LONG));
		tEnv.registerDataStream("mysource", dataStream, "appName,clientIp,uploadTime");

		tEnv.connect(new Bucket().basePath("hdfs:///localhost/tmp/flink-data/json").rawFormat().dateFormat("yyyy-MM-dd-HHmm"))
			.withFormat(new Json().deriveSchema())
			.withSchema(new Schema()
				.field("a", Types.STRING())
				.field("b", Types.STRING())
				.field("c", Types.LONG()))
			.inAppendMode()
			.registerTableSink("mysink");

		tEnv.sqlUpdate("insert into mysink SELECT * FROM mysource");
		env.execute();
	}



	public static class MySource implements SourceFunction<Tuple3<String, String, Long>> {
		private volatile boolean isRunning = true;
		long n = 1;


		@Override
		public void run(SourceContext<Tuple3<String, String, Long>> ctx) throws Exception {
			while (isRunning) {
				Thread.sleep((int) (Math.random() * 100));
				Tuple3<String, String, Long> tuple3 = new Tuple3("appName" + n++, "clientIp" + n++, n++);
				ctx.collect(tuple3);
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}
}
