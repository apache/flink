package org.apache.flink.mongodb.streaming.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.graph.StreamGraph;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Tests for MongoSink insert mode.
 **/
public class MongoInsertSinkTest extends MongoSinkTestBase {

    @Test
    public void testWrite() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getCheckpointConfig().setCheckpointInterval(1000L);

        // if these rows are not multiple times of rps, there would be the records remaining not flushed
        // after the last checkpoint
        long rps = 50;
        long rows = 1000L;

        MongoSink<String> mongoSink = MongoSink.BuilderClient("admin", "SM67q89izW4itH7%", CONNECT_STRING, new StringDocumentSerializer())
                .isTransactional(true)
                .isFlushOnCheckpoint(true)
                .setDatabase(DATABASE_NAME)
                .setCollection(COLLECTION)
                .isRetryWrites(true).build();

        env.addSource(new DataGeneratorSource<>(new StringGenerator(), rps, rows));

        env.addSource(new DataGeneratorSource<>(new StringGenerator(), rps, rows))
                .returns(String.class)
                .sinkTo(mongoSink);

        env.addSource(new DataGeneratorSource<>(new StringGenerator(), rps, rows))
                .returns(String.class)
                .sinkTo(mongoSink);
        StreamGraph streamGraph = env.getStreamGraph();

        final Configuration config = new Configuration();
        config.setString(RestOptions.BIND_PORT, "18081-19000");
        final MiniClusterConfiguration cfg =
                new MiniClusterConfiguration.Builder()
                        .setNumTaskManagers(1)
                        .setNumSlotsPerTaskManager(4)
                        .setConfiguration(config)
                        .build();
        try (MiniCluster miniCluster = new MiniCluster(cfg)) {
            miniCluster.start();
            miniCluster.executeJobBlocking(streamGraph.getJobGraph());
        }

        assertTrue(mongo.getDatabase(DATABASE_NAME).getCollection(COLLECTION).countDocuments() > 0);
    }
}
