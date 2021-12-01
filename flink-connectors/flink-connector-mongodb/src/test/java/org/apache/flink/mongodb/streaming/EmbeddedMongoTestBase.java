package org.apache.flink.mongodb.streaming;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfig;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import org.junit.After;
import org.junit.Before;

/** Base class for tests based on embedded MongoDB. */
public class EmbeddedMongoTestBase {

    protected MongodExecutable mongodExe;
    protected MongodProcess mongod;
    protected MongoClient mongo;

    // these can be overridden by subclasses
    protected static String host = "127.0.0.1";
    protected static int port = 27018;
    protected static String databaseName = "testdb";
    protected static String collection = "testcoll";
    protected static String connectString =
            String.format("mongodb://%s:%d/%s", host, port, databaseName);

    @Before
    public void before() throws Exception {
        MongodStarter starter = MongodStarter.getDefaultInstance();
        MongodConfig mongodConfig =
                MongodConfig.builder()
                        .version(Version.Main.V4_0)
                        .net(new Net(host, port, Network.localhostIsIPv6()))
                        .build();
        this.mongodExe = starter.prepare(mongodConfig);
        this.mongod = mongodExe.start();
        this.mongo = MongoClients.create(connectString);
    }

    @After
    public void after() throws Exception {
        if (this.mongo != null) {
            mongo.getDatabase(databaseName).getCollection(collection).drop();
            mongo.close();
        }
        if (this.mongod != null) {
            this.mongod.stop();
            this.mongodExe.stop();
        }
    }
}
