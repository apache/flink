/*
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

package org.apache.flink.tests.util.mongodb;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.connector.testframe.container.FlinkContainers;
import org.apache.flink.connector.testframe.container.FlinkContainersSettings;
import org.apache.flink.connector.testframe.container.TestcontainersSettings;
import org.apache.flink.test.resources.ResourceTestUtils;
import org.apache.flink.test.util.SQLJobSubmission;
import org.apache.flink.util.DockerImageVersions;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.TestLoggerExtension;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

/** End-to-end test for the MongoDB connectors. */
@Testcontainers
@ExtendWith({TestLoggerExtension.class})
public class MongoTableApiE2ECase extends TestLogger {

    private static final Logger LOG = LoggerFactory.getLogger(MongoTableApiE2ECase.class);

    private static final String MONGODB_HOSTNAME = "mongodb";

    private static final Network NETWORK = Network.newNetwork();

    private final Path sqlConnectorMongoDBJar = ResourceTestUtils.getResource(".*mongodb.jar");

    @Container
    public static final MongoDBContainer MONGO_CONTAINER =
            new MongoDBContainer(DockerImageVersions.MONGO_4_0)
                    .withLogConsumer(new Slf4jLogConsumer(LOG))
                    .withNetwork(NETWORK)
                    .withNetworkAliases(MONGODB_HOSTNAME);

    public static final TestcontainersSettings TESTCONTAINERS_SETTINGS =
            TestcontainersSettings.builder()
                    .logger(LOG)
                    .network(NETWORK)
                    .dependsOn(MONGO_CONTAINER)
                    .build();

    @RegisterExtension
    public static final FlinkContainers FLINK =
            FlinkContainers.builder()
                    .withFlinkContainersSettings(
                            FlinkContainersSettings.builder().numTaskManagers(2).build())
                    .withTestcontainersSettings(TESTCONTAINERS_SETTINGS)
                    .build();

    private static MongoClient mongoClient;

    @BeforeAll
    private static void setUp() throws Exception {
        mongoClient = MongoClients.create(MONGO_CONTAINER.getConnectionString());
    }

    @AfterAll
    private static void teardown() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    @Test
    public void testTableApiSourceAndSink() throws Exception {
        MongoDatabase db = mongoClient.getDatabase("test");

        int ordersCount = 5;
        List<Document> orders = mockOrders(ordersCount);
        db.getCollection("orders").insertMany(orders);

        executeSqlStatements(readSqlFile("mongo_e2e.sql"));

        List<Document> ordersBackup = readAllBackupOrders(db, ordersCount);

        assertThat(ordersBackup, equalTo(orders));
    }

    private List<Document> readAllBackupOrders(MongoDatabase db, int expect) throws Exception {
        Deadline deadline = Deadline.fromNow(Duration.ofSeconds(20));
        List<Document> backupOrders;
        do {
            Thread.sleep(1000);
            backupOrders = db.getCollection("orders_bak").find().into(new ArrayList<>());

            db.getCollection("orders_bak").find().into(new ArrayList<>());
        } while (deadline.hasTimeLeft() && backupOrders.size() < expect);

        return backupOrders;
    }

    private List<Document> mockOrders(int ordersCount) {
        List<Document> orders = new ArrayList<>();
        for (int i = 1; i <= ordersCount; i++) {
            orders.add(
                    new Document("_id", new ObjectId())
                            .append("code", "ORDER_" + i)
                            .append("quantity", ordersCount * 10L));
        }
        return orders;
    }

    private List<String> readSqlFile(final String resourceName) throws Exception {
        return Files.readAllLines(Paths.get(getClass().getResource("/" + resourceName).toURI()));
    }

    private void executeSqlStatements(final List<String> sqlLines) throws Exception {
        FLINK.submitSQLJob(
                new SQLJobSubmission.SQLJobSubmissionBuilder(sqlLines)
                        .addJars(sqlConnectorMongoDBJar)
                        .build());
    }
}
