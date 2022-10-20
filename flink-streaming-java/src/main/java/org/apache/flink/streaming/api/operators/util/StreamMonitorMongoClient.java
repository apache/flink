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

package org.apache.flink.streaming.api.operators.util;

import org.apache.flink.api.common.ExecutionConfig;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.internal.connection.ServerAddressHelper;
import org.json.simple.JSONObject;

import java.io.Serializable;
import java.util.Map;

/**
 * StreamMonitorMongoClient can be used as a static place to hold an instance of Mongo to connect to
 * the PlanGeneratorFlink database.
 */
public class StreamMonitorMongoClient implements Serializable {
    private static final long serialVersionUID = 1L;
    private static StreamMonitorMongoClient instance;
    private static MongoClient mongoClient;
    private static MongoDatabase db;
    private static String mongoCollectionObservations;

    private StreamMonitorMongoClient(ExecutionConfig config) {
        connect(config);
    }

    public static StreamMonitorMongoClient singleton(ExecutionConfig config) {
        if (instance == null) {
            instance = new StreamMonitorMongoClient(config);
        }
        return instance;
    }

    private static MongoClient connect(ExecutionConfig config) {
        Map<String, String> globalJobParametersMap = config.getGlobalJobParameters().toMap();
        String mongoUsername = globalJobParametersMap.get("-mongoUsername");
        String mongoPassword = globalJobParametersMap.get("-mongoPassword");
        String mongoDatabase = globalJobParametersMap.get("-mongoDatabase");
        String mongoAddress = globalJobParametersMap.get("-mongoAddress");
        int mongoPort = Integer.parseInt(globalJobParametersMap.get("-mongoPort"));
        mongoCollectionObservations = globalJobParametersMap.get("-mongoCollectionObservations");

        ServerAddress serverAddress =
                ServerAddressHelper.createServerAddress(mongoAddress, mongoPort);
        MongoCredential mongoCredentials =
                MongoCredential.createCredential(
                        mongoUsername, mongoDatabase, mongoPassword.toCharArray());
        MongoClientOptions mongoOptions = MongoClientOptions.builder().build();
        mongoClient = new MongoClient(serverAddress, mongoCredentials, mongoOptions);
        db = mongoClient.getDatabase(mongoDatabase);

        return mongoClient;
    }

    public MongoCollection<JSONObject> getMongoCollectionObservations() {
        return db.getCollection(mongoCollectionObservations, JSONObject.class);
    }
}
