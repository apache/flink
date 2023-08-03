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

package org.apache.flink.examples.java.misc;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.flink.examples.java.util.DataSetDeprecationInfo.DATASET_DEPRECATION_INFO;

/**
 * This example shows how to use the collection based execution of Flink.
 *
 * <p>The collection based execution is a local mode that is not using the full Flink runtime.
 * DataSet transformations are executed on Java collections.
 *
 * <p>See the "Local Execution" section in the documentation for more details:
 * https://flink.apache.org/docs/latest/apis/local_execution.html
 *
 * <p>Note: All Flink DataSet APIs are deprecated since Flink 1.18 and will be removed in a future
 * Flink major version. You can still build your application in DataSet, but you should move to
 * either the DataStream and/or Table API. This class is retained for testing purposes.
 */
public class CollectionExecutionExample {

    private static final Logger LOGGER = LoggerFactory.getLogger(CollectionExecutionExample.class);

    /** POJO class representing a user. */
    public static class User {
        public int userIdentifier;
        public String name;

        public User() {}

        public User(int userIdentifier, String name) {
            this.userIdentifier = userIdentifier;
            this.name = name;
        }

        public String toString() {
            return "User{userIdentifier=" + userIdentifier + " name=" + name + "}";
        }
    }

    /** POJO for an EMail. */
    public static class EMail {
        public int userId;
        public String subject;
        public String body;

        public EMail() {}

        public EMail(int userId, String subject, String body) {
            this.userId = userId;
            this.subject = subject;
            this.body = body;
        }

        public String toString() {
            return "eMail{userId=" + userId + " subject=" + subject + " body=" + body + "}";
        }
    }

    public static void main(String[] args) throws Exception {

        LOGGER.warn(DATASET_DEPRECATION_INFO);

        // initialize a new Collection-based execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();

        // create objects for users and emails
        User[] usersArray = {new User(1, "Peter"), new User(2, "John"), new User(3, "Bill")};

        EMail[] emailsArray = {
            new EMail(1, "Re: Meeting", "How about 1pm?"),
            new EMail(1, "Re: Meeting", "Sorry, I'm not available"),
            new EMail(3, "Re: Re: Project proposal", "Give me a few more days to think about it.")
        };

        // convert objects into a DataSet
        DataSet<User> users = env.fromElements(usersArray);
        DataSet<EMail> emails = env.fromElements(emailsArray);

        // join the two DataSets
        DataSet<Tuple2<User, EMail>> joined =
                users.join(emails).where("userIdentifier").equalTo("userId");

        // retrieve the resulting Tuple2 elements into a ArrayList.
        List<Tuple2<User, EMail>> result = joined.collect();

        // Do some work with the resulting ArrayList (=Collection).
        for (Tuple2<User, EMail> t : result) {
            System.err.println("Result = " + t);
        }
    }
}
