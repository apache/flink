/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.gcp.pubsub;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;

import java.math.BigInteger;

/** Helper class to send PubSubMessages to a PubSub topic. */
class PubSubPublisher {
    private final String projectName;
    private final String topicName;

    PubSubPublisher(String projectName, String topicName) {
        this.projectName = projectName;
        this.topicName = topicName;
    }

    /**
     * Publish messages with as payload a single integer. The integers inside the messages start
     * from 0 and increase by one for each message send.
     *
     * @param amountOfMessages amount of messages to send
     */
    void publish(int amountOfMessages) {
        Publisher publisher = null;
        try {
            publisher = Publisher.newBuilder(TopicName.of(projectName, topicName)).build();
            for (int i = 0; i < amountOfMessages; i++) {
                ByteString messageData = ByteString.copyFrom(BigInteger.valueOf(i).toByteArray());
                PubsubMessage message = PubsubMessage.newBuilder().setData(messageData).build();
                publisher.publish(message).get();

                System.out.println("Published message: " + i);
                Thread.sleep(100L);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (publisher != null) {
                    publisher.shutdown();
                }
            } catch (Exception e) {
            }
        }
    }
}
