package com.example.kafka;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.apache.kafka.clients.admin.NewTopic;
public class Main {

    public static void main( String[] args ) {
        // Weird bug - the first time you launch the cluster and run this code it polls forever and cant find any new events. Once you stop the code and try again it works
        // Needs to be fixed in the future but for now I will leave it due to time constraints.
        int partitionCount = 3;
        int  messageSendBurstMilli = 200;
        // Initialize queue which stores all incoming messages per partition
        ConcurrentLinkedQueue<kafkaMessage>[] queue = new ConcurrentLinkedQueue[partitionCount];
        for(int i = 0; i < partitionCount; i++) {
            queue[i] = new ConcurrentLinkedQueue<>();
        }

        // Make producer, consumer, and merger
        KafkaTestProducer testProducer = new KafkaTestProducer("localhost:9092", partitionCount);
        KafkaMergeThread mergeThread = new KafkaMergeThread(partitionCount, queue);
        KafkaConsumerThread consumeThread = new KafkaConsumerThread("localhost:9092", partitionCount, queue);

        Thread merge = new Thread(mergeThread);
        Thread consume = new Thread(consumeThread);

        merge.setDaemon(true);
        consume.setDaemon(true);

        consume.start();
        merge.start();

        try {
            Thread.sleep(5000); // Let everyone get initialized
            int totalMessages = 10;
            int messageCount = 0;
            int counter = 0;

            while(counter < 10) { // sent 100 messages
                testProducer.sendLimitedKeyMessages(totalMessages, partitionCount, messageCount);
                messageCount += totalMessages;
                counter++;
                Thread.sleep(messageSendBurstMilli); // delay before consuming
                }
            }
        catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("Closing Consumer and producer");
            consumeThread.stopRunning();
            testProducer.close();

            System.out.println("Stopping merge");
            mergeThread.stopRunning();
        }
    }

}
