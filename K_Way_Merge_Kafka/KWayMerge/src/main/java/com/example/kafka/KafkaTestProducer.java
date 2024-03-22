package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class KafkaTestProducer {
    public int PARTITION_COUNT;
    Producer<String, String> producer;


    public KafkaTestProducer(String bootstrapServer, int numPartitions) {
        this.PARTITION_COUNT = numPartitions;
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", bootstrapServer); // Replace with your Kafka broker addresses
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());
        this.producer = new KafkaProducer<String, String>(producerProps);
    }

    public void sendUniqueKeyMessages(int numMessages) {
        for (int i = 0; i < numMessages; i++) {
            String key = "key-" + i;
            String value = i + "";
            ProducerRecord<String, String> record = new ProducerRecord<>("test_topic", key, value);
            System.out.println("Sent Key: " + key + " value: " + value);
            producer.send(record);
        }
    }

    public void sendLimitedKeyMessages(int numMessages, int numKeys) {
        for (int i = 0; i < numMessages; i++) {
            String key = "key-" + (i % numKeys);
            String value = i + "";
            ProducerRecord<String, String> record = new ProducerRecord<>("test_topic", key, value);
            System.out.println("Sent Key: " + key + " value: " + value);
            producer.send(record);
        }
    }

    public void sendSingleKeyMessage(int numMessages) {
        for (int i = 0; i < numMessages; i++) {
            String key = "test_key";
            String value = i + "";
            ProducerRecord<String, String> record = new ProducerRecord<>("test_topic", key, value);
            System.out.println("Sent Key: " + key + " value: " + value);
            producer.send(record);
        }
    }

    public void close() {
        this.producer.close();
    }
}




//    public static void main( String[] args )
//    {
//        try {
//
//            // sendUniqueKeyMessages(producer, totalMessages);
//            // sendLimitedKeyMessages(producer, totalMessages, PARTITION_COUNT);
//
//
//            Thread.sleep(5000); // delay before consuming
//
//            int recieveCount = 0;
//            int pollCount = 0;
//            while (recieveCount < totalMessages || pollCount > 20) {
//                System.out.println("Attempting to poll server");
//                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
//                for (ConsumerRecord<String, String> record : records) {
////                    System.out.println("Partition " + record.partition() + " Key: " + record.key());
//                    perPartitionData[record.partition()].add(Integer.parseInt(record.value()));
//                    recieveCount++;
//                }
//                consumer.commitSync();
//                pollCount++;
//            }
//
//        for (int i = 0; i < perPartitionData.length; i++){
//            ArrayList<Integer> currList = perPartitionData[i];
//            System.out.println("Partition: " + i + " " + currList.toString());
//        }
//        ArrayList<Integer> res = Merge.merge(perPartitionData);
//        System.out.println("Sorted Array: " + res.toString());
//
//
//}
