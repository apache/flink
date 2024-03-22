package com.example.kafka;

public class Main {

    public static void main( String[] args ) {
        // Weird bug - the first time you launch the cluster and run this code it polls forever and cant find any new events. Once you stop the code and try again it works
        // Needs to be fixed in the future but for now I will leave it due to time contraints.
        KafkaTestProducer ktp = new KafkaTestProducer("localhost:9092", 3);
        KafkaMergeConsumer kmc = new KafkaMergeConsumer("localhost:9092",3);

        int totalMessages = 10;
        try {
            ktp.sendSingleKeyMessage(totalMessages);
            Thread.sleep(5000); // delay before consuming
            kmc.consumeMergeLimitedMessages(totalMessages);
        }
        catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("Closing Consumer and producer");
            kmc.close();
            ktp.close();
        }
    }
}
