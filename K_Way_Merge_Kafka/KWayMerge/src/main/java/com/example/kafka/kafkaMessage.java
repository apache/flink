package com.example.kafka;

public class kafkaMessage {
    int seqNum, value;
    public kafkaMessage(int seqNum, int value) {
        this.seqNum = seqNum;
        this.value = value;
    }
}
