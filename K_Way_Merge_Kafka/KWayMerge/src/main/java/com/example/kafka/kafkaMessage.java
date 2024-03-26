package com.example.kafka;

public class kafkaMessage {
    int seqNum, value;
    long arrivalTime;
    public kafkaMessage(int seqNum, int value, long time) {
        this.seqNum = seqNum;
        this.value = value;
        this.arrivalTime = time;
    }
}
