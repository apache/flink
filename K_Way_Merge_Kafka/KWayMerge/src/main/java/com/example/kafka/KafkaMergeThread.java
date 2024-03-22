package com.example.kafka;

import java.util.concurrent.ConcurrentLinkedQueue;

import java.util.ArrayList;
public class KafkaMergeThread implements  Runnable {
        private volatile boolean running = true;
        private final int partitionCount;
        private ConcurrentLinkedQueue<Integer> partitionQueue [];
        private ArrayList<Integer> perPartitionList [];

        public KafkaMergeThread(int partitionCount, ConcurrentLinkedQueue<Integer>[] queue) {
            this.partitionCount = partitionCount;
            this.partitionQueue = queue;
            this.perPartitionList = new ArrayList[partitionCount];
        }
        @Override
        public void run() {
            boolean foundEmptyQueue = false;

            while (running) {
                for (ConcurrentLinkedQueue<Integer> q : partitionQueue) {
                    if (q.size() == 0) {
                        foundEmptyQueue = true;
                        break;
                    }
                }

                if (!foundEmptyQueue) {
                    // All queues have a value in them
                    for (int qIdx = 0; qIdx < partitionCount; qIdx++) {
                        perPartitionList[qIdx] = new ArrayList<>(partitionQueue[qIdx]);

                        for (Integer item : perPartitionList[qIdx]) {
                            partitionQueue[qIdx].remove(item);
                        }
                    }
                    ArrayList<Integer> res = Merge.merge(perPartitionList);
                    if (res.size() > 0) {
                        System.out.println("Sorted Array: " + res.toString());
                        res.clear();
                    }
                }
//                try {
//                    Thread.sleep(500);
//                }
//                catch (Exception e) {
//                    System.out.println("Had trouble sleeping");
//                    stopRunning();
//                }
                foundEmptyQueue = false;
            }
        }
        public void stopRunning() {
            running = false;
        }
    }
