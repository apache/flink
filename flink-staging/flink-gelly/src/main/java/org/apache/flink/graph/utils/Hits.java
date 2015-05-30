package org.apache.flink.graph.utils;


public enum Hits {
    HUB(0), AUTHORITY(1);

    private int value;

    private Hits(int i) {

        this.value = i;

    }
}
