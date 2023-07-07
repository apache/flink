package org.apache.flink.streaming.examples.allowlatency;

public interface BundleTriggerCallback {
    void finishBundle(boolean exceedLatency);
}
