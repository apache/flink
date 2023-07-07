package org.apache.flink.streaming.examples.allowlatency;

/**
 * Example illustrating a windowed stream join between two data streams.
 *
 * <p>The example works on two input streams with pairs (name, grade) and (name, salary)
 * respectively. It joins the streams based on "name" within a configurable window.
 *
 * <p>The example uses a built-in sample data generator that generates the streams of pairs at a
 * configurable rate.
 */
public interface BundleTriggerCallback {
    void finishBundle(boolean exceedLatency);
}
