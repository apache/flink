package org.apache.flink.runtime.concurrent;

/** Testing ComponentMainThreadExecutor which can be manually triggered. */
public class ManuallyTriggeredComponentMainThreadExecutor
        extends ManuallyTriggeredScheduledExecutorService implements ComponentMainThreadExecutor {

    private Thread executorThread = null;

    @Override
    public void assertRunningInMainThread() {
        // always true
    }

    @Override
    public void trigger() {
        Thread actual = Thread.currentThread();
        if (executorThread == null) {
            executorThread = actual;
        } else if (executorThread != actual) {
            throw new IllegalStateException(
                    "Expecting the use of the same thread for all trigger invocations.");
        }
        super.trigger();
    }
}
