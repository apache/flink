package org.apache.flink.table.runtime.operators.window.internal;

import org.apache.flink.table.runtime.operators.window.Window;

/** Components that want to have some custom logic when
 * window cleaning should implement this interface. */
public interface WindowCleanAware<W extends Window> {
	/** Give a change for sub-class to do some additional work when doing window cleaning. */
	default void onWindowCleaning(W window) throws Exception {
		// default do nothing.
	}
}
