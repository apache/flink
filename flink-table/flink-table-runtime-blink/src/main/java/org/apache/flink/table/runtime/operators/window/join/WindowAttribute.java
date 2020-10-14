package org.apache.flink.table.runtime.operators.window.join;

/**
 * Enumerations to indicate the output of window
 * attributes of the window join side(LHS and RHS).
 *
 * @see WindowAttrCollectors
 */
public enum WindowAttribute {
	/** Does not output any window attributes. */
	NONE(0),
	/** Outputs the window_start. */
	START(1),
	/** Outputs the window_end. */
	END(1),
	/** Outputs the window_start and window_end. */
	START_END(2);

	// Count of the window attributes.
	private final int cnt;

	WindowAttribute(int cnt) {
		this.cnt = cnt;
	}

	public int count() {
		return cnt;
	}
}
