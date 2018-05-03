package org.apache.flink.streaming.tests.artificialstate.eventpayload;

import java.io.Serializable;

/** Callback to verify state upon restoration. */
public interface RestoredStateVerifier<STATE> extends Serializable {
	void verify(STATE state);
}
