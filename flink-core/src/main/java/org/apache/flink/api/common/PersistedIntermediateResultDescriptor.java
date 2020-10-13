package org.apache.flink.api.common;

import org.apache.flink.util.AbstractID;

public interface PersistedIntermediateResultDescriptor {
	AbstractID getIntermediateDataSetId();
}
