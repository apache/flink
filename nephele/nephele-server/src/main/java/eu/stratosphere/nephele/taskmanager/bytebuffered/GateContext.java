package eu.stratosphere.nephele.taskmanager.bytebuffered;

import eu.stratosphere.nephele.io.GateID;

public interface GateContext {
	
	GateID getGateID();
}
