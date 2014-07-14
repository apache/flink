package eu.stratosphere.streaming.faulttolerance;

import java.util.HashMap;
import java.util.Map;

public enum FaultToleranceType {
	NONE(0), AT_LEAST_ONCE(1), EXACTLY_ONCE(2);

	public final int id;

	FaultToleranceType(int id) {
		this.id = id;
	}

	private static final Map<Integer, FaultToleranceType> map = new HashMap<Integer, FaultToleranceType>();
	static {
		for (FaultToleranceType type : FaultToleranceType.values())
			map.put(type.id, type);
	}

	public static FaultToleranceType from(int id) {
		return map.get(id);
	}
}