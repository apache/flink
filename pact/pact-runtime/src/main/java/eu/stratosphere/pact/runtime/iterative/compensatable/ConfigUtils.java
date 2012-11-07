package eu.stratosphere.pact.runtime.iterative.compensatable;

import eu.stratosphere.nephele.configuration.Configuration;

public class ConfigUtils {

  private ConfigUtils() {}

  public static int asInteger(String key, Configuration parameters) {
    int value = parameters.getInteger(key, -1);
    if (value == -1) {
      throw new IllegalStateException();
    }
    return value;
  }

  public static double asDouble(String key, Configuration parameters) {
    double value = Double.parseDouble(parameters.getString(key, String.valueOf(Double.NaN)));
    if (Double.isNaN(value)) {
      throw new IllegalStateException();
    }
    return value;
  }

  public static long asLong(String key, Configuration parameters) {
    long value = parameters.getLong(key, Long.MIN_VALUE);
    if (value == Long.MIN_VALUE) {
      throw new IllegalStateException();
    }
    return value;
  }
}
