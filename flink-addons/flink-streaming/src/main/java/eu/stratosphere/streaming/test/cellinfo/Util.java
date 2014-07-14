package eu.stratosphere.streaming.test.cellinfo;

public class Util {
  public static int mod(int x, int y) {
    int result = x % y;
    if (result < 0) {
      result += y;
    }
    return result;
  }
}
