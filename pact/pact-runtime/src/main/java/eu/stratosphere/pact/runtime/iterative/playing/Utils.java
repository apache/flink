package eu.stratosphere.pact.runtime.iterative.playing;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class Utils {

  public static void ensureLogging() {
    Logger rootLogger = Logger.getRootLogger();
    if (!rootLogger.getAllAppenders().hasMoreElements()) {
      rootLogger.setLevel(Level.INFO);
      rootLogger.addAppender(new ConsoleAppender(
          new PatternLayout("%-5p [%t]: %m%n")));

      // The TTCC_CONVERSION_PATTERN contains more info than
      // the pattern we used for the root logger
      Logger pkgLogger = rootLogger.getLoggerRepository().getLogger("robertmaldon.moneymachine");
      pkgLogger.setLevel(Level.DEBUG);
      pkgLogger.addAppender(new ConsoleAppender(
          new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
    }
  }
}
