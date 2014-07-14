package eu.stratosphere.streaming.util;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class LogUtils {
	
	public static void initializeDefaultConsoleLogger() {
		initializeDefaultConsoleLogger(Level.DEBUG, Level.INFO);
	}
	
	public static void initializeDefaultConsoleLogger(Level logLevel, Level rootLevel) {
		Logger logger = Logger.getLogger("eu.stratosphere.streaming");
		logger.removeAllAppenders();
		PatternLayout layout = new PatternLayout(
				"%d{HH:mm:ss,SSS} %-5p %-60c %x - %m%n");
		ConsoleAppender appender = new ConsoleAppender(layout, "System.err");
		logger.addAppender(appender);
		logger.setLevel(logLevel);
		
		Logger root = Logger.getRootLogger();
		root.removeAllAppenders();
		root.addAppender(appender);
		root.setLevel(rootLevel);
	}
}
