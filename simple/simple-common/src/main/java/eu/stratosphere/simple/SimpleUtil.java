package eu.stratosphere.simple;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.log4j.Level;

public class SimpleUtil {

	protected static final Log LOG = LogFactory.getLog(SimpleParser.class);

	public static void trace() {
		(((Log4JLogger) LOG).getLogger()).setLevel(Level.TRACE);
	}

	public static void untrace() {
		(((Log4JLogger) LOG).getLogger()).setLevel((((Log4JLogger) LOG).getLogger()).getParent().getLevel());
	}
}
