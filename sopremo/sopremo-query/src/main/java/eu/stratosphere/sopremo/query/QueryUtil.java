package eu.stratosphere.sopremo.query;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.log4j.Level;

public class QueryUtil {

	protected static final Log LOG = LogFactory.getLog(AbstractQueryParser.class);

	public static void trace() {
		((Log4JLogger) LOG).getLogger().setLevel(Level.TRACE);
	}

	public static void untrace() {
		((Log4JLogger) LOG).getLogger().setLevel(((Log4JLogger) LOG).getLogger().getParent().getLevel());
	}
}
