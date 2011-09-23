package eu.stratosphere.simple.jaql;

import java.io.IOException;
import java.io.InputStream;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

import org.antlr.runtime.ANTLRInputStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;

import eu.stratosphere.simple.PlanCreator;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SopremoPlan;

public class QueryParser extends PlanCreator {

	Deque<List<Operator<?>>> operatorInputs = new LinkedList<List<Operator<?>>>();

	@Override
	public SopremoPlan getPlan(InputStream stream) {
		try {
			return tryParse(stream);
		} catch (Exception e) {
			return null;
		}
	}

	public SopremoPlan tryParse(InputStream stream) throws IOException, RecognitionException {
		SJaqlLexer lexer = new SJaqlLexer(new ANTLRInputStream(stream));
		CommonTokenStream tokens = new CommonTokenStream();
		tokens.setTokenSource(lexer);
		SJaqlParser parser = new SJaqlParser(tokens);
		parser.setTreeAdaptor(new SopremoTreeAdaptor());
		return parser.parse();
	}
}
