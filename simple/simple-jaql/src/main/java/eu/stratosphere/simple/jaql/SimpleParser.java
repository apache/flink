package eu.stratosphere.simple.jaql;

import antlr.Parser;
import antlr.ParserSharedInputState;

public abstract class SimpleParser extends Parser {
	
	protected BindingManager bindings = new BindingManager();

	public SimpleParser() {
		super();
	}

	public SimpleParser(ParserSharedInputState parserSharedInputState) {
		super(parserSharedInputState);
	}

}
