package eu.stratosphere.sopremo.util;

import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.Token;

public class QueryParserException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5142011370807158960L;

	private int line = -1, charIndex = -1;
	
	private Token invalidToken;
	
	/**
	 * Initializes SimpleException.
	 *
	 */
	public QueryParserException() {
		super();
	}

	/**
	 * Initializes SimpleException.
	 *
	 * @param message
	 * @param cause
	 */
	public QueryParserException(String message, Throwable cause) {
		super(message, cause);
		
		if(cause instanceof RecognitionException) {
			this.line = ((RecognitionException) cause).line;
			this.invalidToken = ((RecognitionException) cause).token;
			this.charIndex = ((RecognitionException) cause).charPositionInLine;
		}
	}

	/**
	 * Initializes SimpleException.
	 *
	 * @param message
	 */
	public QueryParserException(String message) {
		super(message);
	}

	/**
	 * Initializes SimpleException.
	 *
	 * @param cause
	 */
	public QueryParserException(String message, Token invalidToken) {
		super(message);
		
		this.invalidToken = invalidToken;
		this.line = invalidToken.getLine();
		this.charIndex = invalidToken.getCharPositionInLine();
	}

	public int getLine() {
		return this.line;
	}

	public void setLine(int line) {
		this.line = line;
	}

	public int getCharIndex() {
		return this.charIndex;
	}

	public void setCharIndex(int charIndex) {
		this.charIndex = charIndex;
	}

	public Token getInvalidToken() {
		return this.invalidToken;
	}

	public void setInvalidToken(Token token) {
		if (token == null)
			throw new NullPointerException("token must not be null");
	
		this.invalidToken = token;
	}
	
	@Override
	public String getMessage() {
		if(getInvalidToken() == null)
			return super.getMessage();
		return String.format("%s: %s @ (%d, %d)",  super.getMessage(), getInvalidToken().getText(), getLine(), getCharIndex());
	}
}
