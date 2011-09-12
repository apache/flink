grammar SJaql;

options {
    language=Java;
    output=AST;
    ASTLabelType=EvaluationExpression;
    backtrack=true;
    memoize=true;
}

tokens {	/*
    SCRIPT;
    ASSIGNMENT;
    JSON_FIELD;
    JSON_OBJECT;
    FUNCTION_CALL;
    OBJECT_EXPR;
    METHOD_CALL;
    FIELD;
    ARRAY_ACCESS;
    ARRAY_CREATION;
    BIND;
        */
    EXPRESSION;
    OPERATOR;
}

@lexer::header { 
package eu.stratosphere.simple.jaql; 
}

@parser::header { 
package eu.stratosphere.simple.jaql; 

import eu.stratosphere.sopremo.*;
import eu.stratosphere.simple.*;
import eu.stratosphere.sopremo.pact.*;
import eu.stratosphere.sopremo.expressions.*;
import eu.stratosphere.sopremo.aggregation.*;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.*;
import java.math.*;
}

@rulecatch { }

@parser::init {
bindings.addScope();
}

@parser::members {
private Map<String, Operator> variables = new HashMap<String, Operator>();
private OperatorFactory operatorFactory = new OperatorFactory();
private List<Sink> sinks = new ArrayList<Sink>();

public Object recoverFromMismatchedSet(IntStream input, RecognitionException e, BitSet follow)
	throws RecognitionException
{
	throw e;
}

protected Object recoverFromMismatchedToken(IntStream input, int ttype, BitSet follow)
	throws RecognitionException
{
	// if next token is what we are looking for then "delete" this token
	if ( mismatchIsUnwantedToken(input, ttype) )
		throw new UnwantedTokenException(ttype, input);
		
	// can't recover with single token deletion, try insertion
	if ( mismatchIsMissingToken(input, follow)  ){
		Object inserted = getMissingSymbol(input, null, ttype, follow);
		throw new MissingTokenException(ttype, input, inserted);
	}
		
	throw new MismatchedTokenException(ttype, input);
}

public SopremoPlan parse() throws RecognitionException {  
    script();
    return new SopremoPlan(sinks);
}

private EvaluationExpression makePath(Token inputVar, String... path) {
	List<EvaluationExpression> accesses = new ArrayList<EvaluationExpression>();

  accesses.add(new InputSelection(inputIndexForVariable(inputVar)));	
		
	for(String fragment : path)
		accesses.add(new ObjectAccess(fragment));
	
	return PathExpression.valueOf(accesses);
}

private Operator getVariable(Token variable) {
	Operator op = variables.get(variable.getText());
	if(op == null)
		throw new IllegalArgumentException("Unknown variable:", new RecognitionException(variable.getInputStream()));
	return op;
}

private int inputIndexForVariable(Token variable) {
  int index = $operator::aliasNames.indexOf(variable.getText());
  if(index == -1)
    index = $operator::inputNames.indexOf(variable.getText());
  if(index == -1)
		throw new IllegalArgumentException("Unknown variable:", new RecognitionException(variable.getInputStream()));
	return index;
}

private Number parseInt(String text) {
  BigInteger result = new BigInteger(text);
  if(result.bitLength() <= 31)
    return result.intValue();
  if(result.bitLength() <= 63)
    return result.longValue();
  return result;
}

//private EvaluationExpression[] getExpressions(List nodes
}

script
	:	 statement (';' statement)* ';' ->;

statement
	:	(assignment | operator) ->;	
	
assignment
	:	target=VAR '=' source=operator { variables.put($target.text, $source.op); } -> ;

expression
	:	orExpression;

orExpression
  : exprs+=andExpression (('or' | '||') exprs+=andExpression)*
  -> { $exprs.size() == 1 }? { $exprs.get(0) }
  -> ^(EXPRESSION["OrExpression"] { $exprs.toArray(new EvaluationExpression[$exprs.size()]) });
	
andExpression
  : exprs+=elementExpression (('and' | '&&') exprs+=elementExpression)*
  -> { $exprs.size() == 1 }? { $exprs.get(0) }
  -> ^(EXPRESSION["AndExpression"] { $exprs.toArray(new EvaluationExpression[$exprs.size()]) });
  
elementExpression
	:	comparisonExpression
	|	elem=comparisonExpression 'in' set=comparisonExpression -> ^(EXPRESSION["ElementInSetExpression"] $elem {ElementInSetExpression.Quantor.EXISTS_IN} $set)
	|	elem=comparisonExpression 'not in' set=comparisonExpression -> ^(EXPRESSION["ElementInSetExpression"] $elem {ElementInSetExpression.Quantor.EXISTS_NOT_IN} $set);
	
comparisonExpression
	:	e1=arithmeticExpression ((s='<=' | s='>=' | s='<' | s='>' | s='==' | s='!=') e2=arithmeticExpression)?
	-> 	{ $s == null }? $e1
	->	{ $s.getText().equals("!=") }? ^(EXPRESSION["ComparativeExpression"] $e1 {ComparativeExpression.BinaryOperator.NOT_EQUAL} $e2)
	-> 	^(EXPRESSION["ComparativeExpression"] $e1 {ComparativeExpression.BinaryOperator.valueOfSymbol($s.text)} $e2);
	
arithmeticExpression
	:	e1=multiplicationExpression ((s='+' | s='-') e2=multiplicationExpression)?
	-> 	{ s != null }? ^(EXPRESSION["ArithmeticExpression"] $e1 
		{ s.getText().equals("+") ? ArithmeticExpression.ArithmeticOperator.ADDITION : ArithmeticExpression.ArithmeticOperator.SUBTRACTION} $e2)
	-> 	$e1;
	
multiplicationExpression
	:	e1=preincrementExpression ((s='*' | s='/') e2=preincrementExpression)?
	-> 	{ s != null }? ^(EXPRESSION["ArithmeticExpression"] $e1 
		{ s.getText().equals("*") ? ArithmeticExpression.ArithmeticOperator.MULTIPLICATION : ArithmeticExpression.ArithmeticOperator.DIVISION} $e2)
	-> 	$e1;
	
preincrementExpression
	:	'++' preincrementExpression
	|	'--' preincrementExpression
	|	unaryExpression;
	
unaryExpression
	:	('!' | '~')? pathExpression;

//castExpression
//	:	'(' STRING ')' valueExpression;
	
pathExpression
scope {  List<EvaluationExpression> fragments; }
@init { $pathExpression::fragments = new ArrayList<EvaluationExpression>(); }
  : valueExpression (
    ('.' (ID { $pathExpression::fragments.add(new ObjectAccess($ID.text)); } 
        /*| functionCall { fragments.add(new ObjectAccess($ID.text)); } */)) 
    | arrayAccess { $pathExpression::fragments.add($arrayAccess.tree); } )+ { $pathExpression::fragments.add(0, $valueExpression.tree); } ->  ^(EXPRESSION["PathExpression"] { $pathExpression::fragments } )
    | valueExpression;

valueExpression
	:	functionCall 
	| parenthesesExpression 
	| literal 
	| VAR -> { makePath($VAR) }
	| arrayCreation 
	| objectCreation 
	| operatorExpression;
	
operatorExpression
	:	operator;
		
parenthesesExpression
	:	('(' expression ')') -> expression;

functionCall
	:	ID '(' (params+=expression (',' params+=expression)*) ')' -> ^(EXPRESSION["FunctionCall"] $params);
	
fieldAssignment returns [ObjectCreation.Mapping mapping]
	:	VAR '.' STAR { $objectCreation::mappings.add(new ObjectCreation.CopyFields(makePath($VAR))); } ->
	|	VAR '.' ID { $objectCreation::mappings.add(new ObjectCreation.Mapping($ID.text, makePath($VAR, $ID.text))); } ->
	|	ID ':' expression { $objectCreation::mappings.add(new ObjectCreation.Mapping($ID.text, $expression.tree)); } ->;

objectCreation
scope {  List<ObjectCreation.Mapping> mappings; }
@init { $objectCreation::mappings = new ArrayList<ObjectCreation.Mapping>(); }
	:	'{' (fieldAssignment (',' fieldAssignment)* ','?)? '}' -> ^(EXPRESSION["ObjectCreation"] { $objectCreation::mappings });


//objectOrMethodAccess
//	:	 functionCall arrayAccess? objectOrMethodAccess? -> ^(OBJECT_EXPR ID functionCall arrayAccess? objectOrMethodAccess?);

literal
	: val='true' -> ^(EXPRESSION["ConstantExpression"] { Boolean.TRUE })
	| val='false' -> ^(EXPRESSION["ConstantExpression"] { Boolean.FALSE })
	| val=DECIMAL -> ^(EXPRESSION["ConstantExpression"] { new BigDecimal($val.text) })
	| val=STRING -> ^(EXPRESSION["ConstantExpression"] { $val.text })
  | val=INTEGER -> ^(EXPRESSION["ConstantExpression"] { parseInt($val.text) })
  | val=UINT -> ^(EXPRESSION["ConstantExpression"] { parseInt($val.text) });

arrayAccess
  :  '[' pos=(INTEGER | UINT) ']' -> ^(EXPRESSION["ArrayAccess"] { Integer.valueOf($pos.text) })
  |  '[' start=(INTEGER | UINT) ':' end=(INTEGER | UINT) ']' -> ^(EXPRESSION["ArrayAccess"] { Integer.valueOf($start.text) } { Integer.valueOf($end.text) })
  |  '[' STAR ']' -> ^(EXPRESSION["ArrayAccess"]);
	
arrayCreation
	:	 '[' elems+=expression (',' elems+=expression)* ','? ']' -> ^(EXPRESSION["ArrayCreation"] $elems);

operator returns [Operator op=null]
scope { 
  List<Operator> inputOperators; 
  List<String> inputNames; 
  List<String> aliasNames; 
  Operator result;
}
@init { 
	$operator::inputOperators = new ArrayList<Operator>();
  $operator::inputNames = new ArrayList<String>();
  $operator::aliasNames = new ArrayList<String>();
}
	:	opRule=(readOperator | writeOperator | genericOperator) 
{ 
  $op = $operator::result;
}; 

readOperator
	:	'read' (loc=ID? file=STRING | loc=ID '(' file=STRING ')') { $operator::result = new Source(JsonInputFormat.class, $file.text); } ->;

writeOperator
	:	'write' from=VAR 'to' (loc=ID? file=STRING | loc=ID '(' file=STRING ')') 
{ 
	Sink sink = new Sink(JsonOutputFormat.class, $file.text, getVariable(from));
  $operator::result = sink;
  this.sinks.add(sink);
} ->;

genericOperator
scope { 
  OperatorFactory.OperatorInfo operatorInfo;
}
	:	name=ID input (',' input)*	
{ 
  OperatorFactory.OperatorInfo info = operatorFactory.getOperatorInfo($name.text);
  if(info == null)
    throw new IllegalArgumentException("Unknown operator:", new RecognitionException(name.getInputStream()));
  $operator::result = info.newInstance($operator::inputOperators);
  if($operator::aliasNames.size() == 1 && $operator::aliasNames.get(0).equals("\$0"))
    $operator::aliasNames.set(0, "\$");
  $genericOperator::operatorInfo = info;
} operatorOption* ->; 
	
operatorOption
	:	name=ID+ expr=expression { $genericOperator::operatorInfo.setProperty($name.text, $expr.tree); } ->;

input	
	:	(name=VAR 'in')? from=VAR
{ 
	$operator::inputOperators.add(getVariable(from)); 
  $operator::inputNames.add(from.getText()); 
  $operator::aliasNames.add(name == null ? String.format("\$\%d", $operator::aliasNames.size()) : name.getText()); 
} -> ;

//-> {$from.text != null}? ^(BIND $name $from?)
//	-> ^($name);	
	
//identifier
//	:	VAR | ID;
	
	
fragment LOWER_LETTER
	:	'a'..'z';

fragment UPPER_LETTER
	:	'A'..'Z';

fragment DIGIT
	:	'0'..'9';

fragment SIGN:	('+'|'-');

ID	:	(LOWER_LETTER | UPPER_LETTER) (LOWER_LETTER | UPPER_LETTER | DIGIT | '_')*;

VAR	:	'$' ID?;

STAR	:	'*';

COMMENT
    :   '//' ~('\n'|'\r')* '\r'? '\n' {$channel=HIDDEN;}
    |   '/*' ( options {greedy=false;} : . )* '*/' {$channel=HIDDEN;}
    ;
    
fragment QUOTE
	:	'\'';
    
WS 	:	(' '|'\t'|'\n'|'\r')+ { skip(); };
    
STRING
	:	QUOTE (options {greedy=false;} : .)* QUOTE { setText(getText().substring(1, getText().length()-1)); };

fragment
ESC_SEQ
    :   '\\' ('b'|'t'|'n'|'f'|'r'|'\"'|'\''|'\\')
    |   UNICODE_ESC
    |   OCTAL_ESC
    ;

fragment
OCTAL_ESC
    :   '\\' ('0'..'3') ('0'..'7') ('0'..'7')
    |   '\\' ('0'..'7') ('0'..'7')
    |   '\\' ('0'..'7')
    ;

fragment
UNICODE_ESC	:   '\\' 'u' HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT   ;
    
fragment
HEX_DIGIT : ('0'..'9'|'a'..'f'|'A'..'F') ;


UINT :	'0'..'9'+;
    
INTEGER :	('+'|'-')? UINT;

DECIMAL
    :   ('0'..'9')+ '.' ('0'..'9')* EXPONENT?
    |   '.' ('0'..'9')+ EXPONENT?
    |   ('0'..'9')+ EXPONENT;

fragment
EXPONENT : ('e'|'E') ('+'|'-')? ('0'..'9')+ ;

