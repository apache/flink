grammar SJaql;

options {
    language=Java;
    output=AST;
    ASTLabelType=EvaluationExpression;
    backtrack=true;
    memoize=true;
    superClass=SimpleParser;
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
import eu.stratosphere.util.*;
import eu.stratosphere.simple.*;
import eu.stratosphere.sopremo.pact.*;
import eu.stratosphere.sopremo.expressions.*;
import eu.stratosphere.sopremo.aggregation.*;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.*;
import java.math.*;
import java.util.Arrays;
}

@rulecatch { }

@parser::init {
bindings.addScope();
}

@parser::members {
private Map<String, Operator> variables = new HashMap<String, Operator>();

public void parseSinks() throws RecognitionException {  
    script();
}

private EvaluationExpression makePath(Token inputVar, String... path) {
  List<EvaluationExpression> accesses = new ArrayList<EvaluationExpression>();

  int inputIndex = inputIndexForVariable(inputVar);
  if(inputIndex == -1) {
    accesses.add(new JsonStreamExpression(getVariable(inputVar)));
  } else {
    InputSelection inputSelection = new InputSelection(inputIndex);
    for (ExpressionTag tag : $operator::inputTags.get(inputIndex))
      inputSelection.addTag(tag);
    accesses.add(inputSelection);
  }

  for (String fragment : path)
    accesses.add(new ObjectAccess(fragment));

  return PathExpression.valueOf(accesses);
}

private Operator getVariable(Token variable) {
	Operator op = variables.get(variable.getText());
	if(op == null)
		throw new IllegalArgumentException("Unknown variable" + variable.getText(), new RecognitionException(variable.getInputStream()));
	return op;
}

private int inputIndexForVariable(Token variable) {
  int index = $operator::inputNames.indexOf(variable.getText());
  if(index == -1) {
    if(variable.getText().equals("$") && $operator::inputNames.size() == 1 && !$operator::hasExplicitName.get(0))
      return 0;
    try {
      index = Integer.parseInt(variable.getText().substring(1));
      if($operator::hasExplicitName.get(index))
        throw new IllegalArgumentException("Cannot use index variable " + variable.getText() + " for input with explicit name", 
          new RecognitionException(variable.getInputStream()));
      if(0 > index || index >= $operator::inputNames.size()) 
        throw new IllegalArgumentException("Invalid input index " + index, new RecognitionException(variable.getInputStream()));
    } catch(NumberFormatException e) {
    }
  }
  return index;
}

//private EvaluationExpression[] getExpressions(List nodes
}

script
	:	 statement (';' statement)* ';' ->;

statement
	:	(assignment | operator) ->;	
	
assignment
	:	target=VAR '=' source=operator { variables.put($target.text, $source.op); } -> ;

contextAwareExpression [EvaluationExpression contextExpression]
scope { EvaluationExpression context }
@init { $contextAwareExpression::context = $contextExpression; }
  : expression;

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
  ->  { $s.getText().equals("!=") }? ^(EXPRESSION["ComparativeExpression"] $e1 {ComparativeExpression.BinaryOperator.NOT_EQUAL} $e2)
  ->  { $s.getText().equals("==") }? ^(EXPRESSION["ComparativeExpression"] $e1 {ComparativeExpression.BinaryOperator.EQUAL} $e2)
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
	:	('!' | '~')? 
	  (({$contextAwareExpression::context != null}? contextAwarePathExpression) | pathExpression);

//castExpression
//	:	'(' STRING ')' valueExpression;
	
contextAwarePathExpression
scope {  List<EvaluationExpression> fragments; }
@init { $contextAwarePathExpression::fragments = new ArrayList<EvaluationExpression>(); }
  : start=ID { $contextAwarePathExpression::fragments.add($contextAwareExpression::context); $contextAwarePathExpression::fragments.add(new ObjectAccess($start.text));}
    ( ('.' (field=ID { $contextAwarePathExpression::fragments.add(new ObjectAccess($field.text)); } )) 
        | arrayAccess { $contextAwarePathExpression::fragments.add($arrayAccess.tree); } )* ->  ^(EXPRESSION["PathExpression"] { $contextAwarePathExpression::fragments } );
  
pathExpression
scope {  List<EvaluationExpression> fragments; }
@init { $pathExpression::fragments = new ArrayList<EvaluationExpression>(); }
  : // entry point: valueExpression such as variable name
    valueExpression
    // add .field or [index] to path
    ( ('.' (field=ID { $pathExpression::fragments.add(new ObjectAccess($field.text)); } )) 
        | arrayAccess { $pathExpression::fragments.add($arrayAccess.tree); } )+ { $pathExpression::fragments.add(0, $valueExpression.tree); } ->  ^(EXPRESSION["PathExpression"] { $pathExpression::fragments } )
  // or use expression only
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
@init { List<EvaluationExpression> params = new ArrayList(); }
	:	name=ID '('	
	(param=expression { params.add($param.tree); }
	(',' param=expression { params.add($param.tree); })*)? 
	')' -> ^(EXPRESSION["FunctionCall"] { $name.text } { params.toArray(new EvaluationExpression[params.size()]) });
	
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
  : '[' STAR ']' 
  -> ^(EXPRESSION["ArrayAccess"])  
  | '[' (pos=INTEGER | pos=UINT) ']' 
  -> ^(EXPRESSION["ArrayAccess"] { Integer.valueOf($pos.text) })
  | '[' (start=INTEGER | start=UINT) ':' (end=INTEGER | end=UINT) ']' 
  -> ^(EXPRESSION["ArrayAccess"] { Integer.valueOf($start.text) } { Integer.valueOf($end.text) });
	
arrayCreation
	:	 '[' elems+=expression (',' elems+=expression)* ','? ']' -> ^(EXPRESSION["ArrayCreation"] $elems);

operator returns [Operator op=null]
scope { 
  List<String> inputNames; 
  java.util.BitSet hasExplicitName;
  List<List<ExpressionTag>> inputTags; 
  Operator result;
}
@init { 
  $operator::inputNames = new ArrayList<String>();
  $operator::inputTags = new ArrayList<List<ExpressionTag>>();
  $operator::hasExplicitName = new java.util.BitSet();
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
	Sink sink = new Sink(JsonOutputFormat.class, $file.text);
  $operator::result = sink;
  sink.setInputs(getVariable(from));
  this.sinks.add(sink);
} ->;

genericOperator
scope { 
  OperatorFactory.OperatorInfo operatorInfo;
}
	:	name=ID {$genericOperator::operatorInfo = operatorFactory.getOperatorInfo($name.text);}
({$genericOperator::operatorInfo == null}? moreName=ID 
{$genericOperator::operatorInfo = operatorFactory.getOperatorInfo($name.text + " " + $moreName.text);})? 
{ 
  if($genericOperator::operatorInfo == null)
    throw new IllegalArgumentException("Unknown operator:", new RecognitionException(name.getInputStream()));
  $operator::result = $genericOperator::operatorInfo.newInstance();
} 
operatorFlag*
input (',' input)*	
operatorOption* ->; 
	
operatorOption
scope {
 String optionName;
}
	:	name=ID { $operatorOption::optionName = $name.text; }
({!$genericOperator::operatorInfo.hasProperty($operatorOption::optionName)}? moreName=ID 
 { $operatorOption::optionName = $name.text + " " + $moreName.text;})?
expr=contextAwareExpression[null] { $genericOperator::operatorInfo.setProperty($operatorOption::optionName, $operator::result, $expr.tree); } ->;

operatorFlag
scope {
 String flagName;
}
  : name=ID  { $operatorFlag::flagName = $name.text; }
({!$genericOperator::operatorInfo.hasFlag($operatorFlag::flagName)}? moreName=ID 
 { $operatorFlag::flagName = $name.text + " " + $moreName.text;})?
{ $genericOperator::operatorInfo.setProperty($operatorFlag::flagName, $operator::result, true); } ->;

input	
	:	preserveFlag='preserve'? {} (name=VAR 'in')? from=VAR
{ 
  int inputIndex = $operator::inputNames.size();
  $operator::result.setInput(inputIndex, getVariable(from));
  $operator::inputNames.add(name != null ? name.getText() : from.getText());
  $operator::hasExplicitName.set(inputIndex, name != null); 
  $operator::inputTags.add(preserveFlag == null ? new ArrayList<ExpressionTag>() : Arrays.asList(ExpressionTag.RETAIN));
} 
(inputOption=ID {$genericOperator::operatorInfo.hasInputProperty($inputOption.text)}? 
  expr=contextAwareExpression[new InputSelection($operator::inputNames.size() - 1)] { $genericOperator::operatorInfo.setInputProperty($inputOption.text, $operator::result, $operator::inputNames.size()-1, $expr.tree); })?
-> ;


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

