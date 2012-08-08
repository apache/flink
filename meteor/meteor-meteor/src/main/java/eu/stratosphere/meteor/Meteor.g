grammar Meteor;

options {
    language=Java;
    output=AST;
    ASTLabelType=EvaluationExpression;
    //backtrack=true;
    //memoize=true;
    superClass=MeteorParserBase;
}

tokens {	
    EXPRESSION;
    OPERATOR;
}

@lexer::header { 
package eu.stratosphere.meteor; 
}

@parser::header { 
package eu.stratosphere.meteor; 

import eu.stratosphere.sopremo.operator.*;
import eu.stratosphere.sopremo.io.*;
import eu.stratosphere.sopremo.query.*;
import eu.stratosphere.sopremo.pact.*;
import eu.stratosphere.sopremo.expressions.*;
import eu.stratosphere.sopremo.function.*;
import java.math.*;
import java.util.IdentityHashMap;
}

@rulecatch { }

@parser::members {
  private boolean setInnerOutput(Token VAR, Operator<?> op) {
	  JsonStreamExpression output = new JsonStreamExpression($operator::result.getOutput($objectCreation::mappings.size()));
	  $objectCreation::mappings.add(new ObjectCreation.TagMapping(output, new JsonStreamExpression(op)));
	  getVariableRegistry().getRegistry(1).put(VAR.getText(), output);
	  return true;
	}

  public void parseSinks() throws RecognitionException {
    script();
  }
  
  private EvaluationExpression makePath(Token inputVar, String... path) {
      EvaluationExpression selection = getVariable(inputVar).toInputSelection($operator::result);
      
      List<EvaluationExpression> accesses = new ArrayList<EvaluationExpression>();
      accesses.add((EvaluationExpression) selection);
      for (String fragment : path)
        accesses.add(new ObjectAccess(fragment));
      return PathExpression.wrapIfNecessary(accesses);
    }
}

script
	:	 (statement ';')+ ->;

statement
	:	(assignment | operator | packageImport | functionDefinition | javaudf) ->;
	
packageImport
  :  'using' packageName=ID { getPackageManager().importPackage($packageName.text); } ',' 
     (additionalPackage=ID { getPackageManager().importPackage($additionalPackage.text); })* ->;
	
assignment
	:	target=VAR '=' source=operator { putVariable($target, new JsonStreamExpression($source.op)); } -> ;

functionDefinition
@init { List<Token> params = new ArrayList(); }
  : name=ID '=' FN '('  
  (param=ID { params.add($param); }
  (',' param=ID { params.add($param); })*)? 
  ')' 
  { 
    addConstantScope();
    for(int index = 0; index < params.size(); index++) 
      this.getConstantRegistry().put(params.get(index).getText(), new InputSelection(index)); 
  } 
  def=contextAwareExpression[null] 
  { 
    addFunction(name.getText(), new ExpressionFunction(params.size(), def.tree));
    removeConstantScope(); 
  } ->; 

javaudf
  : name=ID '=' JAVAUDF '(' path=STRING ')' 
  { addFunction($name.getText(), path.getText()); } ->;

contextAwareExpression [EvaluationExpression contextExpression]
scope { EvaluationExpression context }
@init { $contextAwareExpression::context = $contextExpression; }
  : ternaryExpression;

expression
  : (operatorExpression)=> operatorExpression
  | ternaryExpression;

ternaryExpression
	:	(orExpression '?')=> ifClause=orExpression '?' ifExpr=orExpression ':' elseExpr=orExpression
	-> ^(EXPRESSION["TernaryExpression"] $ifClause { ifExpr == null ? EvaluationExpression.VALUE : $ifExpr.tree } { $elseExpr.tree })
	| (orExpression IF)=> ifExpr2=orExpression IF ifClause2=orExpression
  -> ^(EXPRESSION["TernaryExpression"] $ifClause2 $ifExpr2 { EvaluationExpression.VALUE })
  | orExpression;
	
orExpression
  : exprs+=andExpression ((OR | '||') exprs+=andExpression)*
  -> { $exprs.size() == 1 }? { $exprs.get(0) }
  -> { OrExpression.valueOf($exprs) };
	
andExpression
  : exprs+=elementExpression ((AND | '&&') exprs+=elementExpression)*
  -> { $exprs.size() == 1 }? { $exprs.get(0) }
  -> { AndExpression.valueOf($exprs) };
  
elementExpression
	:	elem=comparisonExpression (not=NOT? IN set=comparisonExpression)? 
	-> { set == null }? $elem
	-> ^(EXPRESSION["ElementInSetExpression"] $elem 
	{ $not == null ? ElementInSetExpression.Quantor.EXISTS_IN : ElementInSetExpression.Quantor.EXISTS_NOT_IN} $set);
	
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
	:	('!' | '~')? castExpression;

castExpression
	:	('(' ID ')')=> '(' type=ID ')' expr=generalPathExpression
  -> { coerce($type.text, $expr.tree) }
	| (generalPathExpression AS)=> expr=generalPathExpression AS type=ID
  -> { coerce($type.text, $expr.tree) }
	| generalPathExpression;
	
generalPathExpression
	: value=valueExpression 
	  ((pathExpression)=> path=pathExpression -> { PathExpression.wrapIfNecessary($value.tree, $path.tree) } 
	  | -> $value);

contextAwarePathExpression[EvaluationExpression context]
  : path=pathExpression -> { PathExpression.wrapIfNecessary(context, $path.tree) };
  
pathExpression
scope {  List<EvaluationExpression> fragments; }
@init { $pathExpression::fragments = new ArrayList<EvaluationExpression>(); }
  : // add .field or [index] to path
    ( (('?.')=> '?.' (field=ID { $pathExpression::fragments.add(new ObjectAccess($field.text, true)); } )) 
        | (('.')=> '.' (field=ID { $pathExpression::fragments.add(new ObjectAccess($field.text)); } )) 
        | ('[')=> arrayAccess { $pathExpression::fragments.add($arrayAccess.tree); } )+ 
  -> { PathExpression.wrapIfNecessary($pathExpression::fragments) };

valueExpression
	:	(ID '(')=> methodCall[null]
	| parenthesesExpression 
	| literal 
	| (VAR '[' VAR)=> streamIndexAccess
	| VAR -> { makePath($VAR) }
  | ((ID ':')=> packageName=ID ':')? constant=ID { getScope($packageName.text).getConstantRegistry().get($constant.text) != null }? => 
    -> { getScope($packageName.text).getConstantRegistry().get($constant.text) }  
	| arrayCreation 
	| objectCreation ;
	
operatorExpression
	:	op=operator -> ^(EXPRESSION["NestedOperatorExpression"] { $op.op });
		
parenthesesExpression
	:	('(' expression ')') -> expression;

methodCall [EvaluationExpression targetExpr]
@init { List<EvaluationExpression> params = new ArrayList(); }
	:	(packageName=ID ':')? name=ID '('	
	(param=expression { params.add($param.tree); }
	(',' param=expression { params.add($param.tree); })*)? 
	')' -> { createCheckedMethodCall($packageName.text, $name, $targetExpr, params.toArray(new EvaluationExpression[params.size()])) };
	
fieldAssignment
	:	ID ':' expression 
    { $objectCreation::mappings.add(new ObjectCreation.FieldAssignment($ID.text, $expression.tree)); } ->
  | VAR 
    ( '.' STAR { $objectCreation::mappings.add(new ObjectCreation.CopyFields(makePath($VAR))); } ->
      | '=' op=operator { setInnerOutput($VAR, $op.op) }?=>
      | p=contextAwarePathExpression[getVariable($VAR).toInputSelection($operator::result)]
      ( ':' e2=expression { $objectCreation::mappings.add(new ObjectCreation.TagMapping($p.tree, $e2.tree)); } ->
        | /* empty */ { $objectCreation::mappings.add(new ObjectCreation.FieldAssignment(getAssignmentName($p.tree), $p.tree)); } ->
      )
    );

objectCreation
scope {  List<ObjectCreation.Mapping> mappings; }
@init { $objectCreation::mappings = new ArrayList<ObjectCreation.Mapping>(); }
	:	'{' (fieldAssignment (',' fieldAssignment)* ','?)? '}' -> ^(EXPRESSION["ObjectCreation"] { $objectCreation::mappings });

literal
	: val='true' -> ^(EXPRESSION["ConstantExpression"] { Boolean.TRUE })
	| val='false' -> ^(EXPRESSION["ConstantExpression"] { Boolean.FALSE })
	| val=DECIMAL -> ^(EXPRESSION["ConstantExpression"] { new BigDecimal($val.text) })
	| val=STRING -> ^(EXPRESSION["ConstantExpression"] { $val.getText() })
  | (val=UINT | val=INTEGER) -> ^(EXPRESSION["ConstantExpression"] { parseInt($val.text) })
  | 'null' -> { ConstantExpression.NULL };

arrayAccess
  : '[' STAR ']' path=pathExpression
  -> ^(EXPRESSION["ArrayProjection"] $path)  
  | '[' (pos=INTEGER | pos=UINT) ']' 
  -> ^(EXPRESSION["ArrayAccess"] { Integer.valueOf($pos.text) })
  | '[' (start=INTEGER | start=UINT) ':' (end=INTEGER | end=UINT) ']' 
  -> ^(EXPRESSION["ArrayAccess"] { Integer.valueOf($start.text) } { Integer.valueOf($end.text) });
  
streamIndexAccess
  : op=VAR { getVariable($op) != null }?=>
    '[' path=generalPathExpression ']' { !($path.tree instanceof ConstantExpression) }?
  -> { new StreamIndexExpression(getVariable($op).getStream(), $path.tree) };
	
arrayCreation
	:	 '[' elems+=expression (',' elems+=expression)* ','? ']' -> ^(EXPRESSION["ArrayCreation"] { $elems.toArray(new EvaluationExpression[$elems.size()]) });

operator returns [Operator<?> op=null]
scope { 
  Operator<?> result;
  int numInputs;
  Map<JsonStream, List<ExpressionTag>> inputTags;
}
@init {
  if(state.backtracking == 0) 
	  addScope();
	$operator::inputTags = new IdentityHashMap<JsonStream, List<ExpressionTag>>();
}
@after {
  removeScope();
}:	opRule=(readOperator | writeOperator | genericOperator) 
{ 
  $op = $operator::result;
}; 

readOperator
	:	'read' 'from' (loc=ID? file=STRING | loc=ID '(' file=STRING ')') { $operator::result = new Source(JsonInputFormat.class, $file.text); } ->;

writeOperator
	:	'write' from=VAR 'to' (loc=ID? file=STRING | loc=ID '(' file=STRING ')') 
{ 
	Sink sink = new Sink(JsonOutputFormat.class, $file.text);
  $operator::result = sink;
  sink.setInputs(getVariable(from).getStream());
  this.sinks.add(sink);
} ->;

genericOperator
scope { 
  OperatorInfo<?> operatorInfo;
}	:	(packageName=ID ':')? name=ID { ($genericOperator::operatorInfo = findOperatorGreedily($packageName.text, $name)) != null }?=>
{ $operator::result = $genericOperator::operatorInfo.newInstance(); } 
operatorFlag*
(('[')=> arrayInput | (VAR)=> input ((',')=> ',' input)*)
operatorOption* ->; 
	
operatorOption
scope {
 OperatorInfo.OperatorPropertyInfo property;
} : //{ findOperatorPropertyRelunctantly($genericOperator::operatorInfo, input.LT(1)) != null }?	
  name=ID
	{ $operatorOption::property = findOperatorPropertyRelunctantly($genericOperator::operatorInfo, name); }
  expr=contextAwareExpression[null] { $operatorOption::property.setValue($operator::result, $expr.tree); } ->;

operatorFlag
scope {
 OperatorInfo.OperatorPropertyInfo property;
}
  : name=ID  { ($operatorFlag::property = findOperatorPropertyRelunctantly($genericOperator::operatorInfo, $name)) != null }?
{ if(!$operatorFlag::property.isFlag())
    throw new QueryParserException(String.format("Property \%s is not a flag", $name.text), name);
  $operatorFlag::property.setValue($operator::result, true); } ->;

input	
scope {
 OperatorInfo.InputPropertyInfo inputProperty;
}	:	(name=VAR IN)? from=VAR
{ 
  int inputIndex = $operator::numInputs++;
  JsonStreamExpression input = getVariable(from);
  $operator::result.setInput(inputIndex, input.getStream());
  
  JsonStreamExpression inputExpression = new JsonStreamExpression(input.getStream(), inputIndex);
  putVariable(name != null ? name : from, inputExpression);
} 
({ ($input::inputProperty = findInputPropertyRelunctantly($genericOperator::operatorInfo, input.LT(1))) != null }?=> 
  { this.input.consume(); } // consume the initial token
  expr=contextAwareExpression[new InputSelection($operator::numInputs - 1)] { $input::inputProperty.setValue($operator::result, $operator::numInputs-1, $expr.tree); })?
-> ;

arrayInput
  : '[' names+=VAR (',' names+=VAR)? ']' 'in' from=VAR
{ 
  $operator::result.setInput(0, getVariable(from).getStream());
  for(int index = 0; index < $names.size(); index++) {
	  putVariable((Token) $names.get(index), new JsonStreamExpression(null, index)); 
  }
} -> ;

/**
 * Lexer rules
 */	
fragment LOWER_LETTER
	:	'a'..'z';

fragment UPPER_LETTER
	:	'A'..'Z';

fragment DIGIT
	:	'0'..'9';

fragment SIGN:	('+'|'-');

// TYPE  : 'int' | 'decimal' | 'double' | 'string' | 'bool';

JAVAUDF : 'javaudf';

OR  : 'or';

AND  : 'and';

IF  : 'if';

ELSE  : 'else';

NOT : 'not';

IN  : 'in';

FN  : 'fn';

AS  : 'as';

ID	:	(LOWER_LETTER | UPPER_LETTER) (LOWER_LETTER | UPPER_LETTER | DIGIT | '_')*;

VAR	:	'$' ID;

STAR	:	'*';

COMMENT
    :   '//' ~('\n'|'\r')* '\r'? '\n' {$channel=HIDDEN;}
    |   '/*' ( options {greedy=false;} : . )* '*/' {$channel=HIDDEN;}
    ;
    
fragment APOSTROPHE
  : '\'';
  
fragment QUOTATION
  : '\"';
    
WS 	:	(' '|'\t'|'\n'|'\r')+ { skip(); };
    
STRING
	:	(QUOTATION (options {greedy=false;} : .)* QUOTATION | APOSTROPHE (options {greedy=false;} : .)* APOSTROPHE)
	{ setText(getText().substring(1, getText().length()-1)); };

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

