grammar Meteor;

options {
    language=Java;
    output=AST;
    ASTLabelType=EvaluationExpression;
    backtrack=true;
    memoize=true;
    superClass=AbstractQueryParser;
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

import eu.stratosphere.sopremo.*;
import eu.stratosphere.sopremo.query.*;
import eu.stratosphere.sopremo.pact.*;
import eu.stratosphere.sopremo.type.*;
import eu.stratosphere.sopremo.expressions.*;
import eu.stratosphere.sopremo.aggregation.*;
import eu.stratosphere.sopremo.function.*;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.*;
import java.math.*;
import java.util.IdentityHashMap;
import java.util.Arrays;
}

@rulecatch { }

@parser::members {
{
  addTypeAlias("int", IntNode.class);
  addTypeAlias("decimal", DecimalNode.class);
  addTypeAlias("string", TextNode.class);
  addTypeAlias("double", DoubleNode.class);
  addTypeAlias("boolean", BooleanNode.class);
  addTypeAlias("bool", BooleanNode.class);
  
  addParserFlag(ParserFlag.FUNCTION_OBJECTS);
}

public void parseSinks() throws RecognitionException {  
    script();
}

private boolean setInnerOutput(Token VAR, Operator<?> op) {
  JsonStreamExpression output = new JsonStreamExpression($operator::result.getOutput($objectCreation::mappings.size()));
  $objectCreation::mappings.add(new ObjectCreation.TagMapping(output, new JsonStreamExpression(op)));
  setBinding(VAR, output, 1);
  return true;
}

private String getAssignmentName(EvaluationExpression expression) {
  if(expression instanceof PathExpression)
    return getAssignmentName(((PathExpression) expression).getLastFragment());
  if(expression instanceof ObjectAccess)
    return ((ObjectAccess) expression).getField();
  return expression.toString();
}

private EvaluationExpression makePath(Token inputVar, String... path) {
  Object input = getRawBinding(inputVar, Object.class);
  if(input instanceof Operator<?>) {
    int inputIndex = $operator::result.getInputs().indexOf(((Operator<?>)input).getSource());
    input = new InputSelection(inputIndex);
  } else if(input instanceof JsonStreamExpression)
    input = ((JsonStreamExpression)input).toInputSelection($operator::result);
  
  List<EvaluationExpression> accesses = new ArrayList<EvaluationExpression>();
  accesses.add((EvaluationExpression) input);
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
  :  'using' packageName=ID { importPackage($packageName.text); }->;
	
assignment
	:	target=VAR '=' source=operator { setBinding($target, new JsonStreamExpression($source.op)); } -> ;

functionDefinition
@init { List<Token> params = new ArrayList(); }
  : name=ID '=' 'fn' '('  
  (param=ID { params.add($param); }
  (',' param=ID { params.add($param); })*)? 
  ')' 
  { for(int index = 0; index < params.size(); index++) setBinding(params.get(index), new InputSelection(0)); } 
  def=contextAwareExpression[null] { addFunction(new SopremoFunction(name.getText(), def.tree)); } ->; 

javaudf
  : name=ID '=' 'javaudf' '(' path=STRING ')' 
  { addFunction($name.getText(), path.getText()); } ->;

contextAwareExpression [EvaluationExpression contextExpression]
scope { EvaluationExpression context }
@init { $contextAwareExpression::context = $contextExpression; }
  : expression;

expression
  : ternaryExpression
  | operatorExpression;

ternaryExpression
	:	ifClause=orExpression ('?' ifExpr=expression? ':' elseExpr=expression)
	-> ^(EXPRESSION["TernaryExpression"] $ifClause { ifExpr == null ? EvaluationExpression.VALUE : $ifExpr.tree } { $elseExpr.tree })
	| ifExpr2=orExpression 'if' ifClause2=expression
	-> ^(EXPRESSION["TernaryExpression"] $ifClause2 $ifExpr2 { EvaluationExpression.VALUE })
  | orExpression;
	
orExpression
  : exprs+=andExpression (('or' | '||') exprs+=andExpression)*
  -> { $exprs.size() == 1 }? { $exprs.get(0) }
  -> { OrExpression.valueOf($exprs) };
	
andExpression
  : exprs+=elementExpression (('and' | '&&') exprs+=elementExpression)*
  -> { $exprs.size() == 1 }? { $exprs.get(0) }
  -> { AndExpression.valueOf($exprs) };
  
elementExpression
	:	elem=comparisonExpression (not='not'? 'in' set=comparisonExpression)? 
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
	:	('(' type=ID ')' expr=generalPathExpression
	| expr=generalPathExpression 'as' type=ID
	| expr=generalPathExpression) 
	-> { type != null }? { coerce($type.text, $expr.tree) }
	-> $expr;
	
generalPathExpression
	: value=valueExpression path=pathExpression -> { PathExpression.wrapIfNecessary($value.tree, $path.tree) } 
	| valueExpression;

contextAwarePathExpression[EvaluationExpression context]
  : path=pathExpression -> { PathExpression.wrapIfNecessary(context, $path.tree) };
  
pathExpression
scope {  List<EvaluationExpression> fragments; }
@init { $pathExpression::fragments = new ArrayList<EvaluationExpression>(); }
  : // add .field or [index] to path
    ( ('.' (field=ID { $pathExpression::fragments.add(new ObjectAccess($field.text)); } )) 
        | arrayAccess { $pathExpression::fragments.add($arrayAccess.tree); } )+ 
  -> { PathExpression.wrapIfNecessary($pathExpression::fragments) };

valueExpression
	:	methodCall[null]
	| parenthesesExpression 
	| literal 
	| VAR -> { makePath($VAR) }
  | ID { hasBinding($ID, EvaluationExpression.class) }?=> -> { getBinding($ID, EvaluationExpression.class) }
  | streamIndexAccess
	| arrayCreation 
	| objectCreation ;
	
operatorExpression
	:	op=operator -> ^(EXPRESSION["NestedOperatorExpression"] { $op.op });
		
parenthesesExpression
	:	('(' expression ')') -> expression;

methodCall [EvaluationExpression targetExpr]
@init { List<EvaluationExpression> params = new ArrayList(); }
	:	name=ID '('	
	(param=expression { params.add($param.tree); }
	(',' param=expression { params.add($param.tree); })*)? 
	')' -> { createCheckedMethodCall($name, $targetExpr, params.toArray(new EvaluationExpression[params.size()])) };
	
fieldAssignment
	:	ID ':' expression 
    { $objectCreation::mappings.add(new ObjectCreation.FieldAssignment($ID.text, $expression.tree)); } ->
  | VAR 
    ( '.' STAR { $objectCreation::mappings.add(new ObjectCreation.CopyFields(makePath($VAR))); } ->
      | '=' op=operator { setInnerOutput($VAR, $op.op) }?=>
      | p=contextAwarePathExpression[getBinding($VAR, JsonStreamExpression.class).toInputSelection($operator::result)]
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
  | val=INTEGER -> ^(EXPRESSION["ConstantExpression"] { parseInt($val.text) })
  | 'null' -> { ConstantExpression.NULL };

arrayAccess
  : '[' STAR ']' path=pathExpression
  -> ^(EXPRESSION["ArrayProjection"] $path)  
  | '[' (pos=INTEGER | pos=UINT) ']' 
  -> ^(EXPRESSION["ArrayAccess"] { Integer.valueOf($pos.text) })
  | '[' (start=INTEGER | start=UINT) ':' (end=INTEGER | end=UINT) ']' 
  -> ^(EXPRESSION["ArrayAccess"] { Integer.valueOf($start.text) } { Integer.valueOf($end.text) });
  
streamIndexAccess
  : op=VAR { getRawBinding($op, JsonStreamExpression.class) != null }?=>
    '[' path=generalPathExpression ']' { !($path.tree instanceof ConstantExpression) }?
  -> { new StreamIndexExpression(getBinding($op, JsonStreamExpression.class).getStream(), $path.tree) };
	
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
	  getContext().getBindings().addScope();
	$operator::inputTags = new IdentityHashMap<JsonStream, List<ExpressionTag>>();
}
@after {
  getContext().getBindings().removeScope();
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
  sink.setInputs(getBinding(from, JsonStreamExpression.class).getStream());
  this.sinks.add(sink);
} ->;

genericOperator
scope { 
  OperatorInfo<?> operatorInfo;
}	:	name=ID { ($genericOperator::operatorInfo = findOperatorGreedily($name)) != null }?=>
{ $operator::result = $genericOperator::operatorInfo.newInstance(); } 
operatorFlag*
(arrayInput | input (',' input)*)	
operatorOption* ->; 
	
operatorOption
scope {
 OperatorInfo.OperatorPropertyInfo property;
}
	:	name=ID { ($operatorOption::property = findOperatorPropertyRelunctantly($genericOperator::operatorInfo, $name)) != null }?
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
}	:	preserveFlag='preserve'? {} (name=VAR 'in')? from=VAR
{ 
  int inputIndex = $operator::numInputs++;
  JsonStreamExpression input = getBinding(from, JsonStreamExpression.class);
  $operator::result.setInput(inputIndex, input.getStream());
  
  if(preserveFlag != null)
    setBinding(name != null ? name : from, new JsonStreamExpression(input.getStream(), inputIndex).withTag(ExpressionTag.RETAIN));
  else setBinding(name != null ? name : from, new JsonStreamExpression(input.getStream(), inputIndex));
} 
{ if(state.backtracking == 0) {
    addScope();
  }
}
(inputOption=ID { ($input::inputProperty = findInputPropertyRelunctantly($genericOperator::operatorInfo, $inputOption)) != null }?
  expr=contextAwareExpression[new InputSelection($operator::numInputs - 1)] { $input::inputProperty.setValue($operator::result, $operator::numInputs-1, $expr.tree); })?
{ if(state.backtracking == 0) 
    removeScope();
}  
-> ;

arrayInput
  : '[' names+=VAR (',' names+=VAR)? ']' 'in' from=VAR
{ 
  $operator::result.setInput(0, getBinding(from, JsonStreamExpression.class).getStream());
  for(int index = 0; index < $names.size(); index++) {
	  setBinding((Token) $names.get(index), new InputSelection(index)); 
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

