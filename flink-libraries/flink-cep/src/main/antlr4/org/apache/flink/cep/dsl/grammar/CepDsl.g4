/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Grammar for Flink CEP DSL
grammar CepDsl;

@header {
package org.apache.flink.cep.dsl.grammar;
}

startPatternExpressionRule : patternExpression EOF;
patternExpression : skipStrategy patternFilterExpression (followedByOrNext)* timeWindow?;
skipStrategy: MOD ((s=SKIP_NO_SKIP) | (s=SKIP_SKIP_PAST_LAST) | s=SKIP_SKIP_TO_FIRST k=LBRACK stringconstant m=RBRACK | s=SKIP_SKIP_TO_LAST k=LBRACK stringconstant m=RBRACK);
followedByOrNext : followedBy | followedByAny | notFollowedBy | (f=LNOT)? patternFilterExpression;
followedBy: f=FOLLOWED_BY patternFilterExpression;
followedByAny: f=FOLLOWED_BY_ANY patternFilterExpression;
notFollowedBy: f=NOT_FOLLOWED_BY patternFilterExpression;
timeWindow: WITHIN c=numberconstant(u=HOUR_SHORT | u=MINUTE_SHORT | u=SECOND_SHORT | u=MILLSECONDS_SHORT);
patternFilterExpression
    		: patternFilterExpressionOptional | patternFilterExpressionMandatory;
patternFilterExpressionMandatory
    		: (i=IDENT EQUALS)? classIdentifier quantifier? expressionList? stopCondition?;
patternFilterExpressionOptional
    		: (i=IDENT EQUALS)? classIdentifier quantifier? expressionList? QUESTION;
quantifier: plus_quantifier | star_quantifier | number_quantifier | number_quantifier_greedy;
number_quantifier_greedy: s=LCURLY numberconstant upper_bound? t=RCURLY QUESTION;
number_quantifier: s=LCURLY numberconstant upper_bound? t=RCURLY;
star_quantifier: r=STAR;
plus_quantifier: q=PLUS;
upper_bound: z=COMMA (upper_bound_unlimited | upper_bound_limited);
upper_bound_limited: numberconstant;
upper_bound_unlimited: k=PLUS;
classIdentifier : i1=escapableStr (DOT i2=escapableStr)*;
escapableStr : i1=IDENT | i3=TICKED_STRING_LITERAL;

stopCondition : (left=LBRACK expression? right=RBRACK);
expressionList : (left=LPAREN expression? right=RPAREN);

expression : evalOrExpression;

evalOrExpression : evalAndExpression (op=OR_EXPR evalAndExpression)*;

evalAndExpression : negatedExpression (op=AND_EXPR negatedExpression)*;

negatedExpression : evalEqualsExpression
		| NOT_EXPR evalEqualsExpression;

evalEqualsExpression : evalRelationalExpression (
			    (eq=EQUALS
			      |  is=IS
			      |  isnot=IS NOT_EXPR
			      |  ne=NOT_EQUAL
			     )
		       (
			evalRelationalExpression
			|  (expressionList)
		       )
		     )*;

evalRelationalExpression : concatenationExpr (
			(
			  (
			    (r=LT|r=GT|r=LE|r=GE)
			    	(
			    	  concatenationExpr
			    	  | ( expressionList)
			    	)

			  )*
			)
			| (n=NOT_EXPR)?
			(
				// Represent the greedy NOT prefix using the token type by
				// testing 'n' and setting the token type accordingly.
				(in=IN_SET
					  (l=LPAREN | l=LBRACK) expression	// brackets are for inclusive/exclusive
						(
							( col=COLON (expression) )		// range
							|
							( (COMMA expression)* )		// list of values
						)
					  (r=RPAREN | r=RBRACK)
					)			)
		);

concatenationExpr : additiveExpression ( c=LOR additiveExpression ( LOR additiveExpression)* )?;
additiveExpression : multiplyExpression ( (PLUS|MINUS) multiplyExpression )*;

multiplyExpression : unaryExpression ( (STAR|DIV|MOD) unaryExpression )*;

unaryExpression : MINUS eventProperty
		| constant
		| eventProperty;

eventProperty : eventPropertyAtomic (DOT eventPropertyAtomic)*;

eventPropertyAtomic : eventPropertyIdent (
			lb=LBRACK ni=number RBRACK (q=QUESTION)?
			|
			lp=LPAREN (s=STRING_LITERAL | s=QUOTED_STRING_LITERAL) RPAREN (q=QUESTION)?
			|
			q1=QUESTION
			)?;

eventPropertyIdent : ipi=keywordAllowedIdent (ESCAPECHAR DOT ipi2=keywordAllowedIdent?)*;

constant : numberconstant
		| stringconstant
		| t=BOOLEAN_TRUE
		| f=BOOLEAN_FALSE
		| nu=VALUE_NULL;

numberconstant : (m=MINUS | p=PLUS)? number;

stringconstant : sl=STRING_LITERAL
		| qsl=QUOTED_STRING_LITERAL;



keywordAllowedIdent : i1=IDENT
		| i2=TICKED_STRING_LITERAL
		| AT
		| ESCAPE
		| SUM
		| AVG
		| MAX
		| MIN
		| UNTIL
		| WEEKDAY
		| LW
		| INSTANCEOF
		| TYPEOF
		| CAST;

number : IntegerLiteral | FloatingPointLiteral;

// Tokens
SKIP_NO_SKIP: 'no_skip';
SKIP_SKIP_PAST_LAST: 'skip_past_last';
SKIP_SKIP_TO_FIRST: 'skip_to_first';
SKIP_SKIP_TO_LAST: 'skip_to_last';
IN_SET:'in';
BETWEEN:'between';
LIKE:'like';
REGEXP:'regexp';
ESCAPE:'escape';
OR_EXPR:'or';
AND_EXPR:'and';
NOT_EXPR:'not';
WHERE:'where';
AS:'as';
SUM:'sum';
AVG:'avg';
MAX:'max';
MIN:'min';
ON:'on';
IS:'is';
WEEKDAY:'weekday';
LW:'lastweekday';
INSTANCEOF:'instanceof';
TYPEOF:'typeof';
CAST:'cast';
CURRENT_TIMESTAMP:'current_timestamp';
UNTIL:'until';
AT:'at';
TIMEPERIOD_YEAR:'year';
TIMEPERIOD_YEARS:'years';
TIMEPERIOD_MONTH:'month';
TIMEPERIOD_MONTHS:'months';
TIMEPERIOD_WEEK:'week';
TIMEPERIOD_WEEKS:'weeks';
TIMEPERIOD_DAY:'day';
TIMEPERIOD_DAYS:'days';
TIMEPERIOD_HOUR:'hour';
TIMEPERIOD_HOURS:'hours';
TIMEPERIOD_MINUTE:'minute';
TIMEPERIOD_MINUTES:'minutes';
TIMEPERIOD_SEC:'sec';
TIMEPERIOD_SECOND:'second';
TIMEPERIOD_SECONDS:'seconds';
TIMEPERIOD_MILLISEC:'msec';
TIMEPERIOD_MILLISECOND:'millisecond';
TIMEPERIOD_MILLISECONDS:'milliseconds';
TIMEPERIOD_MICROSEC:'usec';
TIMEPERIOD_MICROSECOND:'microsecond';
TIMEPERIOD_MICROSECONDS:'microseconds';
BOOLEAN_TRUE:'true';
BOOLEAN_FALSE:'false';
VALUE_NULL:'null';
WITHIN: 'within';
HOUR_SHORT: 'h';
MINUTE_SHORT: 'm';
SECOND_SHORT: 's';
MILLSECONDS_SHORT: 'ms';


// Operators
NOT_FOLLOWED_BY : '!->';
FOLLOWED_BY 	: '->';
FOLLOWED_BY_ANY : '->>';
GOES 		: '=>';
EQUALS 		: '=';
QUESTION 	: '?';
LPAREN 		: '(';
RPAREN 		: ')';
LBRACK 		: '[';
RBRACK 		: ']';
LCURLY 		: '{';
RCURLY 		: '}';
COLON 		: ':';
COMMA 		: ',';
LNOT 		: '!';
BNOT 		: '~';
NOT_EQUAL 	: '!=';
DIV 		: '/';
PLUS 		: '+';
MINUS 		: '-';
DEC 		: '--';
STAR 		: '*';
MOD 		: '%';
GE 		: '>=';
GT 		: '>';
LE 		: '<=';
LT 		: '<';
BXOR 		: '^';
BOR		: '|';
LOR		: '||';
BAND 		: '&';
BAND_ASSIGN 	: '&=';
LAND 		: '&&';
SEMI 		: ';';
DOT 		: '.';
NUM_LONG	: '\u18FF';  // assign bogus unicode characters so the token exists
NUM_DOUBLE	: '\u18FE';
NUM_FLOAT	: '\u18FD';
ESCAPECHAR	: '\\';
ESCAPEBACKTICK	: '`';
ATCHAR		: '@';
HASHCHAR	: '#';

// Whitespace -- ignored
WS	:	(	' '
		|	'\t'
		|	'\f'
			// handle newlines
		|	(
				'\r'    // Macintosh
			|	'\n'    // Unix (the right way)
			)
		)+
		-> channel(HIDDEN)
	;

// Single-line comments
SL_COMMENT
	:	'//'
		(~('\n'|'\r'))* ('\n'|'\r'('\n')?)?
		-> channel(HIDDEN)
	;

// multiple-line comments
ML_COMMENT
    	:   	'/*' (.)*? '*/'
		-> channel(HIDDEN)
    	;

TICKED_STRING_LITERAL
    :   '`' ( EscapeSequence | ~('`'|'\\') )* '`'
    ;

QUOTED_STRING_LITERAL
    :   '\'' ( EscapeSequence | ~('\''|'\\') )* '\''
    ;

STRING_LITERAL
    :  '"' ( EscapeSequence | ~('\\'|'"') )* '"'
    ;

fragment
EscapeSequence	:	'\\'
		(	'n'
		|	'r'
		|	't'
		|	'b'
		|	'f'
		|	'"'
		|	'\''
		|	'\\'
		|	UnicodeEscape
		|	OctalEscape
		|	. // unknown, leave as it is
		)
    ;

// an identifier.  Note that testLiterals is set to true!  This means
// that after we match the rule, we look in the literals table to see
// if it's a literal or really an identifer
IDENT
	:	('a'..'z'|'_'|'$') ('a'..'z'|'_'|'0'..'9'|'$')*
	;
IntegerLiteral
    :   DecimalIntegerLiteral
    |   HexIntegerLiteral
    |   OctalIntegerLiteral
    |   BinaryIntegerLiteral
    ;

FloatingPointLiteral
    :   DecimalFloatingPointLiteral
    |   HexadecimalFloatingPointLiteral
    ;
fragment
OctalEscape
    :   '\\' ('0'..'3') ('0'..'7') ('0'..'7')
    |   '\\' ('0'..'7') ('0'..'7')
    |   '\\' ('0'..'7')
    ;
fragment
UnicodeEscape
    :   '\\' 'u' HexDigit HexDigit HexDigit HexDigit
    ;

fragment
DecimalIntegerLiteral
    :   DecimalNumeral IntegerTypeSuffix?
    ;
fragment
HexIntegerLiteral
    :   HexNumeral IntegerTypeSuffix?
    ;
fragment
OctalIntegerLiteral
    :   OctalNumeral IntegerTypeSuffix?
    ;
fragment
BinaryIntegerLiteral
    :   BinaryNumeral IntegerTypeSuffix?
    ;
fragment
IntegerTypeSuffix
    :   [lL]
    ;
fragment
DecimalNumeral
    :   '0'
    |   ('0')* NonZeroDigit (Digits? | Underscores Digits)
    ;
fragment
Digits
    :   Digit (DigitOrUnderscore* Digit)?
    ;
fragment
Digit
    :   '0'
    |   NonZeroDigit
    ;
fragment
NonZeroDigit
    :   [1-9]
    ;
fragment
DigitOrUnderscore
    :   Digit
    |   '_'
    ;
fragment
Underscores
    :   '_'+
    ;
fragment
HexNumeral
    :   '0' [xX] HexDigits
    ;
fragment
HexDigits
    :   HexDigit (HexDigitOrUnderscore* HexDigit)?
    ;
fragment
HexDigit
    :   [0-9a-fA-F]
    ;
fragment
HexDigitOrUnderscore
    :   HexDigit
    |   '_'
    ;
fragment
OctalNumeral
    :   '0' Underscores? OctalDigits
    ;
fragment
OctalDigits
    :   OctalDigit (OctalDigitOrUnderscore* OctalDigit)?
    ;
fragment
OctalDigit
    :   [0-7]
    ;
fragment
OctalDigitOrUnderscore
    :   OctalDigit
    |   '_'
    ;
fragment
BinaryNumeral
    :   '0' [bB] BinaryDigits
    ;
fragment
BinaryDigits
    :   BinaryDigit (BinaryDigitOrUnderscore* BinaryDigit)?
    ;
fragment
BinaryDigit
    :   [01]
    ;
fragment
BinaryDigitOrUnderscore
    :   BinaryDigit
    |   '_'
    ;
fragment
DecimalFloatingPointLiteral
    :   Digits '.' Digits? ExponentPart? FloatTypeSuffix?
    |   '.' Digits ExponentPart? FloatTypeSuffix?
    |   Digits ExponentPart FloatTypeSuffix?
    |   Digits FloatTypeSuffix
    ;
fragment
ExponentPart
    :   ExponentIndicator SignedInteger
    ;
fragment
ExponentIndicator
    :   [eE]
    ;
fragment
SignedInteger
    :   Sign? Digits
    ;
fragment
Sign
    :   [+-]
    ;
fragment
FloatTypeSuffix
    :   [fFdD]
    ;
fragment
HexadecimalFloatingPointLiteral
    :   HexSignificand BinaryExponent FloatTypeSuffix?
    ;
fragment
HexSignificand
    :   HexNumeral '.'?
    |   '0' [xX] HexDigits? '.' HexDigits
    ;
fragment
BinaryExponent
    :   BinaryExponentIndicator SignedInteger
    ;
fragment
BinaryExponentIndicator
    :   [pP]
    ;
