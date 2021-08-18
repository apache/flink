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
 /** Counterpart of hive's FromClauseParser.g. */
parser grammar FromClauseASTParser;

options
{
output=AST;
ASTLabelType=HiveParserASTNode;
backtrack=false;
k=3;
}

@members {
  @Override
  public Object recoverFromMismatchedSet(IntStream input,
      RecognitionException re, BitSet follow) throws RecognitionException {
    throw re;
  }
  @Override
  public void displayRecognitionError(String[] tokenNames,
      RecognitionException e) {
    gParent.errors.add(new HiveASTParseError(gParent, e, tokenNames));
  }
}

@rulecatch {
catch (RecognitionException e) {
  throw e;
}
}

//-----------------------------------------------------------------------------------

tableAllColumns
    : STAR
        -> ^(TOK_ALLCOLREF)
    | tableName DOT STAR
        -> ^(TOK_ALLCOLREF tableName)
    ;

// (table|column)
tableOrColumn
@init { gParent.pushMsg("table or column identifier", state); }
@after { gParent.popMsg(state); }
    :
    identifier -> ^(TOK_TABLE_OR_COL identifier)
    ;

expressionList
@init { gParent.pushMsg("expression list", state); }
@after { gParent.popMsg(state); }
    :
    expression (COMMA expression)* -> ^(TOK_EXPLIST expression+)
    ;

aliasList
@init { gParent.pushMsg("alias list", state); }
@after { gParent.popMsg(state); }
    :
    identifier (COMMA identifier)* -> ^(TOK_ALIASLIST identifier+)
    ;

//----------------------- Rules for parsing fromClause ------------------------------
// from [col1, col2, col3] table1, [col4, col5] table2
fromClause
@init { gParent.pushMsg("from clause", state); }
@after { gParent.popMsg(state); }
    :
    KW_FROM fromSource -> ^(TOK_FROM fromSource)
    ;

fromSource
@init { gParent.pushMsg("join source", state); }
@after { gParent.popMsg(state); }
    :
    virtualTableSource
    | 
    uniqueJoinToken^ uniqueJoinSource (COMMA! uniqueJoinSource)+
    |
    joinSource
    ;


atomjoinSource
@init { gParent.pushMsg("joinSource", state); }
@after { gParent.popMsg(state); }
    :
    tableSource (lateralView^)*
    |
    (subQuerySource) => subQuerySource (lateralView^)*
    |
    partitionedTableFunction (lateralView^)*
    |
    LPAREN! joinSource RPAREN!
    ;

joinSource
    :
    atomjoinSource (joinToken^ joinSourcePart (KW_ON! expression {$joinToken.start.getType() != COMMA}? | KW_USING! columnParenthesesList {$joinToken.start.getType() != COMMA}?)?)*
    ;

joinSourcePart
@init { gParent.pushMsg("joinSourcePart", state); }
@after { gParent.popMsg(state); }
    :
    (tableSource | subQuerySource | partitionedTableFunction) (lateralView^)*
    ;

uniqueJoinSource
@init { gParent.pushMsg("unique join source", state); }
@after { gParent.popMsg(state); }
    : KW_PRESERVE? uniqueJoinTableSource uniqueJoinExpr
    ;

uniqueJoinExpr
@init { gParent.pushMsg("unique join expression list", state); }
@after { gParent.popMsg(state); }
    : LPAREN! expressionList RPAREN!
    ;

uniqueJoinToken
@init { gParent.pushMsg("unique join", state); }
@after { gParent.popMsg(state); }
    : KW_UNIQUEJOIN -> TOK_UNIQUEJOIN;

joinToken
@init { gParent.pushMsg("join type specifier", state); }
@after { gParent.popMsg(state); }
    :
      KW_JOIN                      -> TOK_JOIN
    | KW_INNER KW_JOIN             -> TOK_JOIN
    | COMMA                        -> TOK_JOIN
    | KW_CROSS KW_JOIN             -> TOK_CROSSJOIN
    | KW_LEFT  (KW_OUTER)? KW_JOIN -> TOK_LEFTOUTERJOIN
    | KW_RIGHT (KW_OUTER)? KW_JOIN -> TOK_RIGHTOUTERJOIN
    | KW_FULL  (KW_OUTER)? KW_JOIN -> TOK_FULLOUTERJOIN
    | KW_LEFT KW_SEMI KW_JOIN      -> TOK_LEFTSEMIJOIN
    ;

lateralView
@init {gParent.pushMsg("lateral view", state); }
@after {gParent.popMsg(state); }
	:
	(KW_LATERAL KW_VIEW KW_OUTER) => KW_LATERAL KW_VIEW KW_OUTER function tableAlias (KW_AS identifier ((COMMA)=> COMMA identifier)*)?
	-> ^(TOK_LATERAL_VIEW_OUTER ^(TOK_SELECT ^(TOK_SELEXPR function identifier* tableAlias)))
	|
	KW_LATERAL KW_VIEW function tableAlias (KW_AS identifier ((COMMA)=> COMMA identifier)*)?
	-> ^(TOK_LATERAL_VIEW ^(TOK_SELECT ^(TOK_SELEXPR function identifier* tableAlias)))
	;

tableAlias
@init {gParent.pushMsg("table alias", state); }
@after {gParent.popMsg(state); }
    :
    identifier -> ^(TOK_TABALIAS identifier)
    ;

tableBucketSample
@init { gParent.pushMsg("table bucket sample specification", state); }
@after { gParent.popMsg(state); }
    :
    KW_TABLESAMPLE LPAREN KW_BUCKET (numerator=Number) KW_OUT KW_OF (denominator=Number) (KW_ON expr+=expression (COMMA expr+=expression)*)? RPAREN -> ^(TOK_TABLEBUCKETSAMPLE $numerator $denominator $expr*)
    ;

splitSample
@init { gParent.pushMsg("table split sample specification", state); }
@after { gParent.popMsg(state); }
    :
    KW_TABLESAMPLE LPAREN  (numerator=Number) (percent=KW_PERCENT|KW_ROWS) RPAREN
    -> {percent != null}? ^(TOK_TABLESPLITSAMPLE TOK_PERCENT $numerator)
    -> ^(TOK_TABLESPLITSAMPLE TOK_ROWCOUNT $numerator)
    |
    KW_TABLESAMPLE LPAREN  (numerator=ByteLengthLiteral) RPAREN
    -> ^(TOK_TABLESPLITSAMPLE TOK_LENGTH $numerator)
    ;

tableSample
@init { gParent.pushMsg("table sample specification", state); }
@after { gParent.popMsg(state); }
    :
    tableBucketSample |
    splitSample
    ;

tableSource
@init { gParent.pushMsg("table source", state); }
@after { gParent.popMsg(state); }
    : tabname=tableName props=tableProperties? ts=tableSample? (KW_AS? alias=identifier)?
    -> ^(TOK_TABREF $tabname $props? $ts? $alias?)
    ;

uniqueJoinTableSource
@init { gParent.pushMsg("unique join table source", state); }
@after { gParent.popMsg(state); }
    : tabname=tableName ts=tableSample? (KW_AS? alias=identifier)?
    -> ^(TOK_TABREF $tabname $ts? $alias?)
    ;

tableName
@init { gParent.pushMsg("table name", state); }
@after { gParent.popMsg(state); }
    :
    db=identifier DOT tab=identifier
    -> ^(TOK_TABNAME $db $tab)
    |
    tab=identifier
    -> ^(TOK_TABNAME $tab)
    ;

viewName
@init { gParent.pushMsg("view name", state); }
@after { gParent.popMsg(state); }
    :
    (db=identifier DOT)? view=identifier
    -> ^(TOK_TABNAME $db? $view)
    ;

subQuerySource
@init { gParent.pushMsg("subquery source", state); }
@after { gParent.popMsg(state); }
    :
    LPAREN queryStatementExpression RPAREN KW_AS? identifier -> ^(TOK_SUBQUERY queryStatementExpression identifier)
    ;

//---------------------- Rules for parsing PTF clauses -----------------------------
partitioningSpec
@init { gParent.pushMsg("partitioningSpec clause", state); }
@after { gParent.popMsg(state); } 
   :
   partitionByClause orderByClause? -> ^(TOK_PARTITIONINGSPEC partitionByClause orderByClause?) |
   orderByClause -> ^(TOK_PARTITIONINGSPEC orderByClause) |
   distributeByClause sortByClause? -> ^(TOK_PARTITIONINGSPEC distributeByClause sortByClause?) |
   sortByClause -> ^(TOK_PARTITIONINGSPEC sortByClause) |
   clusterByClause -> ^(TOK_PARTITIONINGSPEC clusterByClause)
   ;

partitionTableFunctionSource
@init { gParent.pushMsg("partitionTableFunctionSource clause", state); }
@after { gParent.popMsg(state); } 
   :
   subQuerySource |
   tableSource |
   partitionedTableFunction
   ;

partitionedTableFunction
@init { gParent.pushMsg("ptf clause", state); }
@after { gParent.popMsg(state); } 
   :
   name=Identifier LPAREN KW_ON 
   ((partitionTableFunctionSource) => (ptfsrc=partitionTableFunctionSource spec=partitioningSpec?))
   ((Identifier LPAREN expression RPAREN ) => Identifier LPAREN expression RPAREN ( COMMA Identifier LPAREN expression RPAREN)*)?
   ((RPAREN) => (RPAREN)) ((Identifier) => alias=Identifier)?
   ->   ^(TOK_PTBLFUNCTION $name $alias? $ptfsrc $spec? expression*)
   ; 

//----------------------- Rules for parsing whereClause -----------------------------
// where a=b and ...
whereClause
@init { gParent.pushMsg("where clause", state); }
@after { gParent.popMsg(state); }
    :
    KW_WHERE searchCondition -> ^(TOK_WHERE searchCondition)
    ;

searchCondition
@init { gParent.pushMsg("search condition", state); }
@after { gParent.popMsg(state); }
    :
    expression
    ;

//-----------------------------------------------------------------------------------

//-------- Row Constructor ----------------------------------------------------------
//in support of SELECT * FROM (VALUES(1,2,3),(4,5,6),...) as FOO(a,b,c) and
// INSERT INTO <table> (col1,col2,...) VALUES(...),(...),...
// INSERT INTO <table> (col1,col2,...) SELECT * FROM (VALUES(1,2,3),(4,5,6),...) as Foo(a,b,c)
valueRowConstructor
@init { gParent.pushMsg("value row constructor", state); }
@after { gParent.popMsg(state); }
    :
    expressionsInParenthesis[false] -> ^(TOK_VALUE_ROW expressionsInParenthesis)
    ;

valuesTableConstructor
@init { gParent.pushMsg("values table constructor", state); }
@after { gParent.popMsg(state); }
    :
    valueRowConstructor (COMMA valueRowConstructor)* -> ^(TOK_VALUES_TABLE valueRowConstructor+)
    ;

/*
VALUES(1),(2) means 2 rows, 1 column each.
VALUES(1,2),(3,4) means 2 rows, 2 columns each.
VALUES(1,2,3) means 1 row, 3 columns
*/
valuesClause
@init { gParent.pushMsg("values clause", state); }
@after { gParent.popMsg(state); }
    :
    KW_VALUES! valuesTableConstructor
    ;

/*
This represents a clause like this:
(VALUES(1,2),(2,3)) as VirtTable(col1,col2)
*/
virtualTableSource
@init { gParent.pushMsg("virtual table source", state); }
@after { gParent.popMsg(state); }
   :
   LPAREN valuesClause RPAREN tableNameColList -> ^(TOK_VIRTUAL_TABLE tableNameColList valuesClause)
   ;
/*
e.g. as VirtTable(col1,col2)
Note that we only want literals as column names
*/
tableNameColList
@init { gParent.pushMsg("from source", state); }
@after { gParent.popMsg(state); }
    :
    KW_AS? identifier LPAREN identifier (COMMA identifier)* RPAREN -> ^(TOK_VIRTUAL_TABREF ^(TOK_TABNAME identifier) ^(TOK_COL_NAME identifier+))
    ;

//-----------------------------------------------------------------------------------
