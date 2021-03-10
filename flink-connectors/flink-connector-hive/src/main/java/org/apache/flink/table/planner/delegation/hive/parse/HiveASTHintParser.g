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
parser grammar HiveASTHintParser;

options
{
  tokenVocab=HiveASTLexer;
  output=AST;
  ASTLabelType=ASTNode;
  backtrack=false;
  k=3;
}

tokens {
  TOK_HINTLIST;
  TOK_HINT;
  TOK_MAPJOIN;
  TOK_STREAMTABLE;
  TOK_HINTARGLIST;
}

@header {
package org.apache.flink.table.planner.delegation.hive.parse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.ASTNode;
}


@members {
  ArrayList<HiveASTParseError> errors = new ArrayList<>();

  @Override
  public void displayRecognitionError(String[] tokenNames,
                                      RecognitionException e) {
    errors.add(new HiveASTParseError(this, e, tokenNames));
  }
}

// starting rule
hint
    : hintList EOF -> ^(TOK_HINTLIST hintList)
    ;

hintList
    :
    hintItem (COMMA hintItem)* -> hintItem+
    ;

hintItem
    :
    hintName (LPAREN hintArgs RPAREN)? -> ^(TOK_HINT hintName hintArgs?)
    ;

hintName
    :
    KW_MAPJOIN -> TOK_MAPJOIN
    | KW_STREAMTABLE -> TOK_STREAMTABLE
    ;

hintArgs
    :
    hintArgName (COMMA hintArgName)* -> ^(TOK_HINTARGLIST hintArgName+)
    ;

hintArgName
    :
    Identifier
    ;
