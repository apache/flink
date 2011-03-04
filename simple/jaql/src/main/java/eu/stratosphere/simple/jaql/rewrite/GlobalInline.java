package eu.stratosphere.simple.jaql.rewrite;
///*
// * Copyright (C) IBM Corp. 2008.
// * 
// * Licensed under the Apache License, Version 2.0 (the "License"); you may not
// * use this file except in compliance with the License. You may obtain a copy of
// * the License at
// * 
// * http://www.apache.org/licenses/LICENSE-2.0
// * 
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// * License for the specific language governing permissions and limitations under
// * the License.
// */
//package com.ibm.jaql.lang.rewrite;
//
//import com.ibm.jaql.json.type.Item;
//import com.ibm.jaql.lang.core.Var;
//import com.ibm.jaql.lang.expr.core.ConstExpr;
//import com.ibm.jaql.lang.expr.core.DefineFunctionExpr;
//import com.ibm.jaql.lang.expr.core.Expr;
//import com.ibm.jaql.lang.expr.core.VarExpr;
//import com.ibm.jaql.lang.expr.io.WriteFn;
//
//// TODO: should this be done by LetInline?
//// TODO: I think this dead with the change to do global-to-let.
///**
// * 
// */
//public class GlobalInline extends Rewrite
//{
//  /**
//   * @param phase
//   */
//  public GlobalInline(RewritePhase phase)
//  {
//    super(phase, VarExpr.class);
//  }
//
//  /*
//   * (non-Javadoc)
//   * 
//   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
//   */
//  @Override
//  public boolean rewrite(Expr expr)
//  {
//    VarExpr varExpr = (VarExpr) expr;
//    Var var = varExpr.var();
//
//    if (!var.isGlobal())
//    {
//      return false;
//    }
//
//    // If the global has already been computed, inline it's value
//    if (var.value instanceof Item)
//    {
//      varExpr.replaceInParent(new ConstExpr((Item)var.value));
//      return true;
//    }
//
//    // Always inline "simple" expressions
//    if (var.expr instanceof ConstExpr || var.expr instanceof VarExpr
//        || var.expr instanceof DefineFunctionExpr)
//    {
//      varExpr.replaceInParent(cloneExpr(var.expr));
//      return true;
//    }
//
//    // Don't inline exprs with side-effects (we could if we can prove they will only be invoked the right number of times...)
//    if (var.expr instanceof WriteFn || // FIXME: need to detect write exprs generically - actually need to detect side-effecting fns
//        mightContainMapReduce(var.expr))
//    {
//      return false;
//    }
//
//    Expr root = varExpr;
//    while (root.parent() != null)
//    {
//      root = root.parent();
//    }
//
//    int numUses = countVarUse(root, var);
//    assert numUses > 0;
//    if (numUses == 1)
//    {
//      varExpr.replaceInParent(cloneExpr(var.expr));
//      return true;
//    }
//
//    return false;
//  }
//}
