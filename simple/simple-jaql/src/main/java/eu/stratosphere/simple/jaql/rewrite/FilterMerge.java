/*
 * Copyright (C) IBM Corp. 2009.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package eu.stratosphere.simple.jaql.rewrite;


import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.core.VarMap;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.FilterExpr;
import com.ibm.jaql.lang.expr.core.BindingExpr.Type;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;


/**
 * The main class for merging two adjacent filter expressions.
 * Example: e1 -> filter e2 -> filter e3 ----> e1 -> filter e2 and e3 
 */
public class FilterMerge extends Rewrite
{
  /**
   * @param phase
   */
  public FilterMerge(RewritePhase phase)
  {
    super(phase, FilterExpr.class);
  }
  
  //Merge two filters together. Move the predicates from 'child_filter' to 'parent_filter'.
  private boolean mergeFilterExprs(FilterExpr partent_filter) 
  {
	  FilterExpr child_filter = (FilterExpr) partent_filter.binding().inExpr();

	  //If any of the two filter exprs has "non-deterministic" or "side effecting" predicates, then do not merge them.
	  if (partent_filter.externalEffectPredicates() || child_filter.externalEffectPredicates())
		  return false;
	  
	  Var crnt_filter_var = partent_filter.binding().var;
	  BindingExpr child_filter_input = child_filter.binding();
	  Var child_filter_var = child_filter_input.var;
	  VarMap vm = new VarMap();
	  vm.put(child_filter_var, crnt_filter_var);	  

	  for (int i = 0; i < child_filter.conjunctivePred_count(); i++)
	  {
		  Expr pred = child_filter.conjunctivePred(i).clone(vm);
		  partent_filter.addChild(pred);
	  }
	  
	  //Remove the child filter
	  child_filter.replaceInParent(child_filter_input.inExpr());
	  return true;
  }

  /**
   * Rewrite rule for merging two adjacent filter expressions. 
   */
  @Override
  public boolean rewrite(Expr expr)
  {
	  FilterExpr fe = (FilterExpr) expr;
	  BindingExpr filter_input = fe.binding();
	  
	  if (filter_input.inExpr() instanceof FilterExpr)		  
		  return mergeFilterExprs(fe);
	  else
		  return false;
  }
}