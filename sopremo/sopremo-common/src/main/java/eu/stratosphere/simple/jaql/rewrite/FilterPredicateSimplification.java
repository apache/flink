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


import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.core.ArrayExpr;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.DoExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;
import com.ibm.jaql.lang.expr.core.FilterExpr;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;


/**
 * The main class for simplifying a filter conjunctive predicates.
 * Example 1: e1 -> filter (e2 and e3 and .... and false and ...) -> en  ----->  [] -> en
 * Example 2: e1 -> filter (e2 and e3 and .... and null and ...) -> en   ----->  [] -> en 
 * Example 3: e1 -> filter (e2 and e3 and .... and true and ...) -> en   ----->  e1 -> filter (e2 and e3 and .... and ...) -> en
 * Example 4: e1 -> filter (true) -> en   ----->  e1 -> en
 */
public class FilterPredicateSimplification extends Rewrite
{
  /**
   * @param phase
   */
  public FilterPredicateSimplification(RewritePhase phase)
  {
    super(phase, FilterExpr.class);
  }

  /**
   * Returns false if all predicates in the Filter have no side effect. Otherwise return true.  
   */
  private boolean sideEffectFilter(FilterExpr fe)
  {
	  for (int i = 0; i < fe.conjunctivePred_count(); i++)
	  {
		  Expr pred = fe.conjunctivePred(i);
		  boolean noExternalEffects = pred.getProperty(ExprProperty.HAS_SIDE_EFFECTS, true).never();
		  if (!noExternalEffects)
			  return true;		  
	  }
	  return false;
  }	

  /**
   * Delete any predicate that is not side-effecting except constant predicates.
   */
  private boolean deleteNonEffectingPred(FilterExpr fe) 
  {
	  boolean modify_flag = false;
	  for (int i = 0; i < fe.conjunctivePred_count(); i++)
	  {
		  Expr pred = fe.conjunctivePred(i);
		  if ((pred.getProperty(ExprProperty.HAS_SIDE_EFFECTS, true).never()) && (!(pred instanceof ConstExpr)))
		  {
			  pred.detach();
			  modify_flag = true;
		  }
	  }
	  return modify_flag;
  }


  /**
   * Converts expression e -> filter ..., false, ...  ---> Do (e, [])
   * Converts expression e -> filter ..., null, ...  ---> Do (e, [])
   */
  private boolean convertToDoExpr(FilterExpr fe) 
  {
	  ArrayExpr empty_input = new ArrayExpr();
	  Expr filter_input = fe.binding().inExpr();
	  DoExpr  do_expr = new DoExpr(filter_input, empty_input);
	  fe.replaceInParent(do_expr);
	  return true;
  }

  
  /**
   * Simplify the predicates.
   */
  private boolean predicateSimplification(FilterExpr fe) 
  {
	  ArrayExpr empty_input = new ArrayExpr();
	  
	  //If the filter tree may have side effect, then the tree can not be eliminated
	  boolean noEffect_filterInput = fe.binding().getProperty(ExprProperty.HAS_SIDE_EFFECTS, true).never();
	  boolean noEffect_filterPred = !sideEffectFilter(fe);

	  //Loop over the conjunctive predicates 
	  for (int i = 0; i < fe.conjunctivePred_count(); i++)
	  {
		  Expr pred = fe.conjunctivePred(i);
		  if (pred instanceof ConstExpr)
		  {
			  JsonValue pred_val = ((ConstExpr)pred).value;
			  if ((pred_val == null) || (pred_val.equals(JsonBool.FALSE)))
			  {
				  if (noEffect_filterInput && noEffect_filterPred)
				  {
					  fe.replaceInParent(empty_input);                 //Replace filter with empty input
					  return true;
				  }
				  else if ((!noEffect_filterInput && !noEffect_filterPred) || (noEffect_filterInput && !noEffect_filterPred))
					  return deleteNonEffectingPred(fe);
				  else
					  return convertToDoExpr(fe);
			  }
			  else if (pred_val.equals(JsonBool.TRUE))
			  {
				  pred.detach();                           //Remove the predicate (and the filter expr if becomes empty)
				  if  (fe.conjunctivePred_count() == 0)
					  fe.replaceInParent(fe.binding().inExpr());
				  return true;
			  }
		  }
	  }
	  return false;
  }
  

/**
   * Rewrite rule for simplifying a filter conjunctive predicates. 
   */
  @Override
  public boolean rewrite(Expr expr)
  {
	  FilterExpr fe = (FilterExpr) expr;
	  return predicateSimplification(fe);
  }
}