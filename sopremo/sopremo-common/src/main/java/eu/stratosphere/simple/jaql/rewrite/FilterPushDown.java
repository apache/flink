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

import java.util.ArrayList;

import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.core.VarMap;
import com.ibm.jaql.lang.expr.array.UnionFn;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.BindingExpr.Type;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.FilterExpr;
import com.ibm.jaql.lang.expr.core.ForExpr;
import com.ibm.jaql.lang.expr.core.GroupByExpr;
import com.ibm.jaql.lang.expr.core.JoinExpr;
import com.ibm.jaql.lang.expr.core.SortExpr;
import com.ibm.jaql.lang.expr.core.TransformExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.metadata.MappingTable;
import com.ibm.jaql.lang.expr.path.PathExpr;
import com.ibm.jaql.lang.expr.path.UnrollExpr;
import com.ibm.jaql.lang.expr.path.UnrollField;
import com.ibm.jaql.lang.rewrite.RewritePhase;


/**
 * The main class for pushing Filter down in the query tree. 
 * 
 * Interaction with other rules:
 * -----------------------------
 * 	  FilterPushDown rule (FPD) depends on pathExpr comparisons for detecting identical expressions. 
 * 	  Therefore, other rules that modify pathExprs to other forms may conflict with FPD.
 * 	  Such rules should be in a phase after FPD: Examples of these rules are:
 *    		--RewriteFirstPathStep
 *    		--PathArrayToFor
 *    		--PathIndexToFn
 *    The following rules also conflict with FPD:
 *    		--UnnestFor (This rule may form a cycle with FPD in the case of  "expand -> filter"). This rule should be in a phase after FPD 
 */
public class FilterPushDown extends Rewrite
{
  /**
   * @param phase
   */
  public FilterPushDown(RewritePhase phase)
  {
    super(phase, FilterExpr.class);
  }
 
  /**
   * Replace the filter variable from 'old_var' to 'new_var'
   */  
  private boolean replaceVarInFilter(FilterExpr filter_expr, Var old_var, Var new_var) 
  {
	  BindingExpr filter_input = filter_expr.binding();
	  filter_input.var = new_var;
	  filter_input.var2 = null;
	  filter_expr.replaceVarInPredicates(old_var, new_var);
	  return true;
  } 
  
  /**
   * plugin a new Filter below a Join in the given child direction
   */
  private boolean plugin_filter_below_join(ArrayList<Expr> pred_list, Var filter_var, JoinExpr join_expr, int child_id)
  {
	  BindingExpr input = join_expr.binding(child_id);
	  Expr be_input = input.inExpr();
	  int slot_num = be_input.getChildSlot(); 
	  
	  BindingExpr new_filter_input = new BindingExpr(Type.IN, new Var(filter_var.name()), null, be_input);
	  FilterExpr new_fe = new FilterExpr(new_filter_input, pred_list);
	  input.setChild(slot_num, new_fe);
	  
	  //Make sure the VARs under the FilterExpr are correct. 
	  replaceVarInFilter(new_fe, filter_var, new_filter_input.var);
	  return true;
  }  
 
  /**
   * Change the expressions in usedIn_list according to the mapping in mappedTo_list.
   * -- usedIn_list points to Exprs in one predicate
   * -- mappedTo_list points to Exprs in the query tree
   */
  private Expr changePredicate(Expr crnt_pred, ArrayList<Expr> usedIn_list, ArrayList<Expr> mappedTo_list, Var old_var, Var new_var) 
  {
	  VarMap vm = new VarMap();
	  vm.put(old_var, new_var);
	  for (int i = 0; i < usedIn_list.size(); i++)
	  {
		  Expr src_expr = usedIn_list.get(i);
		  assert((src_expr instanceof PathExpr) || (src_expr instanceof VarExpr));
		  Expr  map_to = mappedTo_list.get(i).clone(vm);
		  
		  //If the predicate is entirely being replaced, i.e., src_expr == crnt_pred, then we return the new 'map_to' expression. Otherwise,
		  // we return the 'crnt_pred' after modifying it.
		  if (src_expr.equals(crnt_pred))
		  {
			  assert (usedIn_list.size() == 1);
			  if (src_expr.parent() != null)                         //The parent can be null in the case of GroupBy because the predicate is cloned.
				  src_expr.replaceInParent(map_to);
			  return map_to;
		  }
		  else
			  src_expr.replaceInParent(map_to);
	  }
	  return crnt_pred;
  }

  
  /**
   * Try to push Filter below the Transform
   */
  private boolean filterTransformRewrite(FilterExpr filter_expr)
  {
	  TransformExpr transform_expr = (TransformExpr) filter_expr.binding().inExpr();
	  Var transform_var = transform_expr.var();
	  Var filter_pipe_var = filter_expr.binding().var;

	  //If the "filter" or the "transform" is non-deterministic, then do not push the filter
	  if (transform_expr.externalEffectProjection())
		  return false;

	  MappingTable mt = transform_expr.getMappingTable();
	  if (mt.replaceVarInAfterExpr(filter_pipe_var) == false)
		  return false;

	  //A new variable for the new filter	
	  Var newVar = engine.env.makeVar(filter_pipe_var.name());

	  //Check each conjunctive predicate
	  ArrayList<Expr> pushed_pred = new ArrayList<Expr>();
	  for (int k = 0; k < filter_expr.conjunctivePred_count(); k++)
	  {
		  Expr crnt_pred = filter_expr.conjunctivePred(k);
		  ArrayList<Expr> usedIn_list = findMaximalVarOrPathExpr(crnt_pred, filter_pipe_var);  
		  if (usedIn_list.size() == 0)
		  {
			  pushed_pred.add(crnt_pred);
			  continue;
		  }
		  
		  //check if the usage of the piped var can be mapped.
		  ArrayList<Expr> mappedTo_list = predMappedTo(usedIn_list, mt, true);		  
		  if (mappedTo_list != null)
		  {
			  //change_predicate() has the side effect of changing "crnt_pred"
			  //Warning: It also make the query tree in-consistent at this moment
			  Expr modifiedPred = changePredicate(crnt_pred, usedIn_list, mappedTo_list, transform_var, newVar);       
			  pushed_pred.add(modifiedPred);
		  }
	  }
	
	  if (pushed_pred.size() == 0)
		  return false;
	  
	  //Remove the pushed predicates from the main Filter. If it becomes empty, then delete this filter
	  for (int i = 0; i < pushed_pred.size(); i++)
		  pushed_pred.get(i).detach();
	  if  (filter_expr.conjunctivePred_count() == 0)
		  filter_expr.replaceInParent(filter_expr.binding().inExpr());
  
	  //plugin the new filter
	  BindingExpr transformBind = transform_expr.binding();
	  BindingExpr bind = new BindingExpr(Type.IN, newVar, null, transformBind.inExpr());
	  FilterExpr newFilter = new FilterExpr(bind, pushed_pred);
	  transformBind.setChild(0, newFilter);	  
	  return true;
  }


  /**
   * Try to push Filter below the Join
   */
  private boolean filterJoinRewrite(FilterExpr filter_expr) 
  {
	  VarMap vm = new VarMap();
	  JoinExpr join_expr = (JoinExpr) filter_expr.binding().inExpr();
	  
	  //If the "filter" has side effect, then do not push the filter.
	  //Right now, we are limited to only 2-way joins
	  if (join_expr.numBindings() > 2) 
		  return false;

	  boolean left_preserve = join_expr.binding(0).preserve;
	  boolean right_preserve = join_expr.binding(1).preserve;
	  Expr left_child = join_expr.binding(0).inExpr();
	  Expr right_child = join_expr.binding(1).inExpr();
	  MappingTable left_mt = left_child.getMappingTable();
	  MappingTable right_mt = right_child.getMappingTable();  
	  Var filter_pipe_var = filter_expr.binding().var;
	  if (left_mt.replaceVarInAfterExpr(filter_pipe_var) == false)
		  return false;
	  if (right_mt.replaceVarInAfterExpr(filter_pipe_var) == false)
		  return false;
	  
	  //Check each conjunctive predicate
	  ArrayList<Expr> left_pushed_pred = new ArrayList<Expr>();
	  ArrayList<Expr> right_pushed_pred = new ArrayList<Expr>();
	  for (int k = 0; k < filter_expr.conjunctivePred_count(); k++)
	  {
		  Expr crnt_pred = filter_expr.conjunctivePred(k);
		  ArrayList<Expr> usedIn_list = findMaximalVarOrPathExpr(crnt_pred, filter_pipe_var);  
		  if (usedIn_list.size() == 0)
		  {
			  left_pushed_pred.add(crnt_pred);
			  right_pushed_pred.add(crnt_pred.clone(vm));
			  continue;
		  }

		  //Check if the usage of the piped var can be mapped from the left child.
		  //We try pushing the Filter after the left child. So we do not care if the mapping with the child is safe or not (we are not pushing below the child)
		  ArrayList<Expr> mappedTo_list = predMappedTo(usedIn_list, left_mt, false);		  
		  if ((mappedTo_list != null) && (!right_preserve))
		  {
			  left_pushed_pred.add(crnt_pred);
			  continue;
		  }
		  
		  //Check if the usage of the piped var can be mapped from the right child.
		  //We try pushing the Filter after the right child. So we do not care if the mapping with the child is safe or not (we are not pushing below the child)
		  mappedTo_list = predMappedTo(usedIn_list, right_mt, false);		  
		  if ((mappedTo_list != null) && (!left_preserve))
		  {
			  right_pushed_pred.add(crnt_pred);
			  continue;
		  }		  
	  }
	
	  if ((left_pushed_pred.size() == 0) && (right_pushed_pred.size() == 0)) 
		  return false;

	  //Remove the pushed predicates from the main Filter. If it becomes empty, then delete this filter
	  for (int i = 0; i < left_pushed_pred.size(); i++)
		  left_pushed_pred.get(i).detach();
	  for (int i = 0; i < right_pushed_pred.size(); i++)
		  right_pushed_pred.get(i).detach();
	  if  (filter_expr.conjunctivePred_count() == 0)
		  filter_expr.replaceInParent(filter_expr.binding().inExpr());

	  if (left_pushed_pred.size() > 0) 
		  plugin_filter_below_join(left_pushed_pred, filter_pipe_var, join_expr, 0);
	  if (right_pushed_pred.size() > 0) 
		  plugin_filter_below_join(right_pushed_pred, filter_pipe_var, join_expr, 1);
	  
	  return true;
  }

  
  /**
   * Push the filter below the child expression without restrictions, i.e., including all the predicates.
   * The child_ids are specified in the given array 'child_ids'.
   */
  private boolean filterDirectpushRewrite(FilterExpr filter_expr, ArrayList<Integer> child_ids) 
  {
	  Expr input_expr = filter_expr.binding().inExpr();
	  Var filter_pipe_var = filter_expr.binding().var;
	  ArrayList<Expr> pred_list = filter_expr.conjunctivePredList();
	  
	  //pluginFilterBelowExpr(filter_expr.conjunctivePredList(), filter_pipe_var, input_expr, child_ids);
	  for (int i  = 0 ; i < child_ids.size(); i++)
	  {
		  Var newVar = new Var(filter_pipe_var.name());
		  VarMap vm = new VarMap();
		  vm.put(filter_pipe_var, newVar);
		  
		  //Clone the predicates 
		  ArrayList<Expr> predClone_list = new ArrayList<Expr>();
		  for (int j = 0; j < pred_list.size(); j++)
			  predClone_list.add(pred_list.get(j).clone(vm));
		  
		  //Create the new filter
		  Expr branch = input_expr.child(child_ids.get(i));
		  BindingExpr new_filter_input = new BindingExpr(Type.IN, newVar, null, branch);
		  FilterExpr new_fe = new FilterExpr(new_filter_input, predClone_list);
		  //new_fe.replaceVarInPredicates(filter_pipe_var, new_filter_input.var);
		  input_expr.setChild(child_ids.get(i), new_fe);
	  }
	  
	  filter_expr.replaceInParent(input_expr);
	  return true;
  }

  
  /**
   * Try to push Filter below the GroupBy
   * A filter predicate can be pushed below the GroupBy if it references only the grouping keys. 
   * If there are multiple inputs to the GroupBy, then a predicate is pushed down only if it can be pushed over all child branches.
   */
  private boolean filterGroupbyRewrite(FilterExpr filter_expr) 
  {
	  GroupByExpr grp_expr = (GroupByExpr) filter_expr.binding().inExpr();
	  Var filter_pipe_var = filter_expr.binding().var;

	  //If the "filter" or the "group by" is non-deterministic, then do not push the filter
	  if (grp_expr.externalEffectIntoClause())
		  return false;

	  //get the Group By mapping tables
	  int grp_childNum = grp_expr.byBinding().numChildren();
	  MappingTable into_mt = grp_expr.getMappingTable(grp_childNum);         //get the mapping table of the INTO clause
	  MappingTable[] by_mts = new MappingTable[grp_childNum];
	  for (int i = 0; i < grp_childNum; i++)
		  by_mts[i] = grp_expr.getMappingTable(i);                          //get the mapping table(s) of the BY cluase(s) 

	 if (into_mt.replaceVarInAfterExpr(filter_pipe_var) == false)
		 return false;

	  ArrayList<Expr> pushed_pred = new ArrayList<Expr>();              //Pred. that will be removed from the Filter
	  ArrayList<Expr>[] pushed_to_child = new ArrayList[grp_childNum];  //Pred. that will be pushed to each child
	  for (int i = 0; i < grp_childNum; i++)
		  pushed_to_child[i] = new ArrayList<Expr>();
	  
	  //Check each conjunctive predicate. A predicate can be pushed down only if it can be pushed in ALL child branches.
	  VarMap vm = new VarMap();
	  for (int k = 0; k < filter_expr.conjunctivePred_count(); k++)
	  {
		  Expr crnt_pred = filter_expr.conjunctivePred(k);
		  ArrayList<Expr> usedIn_list = findMaximalVarOrPathExpr(crnt_pred, filter_pipe_var);  
		  if (usedIn_list.size() == 0)
		  {
			  //Predicate will be removed from the Filter and pushed to all child branches.	
			  pushed_pred.add(crnt_pred);
			  for (int i = 0; i < grp_childNum; i++)
				  pushed_to_child[i].add(crnt_pred.clone(vm));
			  continue;
		  }
		  
		  //check if the usage of the piped var can be mapped through both the INTO clause and the BY clause.
		  ArrayList<Expr> mappedTo_list1 = predMappedTo(usedIn_list, into_mt, true);
		  if (mappedTo_list1 == null)
			  continue;

		  //check if the predicate can be pushed through all child branches.
		  ArrayList<Expr>[] mappedTo_list2 = new ArrayList[grp_childNum];
		  for (int i = 0; i < grp_childNum; i++)
			  mappedTo_list2[i] = new ArrayList<Expr>();
		  
		  boolean can_be_pushed = true;
		  for (int i = 0; i < grp_childNum; i++)
		  {
			  mappedTo_list2[i] = predMappedTo(mappedTo_list1, by_mts[i], true);
			  if (mappedTo_list2[i] == null)
				  can_be_pushed = false;			  
		  }
		  if (!can_be_pushed)
			  continue;
		  
		  //Clone the crnt_pred and push it down over each child
		  for (int i = 0; i < grp_childNum; i++)
		  {
			  Expr crntPredClone = crnt_pred.clone(vm);
			  ArrayList<Expr> usedIn_list_clone = findMaximalVarOrPathExpr(crntPredClone, filter_pipe_var);  
			  
			  //change_predicate() has the side effect of changing "crnt_pred"
			  //Warning: It also make the query tree in-consistent at this moment
			  Expr modifiedPred = changePredicate(crntPredClone, usedIn_list_clone, mappedTo_list2[i], grp_expr.inVar(), filter_pipe_var);         
			  pushed_to_child[i].add(modifiedPred);			  
		  }		  
		  pushed_pred.add(crnt_pred);
	  }	
	  if (pushed_pred.size() == 0)
		  return false;

	  //Remove the pushed predicates from the main Filter. If it becomes empty, then delete this filter
	  for (int i = 0; i < pushed_pred.size(); i++)
		  pushed_pred.get(i).detach();
	  if  (filter_expr.conjunctivePred_count() == 0)
		  filter_expr.replaceInParent(filter_expr.binding().inExpr());


	  //Push the predicates to each of the groupBy children
	  BindingExpr grp_input = grp_expr.inBinding();
	  int child_cnt = grp_input.numChildren();	  
	  for (int i = 0; i < child_cnt; i++)
	  {
		  BindingExpr new_filter_input = new BindingExpr(Type.IN, new Var(filter_pipe_var.name()), null, grp_input.child(i));
		  FilterExpr new_fe = new FilterExpr(new_filter_input, pushed_to_child[i]);
		  grp_input.setChild(i, new_fe);

		  //Make sure the VARs under the FilterExpr are correct. 
		  replaceVarInFilter(new_fe, filter_pipe_var, new_filter_input.var);
	  }

	  return true;
  }

  /**
   * Try to push Filter below the Expand (For) expression.
   * Filter can be always pushed inside the expand expression. However, in case of 'expand unroll' we may push predicates before (not inside) the expand.
   */
  private boolean filterExpandRewrite(FilterExpr filter_expr) 
  {
	  ForExpr for_expr = (ForExpr) filter_expr.binding().inExpr();
	  Var filter_pipe_var = filter_expr.binding().var;
	  boolean is_unroll = (for_expr.collectExpr() instanceof UnrollExpr);
	  boolean is_unroll_over_pathExpr = true;
	  String unroll_str = "";
	  
	  if (is_unroll)
	  {
		  //We only handle the case where the unrolled expression is pathExr
		  UnrollExpr ue = (UnrollExpr)for_expr.collectExpr(); 
		  if (!(ue.child(0) instanceof VarExpr))
			  is_unroll_over_pathExpr = false;
		  else
		  {
			  unroll_str = filter_pipe_var.name();
			  for(int i = 1; i < ue.numChildren(); i++)
			  {
				  if ((ue.child(i) instanceof UnrollField) && ((ue.child(i)).numChildren() == 1) && ((ue.child(i)).child(0) instanceof ConstExpr))
				  {
					  unroll_str = unroll_str + "." + (ue.child(i).child(0)).toString();
					  continue;
				  }
				  is_unroll_over_pathExpr = false;
				  break;
			  }
		  }
	  }
	  
	  if ((!is_unroll) || !is_unroll_over_pathExpr)
	  {
		  //Push the filter inside the expand.
		  ArrayList<Integer> child_ids = new ArrayList<Integer>();
		  child_ids.add(1);
		  return filterDirectpushRewrite(filter_expr, child_ids);		  
	  }
	  
	  //We have 'Expand Unroll', so if a filter predicate is on attribute(s) other than the unrolled one, 
	  // then the predicate can be pushed before (not inside) the Expand. 
	  //Predicates that cannot be pushed before the Expand will be pushed inside the expand.
	  ArrayList<Expr> pushed_pred = new ArrayList<Expr>();
	  for (int i = 0; i < filter_expr.conjunctivePred_count(); i++)
	  {
		  Expr pred = filter_expr.conjunctivePred(i);
		  ArrayList<Expr> usedIn_list = findMaximalVarOrPathExpr(pred, filter_pipe_var);  
		  if (usedIn_list.size() == 0)
		  {
			  pushed_pred.add(pred);
			  continue;
		  }
		  
		  //make sure the unrolled expression is not part of the predicate
		  String pred_str = pred.toString().replace("(", "").replace(")", "");
		  if (!pred_str.contains(unroll_str))
			  pushed_pred.add(pred);
	  }
	  
	  //Remove the pushed predicates from the main Filter. 
	  for (int i = 0; i < pushed_pred.size(); i++)
		  pushed_pred.get(i).detach();	 
	  
	  //Push the predicates before the expand
	  if (pushed_pred.size() > 0)
	  {
		  //plugin_filter_below_expand(pushed_pred, filter_pipe_var, for_expr);
		  BindingExpr for_input = for_expr.binding();	  
		  BindingExpr new_filter_input = new BindingExpr(Type.IN, filter_pipe_var, null, for_input.child(0));
		  FilterExpr new_fe = new FilterExpr(new_filter_input, pushed_pred);
		  for_input.setChild(0, new_fe);

		  //Make sure the VARs under the FilterExpr are correct. 
		  replaceVarInFilter(new_fe, filter_pipe_var, new Var(filter_pipe_var.name()));
	  }
	  
	  //If Filter becomes empty, then delete it. Otherwise push the remaining predicates inside the expand expression. 
	  if  (filter_expr.conjunctivePred_count() == 0)
		  filter_expr.replaceInParent(filter_expr.binding().inExpr());
	  else
	  {
		  ArrayList<Integer> child_ids = new ArrayList<Integer>();
		  child_ids.add(1);
		  filterDirectpushRewrite(filter_expr, child_ids);		  		  
	  }
	  return true;
  }
  
  /**
   * If the expressions in "usedIn_list" can be mapped according to the mapping table "mt", then return the mappings. Otherwise return NULL.
   * If safeMappingOnly = true, then we are looking for safe-mappings only. Otherwise, un-safe mappings are accepted as well. 
   */
  public static ArrayList<Expr> predMappedTo(ArrayList<Expr> usedIn_list, MappingTable mt, boolean safeMappingOnly)
  {
	  ArrayList<Expr> mapped_to_list = new ArrayList<Expr>();

	  //If we managed to do the mapping for all exprs in usedIn_list, then the predicate can be mapped and pushed down
	  for (int i = 0; i < usedIn_list.size(); i++)
	  {
		  Expr pe = usedIn_list.get(i);
		  MappingTable.ExprMapping mapped_to = mt.findPatternMatch(pe, safeMappingOnly);
		  if (mapped_to == null)
			  return null;
		  mapped_to_list.add(mapped_to.getBeforeExpr());
	  }
	return mapped_to_list;
  }
  
  /**
   * Rewrite rule for pushing the filter down in the query tree.
   */
  @Override
  public boolean rewrite(Expr expr)
  {
	  FilterExpr fe = (FilterExpr) expr;
	  BindingExpr filter_input = fe.binding();
	  
	  //If the Filter has side-effect or non-determinism then return.
	  if (fe.externalEffectPredicates())
		  return false;
			  
	  if (filter_input.inExpr() instanceof TransformExpr)
		  return filterTransformRewrite(fe);
	  else if (filter_input.inExpr() instanceof JoinExpr)
		  return filterJoinRewrite(fe);
	  else if (filter_input.inExpr() instanceof GroupByExpr)
		  return filterGroupbyRewrite(fe);
	  else if (filter_input.inExpr() instanceof ForExpr)
		  return filterExpandRewrite(fe);
	  else if (filter_input.inExpr() instanceof SortExpr)
	  {
		  ArrayList<Integer> child_ids = new ArrayList<Integer>();
		  child_ids.add(0);
		  return filterDirectpushRewrite(fe, child_ids);
	  }
	  else if (filter_input.inExpr() instanceof UnionFn)
	  {
	      UnionFn expr_below = (UnionFn) filter_input.inExpr();
		  ArrayList<Integer> child_ids = new ArrayList<Integer>();
		  for (int i = 0; i < expr_below.numChildren(); i++)
			  child_ids.add(i);		  
		  return filterDirectpushRewrite(fe, child_ids);
	  }
	  return false;
  }
}
