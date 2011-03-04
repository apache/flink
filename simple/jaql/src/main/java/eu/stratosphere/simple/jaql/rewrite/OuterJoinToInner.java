package eu.stratosphere.simple.jaql.rewrite;

import java.util.ArrayList;

import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.array.ExistsFn;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.CompareExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.FilterExpr;
import com.ibm.jaql.lang.expr.core.JoinExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.metadata.MappingTable;
import com.ibm.jaql.lang.expr.path.PathExpr;
import com.ibm.jaql.lang.rewrite.FilterPushDown;
import com.ibm.jaql.lang.rewrite.RewritePhase;

/**
 * If the join is outer join, i.e., at least one of the two children has 'preserve' flag set,
 * then the 'preserve' flag can be turned off iff there is a "null-rejecting" condition of the other child.
 * "null-rejection" predicate: a predicate that always returns false if the input is null.
 * Example:
 * 			e1 join e2(preserve) -> filter <null-rejecting predicate of e1 attributes>  
 * 		--> 
 * 			e1 join e2 -> filter <predicate of e1 attributes>
 * In the example above, e1 may or may not have the 'preserve' flag set.
 */
public class OuterJoinToInner extends Rewrite
{
  public OuterJoinToInner(RewritePhase phase)
  {
    super(phase, JoinExpr.class);
  }

  
  /**
   * Returns true if 'filter_expr' has at least one "null-rejecting" predicate (pred. that rejects NULLs) over 'input' expression.
   */
  private boolean nullRejectingPredicate(FilterExpr filter_expr, Expr input) 
  {
	  MappingTable mt = input.getMappingTable();  
	  Var filter_pipe_var = filter_expr.binding().var;
	  if (mt.replaceVarInAfterExpr(filter_pipe_var) == false)
		  return false;

	  //Loop over the filter predicates and check if any of them is "null-rejecting" over 'input' expr.
	  for (int i = 0; i < filter_expr.conjunctivePred_count(); i++)
	  {
		  Expr pred = filter_expr.conjunctivePred(i);
	  
		  //Consider different types of predicates 
		  if (pred instanceof CompareExpr)
		  {
			  Expr left_side = pred.child(0);
			  Expr right_side = pred.child(1);
			  if ((left_side instanceof PathExpr) || (left_side instanceof VarExpr))
			  {
				  ArrayList<Expr> usedIn_list = findMaximalVarOrPathExpr(left_side, filter_pipe_var);  
				  if (usedIn_list.size() > 0)
				  {
					  ArrayList<Expr> mappedTo_list = FilterPushDown.predMappedTo(usedIn_list, mt, false);		  
					  if (mappedTo_list != null)
					  {
						  //This predicate is "null-rejecting" predicate over 'input' expression
						  return true;
					  }
				  }
			  }

			  if ((right_side instanceof PathExpr) || (right_side instanceof VarExpr))
			  {
				  ArrayList<Expr> usedIn_list = findMaximalVarOrPathExpr(right_side, filter_pipe_var);  
				  if (usedIn_list.size() > 0)
				  {
					  ArrayList<Expr> mappedTo_list = FilterPushDown.predMappedTo(usedIn_list, mt, false);		  
					  if (mappedTo_list != null)
					  {
						  //This predicate is "null-rejecting" predicate over 'input' expression
						  return true;
					  }
				  }
			  }			  
		  }
		  else if (pred instanceof ExistsFn)
		  {
			  Expr child = pred.child(1);
			  if ((child instanceof PathExpr) || (child instanceof VarExpr))
			  {
				  ArrayList<Expr> usedIn_list = findMaximalVarOrPathExpr(child, filter_pipe_var);  
				  if (usedIn_list.size() > 0)
				  {
					  ArrayList<Expr> mappedTo_list = FilterPushDown.predMappedTo(usedIn_list, mt, false);		  
					  if (mappedTo_list != null)
					  {
						  //This predicate is "null-rejecting" predicate over 'input' expression
						  return true;
					  }
				  }
			  }			  
		  }
	  }
	  return false;
  }

  private boolean OuterToInnerConversion(JoinExpr join_expr, FilterExpr filter_expr) 
  {
	  boolean left_preserve = join_expr.binding(0).preserve;
	  boolean right_preserve = join_expr.binding(1).preserve;
	  Expr left_child = join_expr.binding(0).inExpr();
	  Expr right_child = join_expr.binding(1).inExpr();

	  boolean turned_off = false;
	  if (left_preserve)
	  {
		  if (nullRejectingPredicate(filter_expr, right_child))
		  {
			  join_expr.binding(0).preserve = false;
			  turned_off = true;
		  }
	  }
	  
	  if (right_preserve)
	  {
		  if (nullRejectingPredicate(filter_expr, left_child))
		  {
			  join_expr.binding(1).preserve = false;
			  turned_off = true;
		  }
	  }
	  return turned_off;
  }
  

/**
   * If the join is outer join, i.e., at least one of the two children has 'preserve' flag set,
   * then the 'preserve' flag can be turned off iff there is a "null-rejecting" condition (condition that rejects NULLs) of the other child. 
   */
  @Override
  public boolean rewrite(Expr expr)
  {
	  JoinExpr join_expr = (JoinExpr) expr;
	  
	  //We are limited right now to 2-way joins
	  if (join_expr.numBindings() > 2)
		  return false;
	  
	  Expr parent = join_expr.parent();
	  boolean left_preserve = join_expr.binding(0).preserve;
	  boolean right_preserve = join_expr.binding(1).preserve;
	  
	  //Exit if the join is inner join or the join's parent is not FilterExpr
	  if (!left_preserve && !right_preserve)
		  return false;	  
	  if (!(parent instanceof BindingExpr) || !(parent.parent() instanceof FilterExpr))
		  return false;

	  FilterExpr fe = (FilterExpr) parent.parent();	  
	  if (fe.externalEffectPredicates())
		  return false;
	  
	  return OuterToInnerConversion(join_expr, fe);
  }
}