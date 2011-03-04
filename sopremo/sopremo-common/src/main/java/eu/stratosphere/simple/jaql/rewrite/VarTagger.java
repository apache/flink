/*
 * Copyright (C) IBM Corp. 2008.
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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.util.JaqlUtil;

/**
 * Provides methods for tagging variables of the same name so as to avoid problems in 
 * decompilation. Has to be called before decompilation.
 */
public class VarTagger 
{
  /** In <code>e</code>, tags variables that are referred to while a different variable of the 
   * same name is in scope. This makes decompilation of <code>e</code> is safe. Expression
   * <code>e</code> must not have any captured, non-global variables. */
  public static void tag(Expr e)
  {
    Map<String, Stack<Var>> scope = new HashMap<String, Stack<Var>>();
    Set<Var> varsToTag = tag(e, scope, new NumberTagGenerator());
    if (!varsToTag.isEmpty()) 
    {
      // only happens when e has non-global captures that should be tagged
      // in this case, tag(Expr, Map<String, Stack<Var>>) should have been used
      throw new IllegalArgumentException("rebinding of variables failed");
    }
  }
  
  /** In <code>e</code>, tags variables that are referred to while a different variable of the 
   * same name is in scope. Returns the set of captured variables in <code>e</code> that have to 
   * be tagged */
  public static Set<Var> tag(Expr e, Map<String, Stack<Var>> scope, TagGenerator tagGenerator)
  {
    Map<Var, BindingExpr> myVars = new HashMap<Var, BindingExpr>();
    Set<Var> varsToTag = new HashSet<Var>();
    
    // traverse the subtree and look for variable definitions and usages
    for (int i=0; i<e.numChildren(); i++)
    {
      Expr child = e.child(i);
      varsToTag.addAll(tag(child, scope, tagGenerator));
      
      // check for variable definitions
      if (child instanceof BindingExpr)
      {
        BindingExpr be = (BindingExpr)child;
        Var var = be.var;
        
        // add the variable to the list of variables of this expression 
        assert !myVars.containsKey(var);
        myVars.put(var, be);
        
        // add the variable to the scope
        scope(var, scope);
      }
      
      // check for variable use
      if (child instanceof VarExpr)
      {
        VarExpr ve = (VarExpr)child;
        Var var = ve.var();
        
        // ignore global variables
        if (!var.isGlobal())
        {
          String varName = var.name();
          Stack<Var> bindings = scope.get(varName);
          if (bindings != null && !bindings.isEmpty())
          {
            Var topVar = bindings.peek();
            if (topVar != var)
            {
              // has to be tagged
              varsToTag.add(var);            
            }
          }
          else
          {
            // free variable; that's OK 
          }
          
          if (JaqlUtil.isKeyword(var.name()))
          {
            // all keyword variable will be tagged
            varsToTag.add(var);
          }
        }
      }
    }
    
    // unscope my variables and retain only those that have to be tagged 
    Iterator<Map.Entry<Var, BindingExpr>> myVarsIt = myVars.entrySet().iterator();
    while (myVarsIt.hasNext())
    {
      Var var = myVarsIt.next().getKey();
      Stack<Var> bindings = scope.get(var.name());
      bindings.pop();
      if (!varsToTag.contains(var))
      {
        myVarsIt.remove();
        var.setTag(null);
      }
      else
      {
        varsToTag.remove(var); // tagged below
      }
    }
    
    // tag my remaining variables
    if (!myVars.isEmpty())
    {
      for (Var var : myVars.keySet())
      {
        var.setTag(tagGenerator.nextTag());
      }
    }
    
    // remaining vars are tagged somewhere above this node
    return varsToTag;
  }
  
  /** Scopes a variable */
  public static void scope(Var var, Map<String, Stack<Var>> scope)
  {
    String varName = var.name();
    Stack<Var> bindings = scope.get(varName);
    if (bindings == null)
    {
      bindings = new Stack<Var>();
      scope.put(varName, bindings);
    }
    bindings.push(var);
  }
  
  /** Generates a new unique tag */
  public interface TagGenerator
  {
    public String nextTag();
  }
  
  /** Tag generator for numeric tags. Generates sequence 0, 1, 2, ... */
  public static class NumberTagGenerator implements TagGenerator
  {
    int i;
    
    public NumberTagGenerator(int start)
    {
      this.i = start;
    }
    
    public NumberTagGenerator()
    {
      this(0);
    }
    
    public String nextTag()
    {
      return Integer.toString(i++);      
    }
  }
}
