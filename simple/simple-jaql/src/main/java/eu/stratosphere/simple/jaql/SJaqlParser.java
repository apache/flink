// $ANTLR 3.3 Nov 30, 2010 12:46:29 /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g 2011-10-24 00:07:40
 
package eu.stratosphere.simple.jaql; 

import eu.stratosphere.sopremo.*;
import eu.stratosphere.util.*;
import eu.stratosphere.simple.*;
import eu.stratosphere.sopremo.pact.*;
import eu.stratosphere.sopremo.type.*;
import eu.stratosphere.sopremo.expressions.*;
import eu.stratosphere.sopremo.aggregation.*;
import eu.stratosphere.sopremo.function.*;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.*;
import java.math.*;
import java.util.Arrays;


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import org.antlr.runtime.tree.*;

public class SJaqlParser extends SimpleParser {
    public static final String[] tokenNames = new String[] {
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "EXPRESSION", "OPERATOR", "ID", "VAR", "STRING", "STAR", "DECIMAL", "INTEGER", "UINT", "LOWER_LETTER", "UPPER_LETTER", "DIGIT", "SIGN", "COMMENT", "APOSTROPHE", "QUOTATION", "WS", "UNICODE_ESC", "OCTAL_ESC", "ESC_SEQ", "HEX_DIGIT", "EXPONENT", "';'", "'using'", "'='", "'fn'", "'('", "','", "')'", "'javaudf'", "'?'", "':'", "'if'", "'or'", "'||'", "'and'", "'&&'", "'not'", "'in'", "'<='", "'>='", "'<'", "'>'", "'=='", "'!='", "'+'", "'-'", "'/'", "'++'", "'--'", "'!'", "'~'", "'as'", "'.'", "'{'", "'}'", "'true'", "'false'", "'null'", "'['", "']'", "'read'", "'write'", "'to'", "'preserve'"
    };
    public static final int EOF=-1;
    public static final int T__26=26;
    public static final int T__27=27;
    public static final int T__28=28;
    public static final int T__29=29;
    public static final int T__30=30;
    public static final int T__31=31;
    public static final int T__32=32;
    public static final int T__33=33;
    public static final int T__34=34;
    public static final int T__35=35;
    public static final int T__36=36;
    public static final int T__37=37;
    public static final int T__38=38;
    public static final int T__39=39;
    public static final int T__40=40;
    public static final int T__41=41;
    public static final int T__42=42;
    public static final int T__43=43;
    public static final int T__44=44;
    public static final int T__45=45;
    public static final int T__46=46;
    public static final int T__47=47;
    public static final int T__48=48;
    public static final int T__49=49;
    public static final int T__50=50;
    public static final int T__51=51;
    public static final int T__52=52;
    public static final int T__53=53;
    public static final int T__54=54;
    public static final int T__55=55;
    public static final int T__56=56;
    public static final int T__57=57;
    public static final int T__58=58;
    public static final int T__59=59;
    public static final int T__60=60;
    public static final int T__61=61;
    public static final int T__62=62;
    public static final int T__63=63;
    public static final int T__64=64;
    public static final int T__65=65;
    public static final int T__66=66;
    public static final int T__67=67;
    public static final int T__68=68;
    public static final int EXPRESSION=4;
    public static final int OPERATOR=5;
    public static final int ID=6;
    public static final int VAR=7;
    public static final int STRING=8;
    public static final int STAR=9;
    public static final int DECIMAL=10;
    public static final int INTEGER=11;
    public static final int UINT=12;
    public static final int LOWER_LETTER=13;
    public static final int UPPER_LETTER=14;
    public static final int DIGIT=15;
    public static final int SIGN=16;
    public static final int COMMENT=17;
    public static final int APOSTROPHE=18;
    public static final int QUOTATION=19;
    public static final int WS=20;
    public static final int UNICODE_ESC=21;
    public static final int OCTAL_ESC=22;
    public static final int ESC_SEQ=23;
    public static final int HEX_DIGIT=24;
    public static final int EXPONENT=25;

    // delegates
    // delegators


        public SJaqlParser(TokenStream input) {
            this(input, new RecognizerSharedState());
        }
        public SJaqlParser(TokenStream input, RecognizerSharedState state) {
            super(input, state);
            this.state.ruleMemo = new HashMap[117+1];
             
             
        }
        
    protected TreeAdaptor adaptor = new CommonTreeAdaptor();

    public void setTreeAdaptor(TreeAdaptor adaptor) {
        this.adaptor = adaptor;
    }
    public TreeAdaptor getTreeAdaptor() {
        return adaptor;
    }

    public String[] getTokenNames() { return SJaqlParser.tokenNames; }
    public String getGrammarFileName() { return "/Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g"; }


    {
      addTypeAlias("int", IntNode.class);
      addTypeAlias("decimal", DecimalNode.class);
      addTypeAlias("string", TextNode.class);
      addTypeAlias("double", DoubleNode.class);
      addTypeAlias("boolean", BooleanNode.class);
      addTypeAlias("bool", BooleanNode.class);
    }

    public void parseSinks() throws RecognitionException {  
        script();
    }

    private EvaluationExpression makePath(Token inputVar, String... path) {
      List<EvaluationExpression> accesses = new ArrayList<EvaluationExpression>();

      int inputIndex = inputIndexForBinding(inputVar);
      if(inputIndex == -1) {
        if(!inputVar.getText().equals("$"))
          accesses.add(new JsonStreamExpression(getBinding(inputVar, Operator.class)));
      } else {
        InputSelection inputSelection = new InputSelection(inputIndex);
        for (ExpressionTag tag : ((operator_scope)operator_stack.peek()).inputTags.get(inputIndex))
          inputSelection.addTag(tag);
        accesses.add(inputSelection);
      }

      for (String fragment : path)
        accesses.add(new ObjectAccess(fragment));

      return PathExpression.valueOf(accesses);
    }

    private int inputIndexForBinding(Token variable) {
      int index = ((operator_scope)operator_stack.peek()).inputNames.indexOf(variable.getText());
      if(index == -1) {
        if(variable.getText().equals("$") && ((operator_scope)operator_stack.peek()).inputNames.size() == 1 && !((operator_scope)operator_stack.peek()).hasExplicitName.get(0))
          return 0;
        try {
          index = Integer.parseInt(variable.getText().substring(1));
          if(((operator_scope)operator_stack.peek()).hasExplicitName.get(index))
            throw new SimpleException("Cannot use index variable for input with explicit name", variable);
          if(0 > index || index >= ((operator_scope)operator_stack.peek()).inputNames.size()) 
            throw new SimpleException("Invalid input index", variable);
        } catch(NumberFormatException e) {
        }
      }
      return index;
    }


    public static class script_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "script"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:92:1: script : statement ( ';' statement )* ';' ->;
    public final SJaqlParser.script_return script() throws RecognitionException {
        SJaqlParser.script_return retval = new SJaqlParser.script_return();
        retval.start = input.LT(1);
        int script_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token char_literal2=null;
        Token char_literal4=null;
        SJaqlParser.statement_return statement1 = null;

        SJaqlParser.statement_return statement3 = null;


        EvaluationExpression char_literal2_tree=null;
        EvaluationExpression char_literal4_tree=null;
        RewriteRuleTokenStream stream_26=new RewriteRuleTokenStream(adaptor,"token 26");
        RewriteRuleSubtreeStream stream_statement=new RewriteRuleSubtreeStream(adaptor,"rule statement");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 1) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:93:2: ( statement ( ';' statement )* ';' ->)
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:93:5: statement ( ';' statement )* ';'
            {
            pushFollow(FOLLOW_statement_in_script124);
            statement1=statement();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_statement.add(statement1.getTree());
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:93:15: ( ';' statement )*
            loop1:
            do {
                int alt1=2;
                int LA1_0 = input.LA(1);

                if ( (LA1_0==26) ) {
                    int LA1_1 = input.LA(2);

                    if ( ((LA1_1>=ID && LA1_1<=VAR)||LA1_1==27||(LA1_1>=65 && LA1_1<=66)) ) {
                        alt1=1;
                    }


                }


                switch (alt1) {
            	case 1 :
            	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:93:16: ';' statement
            	    {
            	    char_literal2=(Token)match(input,26,FOLLOW_26_in_script127); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_26.add(char_literal2);

            	    pushFollow(FOLLOW_statement_in_script129);
            	    statement3=statement();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_statement.add(statement3.getTree());

            	    }
            	    break;

            	default :
            	    break loop1;
                }
            } while (true);

            char_literal4=(Token)match(input,26,FOLLOW_26_in_script133); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_26.add(char_literal4);



            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 93:36: ->
            {
                root_0 = null;
            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 1, script_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "script"

    public static class statement_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "statement"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:95:1: statement : ( assignment | operator | packageImport | functionDefinition | javaudf ) ->;
    public final SJaqlParser.statement_return statement() throws RecognitionException {
        SJaqlParser.statement_return retval = new SJaqlParser.statement_return();
        retval.start = input.LT(1);
        int statement_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        SJaqlParser.assignment_return assignment5 = null;

        SJaqlParser.operator_return operator6 = null;

        SJaqlParser.packageImport_return packageImport7 = null;

        SJaqlParser.functionDefinition_return functionDefinition8 = null;

        SJaqlParser.javaudf_return javaudf9 = null;


        RewriteRuleSubtreeStream stream_assignment=new RewriteRuleSubtreeStream(adaptor,"rule assignment");
        RewriteRuleSubtreeStream stream_functionDefinition=new RewriteRuleSubtreeStream(adaptor,"rule functionDefinition");
        RewriteRuleSubtreeStream stream_javaudf=new RewriteRuleSubtreeStream(adaptor,"rule javaudf");
        RewriteRuleSubtreeStream stream_operator=new RewriteRuleSubtreeStream(adaptor,"rule operator");
        RewriteRuleSubtreeStream stream_packageImport=new RewriteRuleSubtreeStream(adaptor,"rule packageImport");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 2) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:96:2: ( ( assignment | operator | packageImport | functionDefinition | javaudf ) ->)
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:96:4: ( assignment | operator | packageImport | functionDefinition | javaudf )
            {
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:96:4: ( assignment | operator | packageImport | functionDefinition | javaudf )
            int alt2=5;
            switch ( input.LA(1) ) {
            case VAR:
                {
                alt2=1;
                }
                break;
            case 65:
            case 66:
                {
                alt2=2;
                }
                break;
            case ID:
                {
                int LA2_3 = input.LA(2);

                if ( (LA2_3==28) ) {
                    int LA2_5 = input.LA(3);

                    if ( (LA2_5==29) ) {
                        alt2=4;
                    }
                    else if ( (LA2_5==33) ) {
                        alt2=5;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 2, 5, input);

                        throw nvae;
                    }
                }
                else if ( ((LA2_3>=ID && LA2_3<=VAR)||LA2_3==68) ) {
                    alt2=2;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 2, 3, input);

                    throw nvae;
                }
                }
                break;
            case 27:
                {
                alt2=3;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 2, 0, input);

                throw nvae;
            }

            switch (alt2) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:96:5: assignment
                    {
                    pushFollow(FOLLOW_assignment_in_statement145);
                    assignment5=assignment();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_assignment.add(assignment5.getTree());

                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:96:18: operator
                    {
                    pushFollow(FOLLOW_operator_in_statement149);
                    operator6=operator();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_operator.add(operator6.getTree());

                    }
                    break;
                case 3 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:96:29: packageImport
                    {
                    pushFollow(FOLLOW_packageImport_in_statement153);
                    packageImport7=packageImport();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_packageImport.add(packageImport7.getTree());

                    }
                    break;
                case 4 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:96:45: functionDefinition
                    {
                    pushFollow(FOLLOW_functionDefinition_in_statement157);
                    functionDefinition8=functionDefinition();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_functionDefinition.add(functionDefinition8.getTree());

                    }
                    break;
                case 5 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:96:66: javaudf
                    {
                    pushFollow(FOLLOW_javaudf_in_statement161);
                    javaudf9=javaudf();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_javaudf.add(javaudf9.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 96:75: ->
            {
                root_0 = null;
            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 2, statement_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "statement"

    public static class packageImport_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "packageImport"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:98:1: packageImport : 'using' packageName= ID ->;
    public final SJaqlParser.packageImport_return packageImport() throws RecognitionException {
        SJaqlParser.packageImport_return retval = new SJaqlParser.packageImport_return();
        retval.start = input.LT(1);
        int packageImport_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token packageName=null;
        Token string_literal10=null;

        EvaluationExpression packageName_tree=null;
        EvaluationExpression string_literal10_tree=null;
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_27=new RewriteRuleTokenStream(adaptor,"token 27");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 3) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:99:3: ( 'using' packageName= ID ->)
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:99:6: 'using' packageName= ID
            {
            string_literal10=(Token)match(input,27,FOLLOW_27_in_packageImport176); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_27.add(string_literal10);

            packageName=(Token)match(input,ID,FOLLOW_ID_in_packageImport180); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(packageName);

            if ( state.backtracking==0 ) {
               importPackage((packageName!=null?packageName.getText():null)); 
            }


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 99:66: ->
            {
                root_0 = null;
            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 3, packageImport_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "packageImport"

    public static class assignment_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "assignment"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:101:1: assignment : target= VAR '=' source= operator ->;
    public final SJaqlParser.assignment_return assignment() throws RecognitionException {
        SJaqlParser.assignment_return retval = new SJaqlParser.assignment_return();
        retval.start = input.LT(1);
        int assignment_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token target=null;
        Token char_literal11=null;
        SJaqlParser.operator_return source = null;


        EvaluationExpression target_tree=null;
        EvaluationExpression char_literal11_tree=null;
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_28=new RewriteRuleTokenStream(adaptor,"token 28");
        RewriteRuleSubtreeStream stream_operator=new RewriteRuleSubtreeStream(adaptor,"rule operator");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 4) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:102:2: (target= VAR '=' source= operator ->)
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:102:4: target= VAR '=' source= operator
            {
            target=(Token)match(input,VAR,FOLLOW_VAR_in_assignment195); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_VAR.add(target);

            char_literal11=(Token)match(input,28,FOLLOW_28_in_assignment197); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_28.add(char_literal11);

            pushFollow(FOLLOW_operator_in_assignment201);
            source=operator();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_operator.add(source.getTree());
            if ( state.backtracking==0 ) {
               setBinding(target, (source!=null?source.op:null)); 
            }


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 102:72: ->
            {
                root_0 = null;
            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 4, assignment_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "assignment"

    public static class functionDefinition_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "functionDefinition"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:104:1: functionDefinition : name= ID '=' 'fn' '(' (param= ID ( ',' param= ID )* )? ')' def= contextAwareExpression[null] ->;
    public final SJaqlParser.functionDefinition_return functionDefinition() throws RecognitionException {
        SJaqlParser.functionDefinition_return retval = new SJaqlParser.functionDefinition_return();
        retval.start = input.LT(1);
        int functionDefinition_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token name=null;
        Token param=null;
        Token char_literal12=null;
        Token string_literal13=null;
        Token char_literal14=null;
        Token char_literal15=null;
        Token char_literal16=null;
        SJaqlParser.contextAwareExpression_return def = null;


        EvaluationExpression name_tree=null;
        EvaluationExpression param_tree=null;
        EvaluationExpression char_literal12_tree=null;
        EvaluationExpression string_literal13_tree=null;
        EvaluationExpression char_literal14_tree=null;
        EvaluationExpression char_literal15_tree=null;
        EvaluationExpression char_literal16_tree=null;
        RewriteRuleTokenStream stream_30=new RewriteRuleTokenStream(adaptor,"token 30");
        RewriteRuleTokenStream stream_32=new RewriteRuleTokenStream(adaptor,"token 32");
        RewriteRuleTokenStream stream_31=new RewriteRuleTokenStream(adaptor,"token 31");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_28=new RewriteRuleTokenStream(adaptor,"token 28");
        RewriteRuleTokenStream stream_29=new RewriteRuleTokenStream(adaptor,"token 29");
        RewriteRuleSubtreeStream stream_contextAwareExpression=new RewriteRuleSubtreeStream(adaptor,"rule contextAwareExpression");
         List<Token> params = new ArrayList(); 
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 5) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:106:3: (name= ID '=' 'fn' '(' (param= ID ( ',' param= ID )* )? ')' def= contextAwareExpression[null] ->)
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:106:5: name= ID '=' 'fn' '(' (param= ID ( ',' param= ID )* )? ')' def= contextAwareExpression[null]
            {
            name=(Token)match(input,ID,FOLLOW_ID_in_functionDefinition223); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);

            char_literal12=(Token)match(input,28,FOLLOW_28_in_functionDefinition225); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_28.add(char_literal12);

            string_literal13=(Token)match(input,29,FOLLOW_29_in_functionDefinition227); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_29.add(string_literal13);

            char_literal14=(Token)match(input,30,FOLLOW_30_in_functionDefinition229); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_30.add(char_literal14);

            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:107:3: (param= ID ( ',' param= ID )* )?
            int alt4=2;
            int LA4_0 = input.LA(1);

            if ( (LA4_0==ID) ) {
                alt4=1;
            }
            switch (alt4) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:107:4: param= ID ( ',' param= ID )*
                    {
                    param=(Token)match(input,ID,FOLLOW_ID_in_functionDefinition238); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(param);

                    if ( state.backtracking==0 ) {
                       params.add(param); 
                    }
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:108:3: ( ',' param= ID )*
                    loop3:
                    do {
                        int alt3=2;
                        int LA3_0 = input.LA(1);

                        if ( (LA3_0==31) ) {
                            alt3=1;
                        }


                        switch (alt3) {
                    	case 1 :
                    	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:108:4: ',' param= ID
                    	    {
                    	    char_literal15=(Token)match(input,31,FOLLOW_31_in_functionDefinition245); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_31.add(char_literal15);

                    	    param=(Token)match(input,ID,FOLLOW_ID_in_functionDefinition249); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_ID.add(param);

                    	    if ( state.backtracking==0 ) {
                    	       params.add(param); 
                    	    }

                    	    }
                    	    break;

                    	default :
                    	    break loop3;
                        }
                    } while (true);


                    }
                    break;

            }

            char_literal16=(Token)match(input,32,FOLLOW_32_in_functionDefinition260); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_32.add(char_literal16);

            if ( state.backtracking==0 ) {
               for(int index = 0; index < params.size(); index++) setBinding(params.get(index), new InputSelection(0)); 
            }
            pushFollow(FOLLOW_contextAwareExpression_in_functionDefinition272);
            def=contextAwareExpression(null);

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_contextAwareExpression.add(def.getTree());
            if ( state.backtracking==0 ) {
               addFunction(new SopremoFunction(name.getText(), def.tree)); 
            }


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 111:100: ->
            {
                root_0 = null;
            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 5, functionDefinition_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "functionDefinition"

    public static class javaudf_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "javaudf"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:113:1: javaudf : name= ID '=' 'javaudf' '(' path= STRING ')' ->;
    public final SJaqlParser.javaudf_return javaudf() throws RecognitionException {
        SJaqlParser.javaudf_return retval = new SJaqlParser.javaudf_return();
        retval.start = input.LT(1);
        int javaudf_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token name=null;
        Token path=null;
        Token char_literal17=null;
        Token string_literal18=null;
        Token char_literal19=null;
        Token char_literal20=null;

        EvaluationExpression name_tree=null;
        EvaluationExpression path_tree=null;
        EvaluationExpression char_literal17_tree=null;
        EvaluationExpression string_literal18_tree=null;
        EvaluationExpression char_literal19_tree=null;
        EvaluationExpression char_literal20_tree=null;
        RewriteRuleTokenStream stream_30=new RewriteRuleTokenStream(adaptor,"token 30");
        RewriteRuleTokenStream stream_32=new RewriteRuleTokenStream(adaptor,"token 32");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_33=new RewriteRuleTokenStream(adaptor,"token 33");
        RewriteRuleTokenStream stream_STRING=new RewriteRuleTokenStream(adaptor,"token STRING");
        RewriteRuleTokenStream stream_28=new RewriteRuleTokenStream(adaptor,"token 28");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 6) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:114:3: (name= ID '=' 'javaudf' '(' path= STRING ')' ->)
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:114:5: name= ID '=' 'javaudf' '(' path= STRING ')'
            {
            name=(Token)match(input,ID,FOLLOW_ID_in_javaudf290); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);

            char_literal17=(Token)match(input,28,FOLLOW_28_in_javaudf292); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_28.add(char_literal17);

            string_literal18=(Token)match(input,33,FOLLOW_33_in_javaudf294); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_33.add(string_literal18);

            char_literal19=(Token)match(input,30,FOLLOW_30_in_javaudf296); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_30.add(char_literal19);

            path=(Token)match(input,STRING,FOLLOW_STRING_in_javaudf300); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_STRING.add(path);

            char_literal20=(Token)match(input,32,FOLLOW_32_in_javaudf302); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_32.add(char_literal20);

            if ( state.backtracking==0 ) {
               addFunction(name.getText(), path.getText()); 
            }


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 115:53: ->
            {
                root_0 = null;
            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 6, javaudf_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "javaudf"

    protected static class contextAwareExpression_scope {
        EvaluationExpression context;
    }
    protected Stack contextAwareExpression_stack = new Stack();

    public static class contextAwareExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "contextAwareExpression"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:117:1: contextAwareExpression[EvaluationExpression contextExpression] : expression ;
    public final SJaqlParser.contextAwareExpression_return contextAwareExpression(EvaluationExpression contextExpression) throws RecognitionException {
        contextAwareExpression_stack.push(new contextAwareExpression_scope());
        SJaqlParser.contextAwareExpression_return retval = new SJaqlParser.contextAwareExpression_return();
        retval.start = input.LT(1);
        int contextAwareExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        SJaqlParser.expression_return expression21 = null;



         ((contextAwareExpression_scope)contextAwareExpression_stack.peek()).context = contextExpression; 
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 7) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:120:3: ( expression )
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:120:5: expression
            {
            root_0 = (EvaluationExpression)adaptor.nil();

            pushFollow(FOLLOW_expression_in_contextAwareExpression330);
            expression21=expression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, expression21.getTree());

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 7, contextAwareExpression_StartIndex); }
            contextAwareExpression_stack.pop();
        }
        return retval;
    }
    // $ANTLR end "contextAwareExpression"

    public static class expression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "expression"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:122:1: expression : ternaryExpression ;
    public final SJaqlParser.expression_return expression() throws RecognitionException {
        SJaqlParser.expression_return retval = new SJaqlParser.expression_return();
        retval.start = input.LT(1);
        int expression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        SJaqlParser.ternaryExpression_return ternaryExpression22 = null;



        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 8) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:123:3: ( ternaryExpression )
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:123:5: ternaryExpression
            {
            root_0 = (EvaluationExpression)adaptor.nil();

            pushFollow(FOLLOW_ternaryExpression_in_expression340);
            ternaryExpression22=ternaryExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, ternaryExpression22.getTree());

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 8, expression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "expression"

    public static class ternaryExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "ternaryExpression"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:125:1: ternaryExpression : (ifClause= orExpression ( '?' (ifExpr= expression )? ':' elseExpr= expression ) -> ^( EXPRESSION[\"TernaryExpression\"] $ifClause) | ifExpr2= orExpression 'if' ifClause2= expression -> ^( EXPRESSION[\"TernaryExpression\"] $ifClause2 $ifExpr2) | orExpression );
    public final SJaqlParser.ternaryExpression_return ternaryExpression() throws RecognitionException {
        SJaqlParser.ternaryExpression_return retval = new SJaqlParser.ternaryExpression_return();
        retval.start = input.LT(1);
        int ternaryExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token char_literal23=null;
        Token char_literal24=null;
        Token string_literal25=null;
        SJaqlParser.orExpression_return ifClause = null;

        SJaqlParser.expression_return ifExpr = null;

        SJaqlParser.expression_return elseExpr = null;

        SJaqlParser.orExpression_return ifExpr2 = null;

        SJaqlParser.expression_return ifClause2 = null;

        SJaqlParser.orExpression_return orExpression26 = null;


        EvaluationExpression char_literal23_tree=null;
        EvaluationExpression char_literal24_tree=null;
        EvaluationExpression string_literal25_tree=null;
        RewriteRuleTokenStream stream_35=new RewriteRuleTokenStream(adaptor,"token 35");
        RewriteRuleTokenStream stream_36=new RewriteRuleTokenStream(adaptor,"token 36");
        RewriteRuleTokenStream stream_34=new RewriteRuleTokenStream(adaptor,"token 34");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
        RewriteRuleSubtreeStream stream_orExpression=new RewriteRuleSubtreeStream(adaptor,"rule orExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 9) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:126:2: (ifClause= orExpression ( '?' (ifExpr= expression )? ':' elseExpr= expression ) -> ^( EXPRESSION[\"TernaryExpression\"] $ifClause) | ifExpr2= orExpression 'if' ifClause2= expression -> ^( EXPRESSION[\"TernaryExpression\"] $ifClause2 $ifExpr2) | orExpression )
            int alt6=3;
            alt6 = dfa6.predict(input);
            switch (alt6) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:126:4: ifClause= orExpression ( '?' (ifExpr= expression )? ':' elseExpr= expression )
                    {
                    pushFollow(FOLLOW_orExpression_in_ternaryExpression351);
                    ifClause=orExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_orExpression.add(ifClause.getTree());
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:126:26: ( '?' (ifExpr= expression )? ':' elseExpr= expression )
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:126:27: '?' (ifExpr= expression )? ':' elseExpr= expression
                    {
                    char_literal23=(Token)match(input,34,FOLLOW_34_in_ternaryExpression354); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_34.add(char_literal23);

                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:126:37: (ifExpr= expression )?
                    int alt5=2;
                    int LA5_0 = input.LA(1);

                    if ( ((LA5_0>=ID && LA5_0<=STRING)||(LA5_0>=DECIMAL && LA5_0<=UINT)||LA5_0==30||(LA5_0>=52 && LA5_0<=55)||LA5_0==58||(LA5_0>=60 && LA5_0<=63)||(LA5_0>=65 && LA5_0<=66)) ) {
                        alt5=1;
                    }
                    switch (alt5) {
                        case 1 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: ifExpr= expression
                            {
                            pushFollow(FOLLOW_expression_in_ternaryExpression358);
                            ifExpr=expression();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_expression.add(ifExpr.getTree());

                            }
                            break;

                    }

                    char_literal24=(Token)match(input,35,FOLLOW_35_in_ternaryExpression361); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_35.add(char_literal24);

                    pushFollow(FOLLOW_expression_in_ternaryExpression365);
                    elseExpr=expression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_expression.add(elseExpr.getTree());

                    }



                    // AST REWRITE
                    // elements: ifClause
                    // token labels: 
                    // rule labels: retval, ifClause
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
                    RewriteRuleSubtreeStream stream_ifClause=new RewriteRuleSubtreeStream(adaptor,"rule ifClause",ifClause!=null?ifClause.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 127:2: -> ^( EXPRESSION[\"TernaryExpression\"] $ifClause)
                    {
                        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:127:5: ^( EXPRESSION[\"TernaryExpression\"] $ifClause)
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "TernaryExpression"), root_1);

                        adaptor.addChild(root_1, stream_ifClause.nextTree());
                        adaptor.addChild(root_1,  ifExpr == null ? EvaluationExpression.VALUE : (ifExpr!=null?((EvaluationExpression)ifExpr.tree):null) );
                        adaptor.addChild(root_1,  (elseExpr!=null?((EvaluationExpression)elseExpr.tree):null) );

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:128:4: ifExpr2= orExpression 'if' ifClause2= expression
                    {
                    pushFollow(FOLLOW_orExpression_in_ternaryExpression388);
                    ifExpr2=orExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_orExpression.add(ifExpr2.getTree());
                    string_literal25=(Token)match(input,36,FOLLOW_36_in_ternaryExpression390); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_36.add(string_literal25);

                    pushFollow(FOLLOW_expression_in_ternaryExpression394);
                    ifClause2=expression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_expression.add(ifClause2.getTree());


                    // AST REWRITE
                    // elements: ifExpr2, ifClause2
                    // token labels: 
                    // rule labels: retval, ifExpr2, ifClause2
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
                    RewriteRuleSubtreeStream stream_ifExpr2=new RewriteRuleSubtreeStream(adaptor,"rule ifExpr2",ifExpr2!=null?ifExpr2.tree:null);
                    RewriteRuleSubtreeStream stream_ifClause2=new RewriteRuleSubtreeStream(adaptor,"rule ifClause2",ifClause2!=null?ifClause2.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 129:2: -> ^( EXPRESSION[\"TernaryExpression\"] $ifClause2 $ifExpr2)
                    {
                        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:129:5: ^( EXPRESSION[\"TernaryExpression\"] $ifClause2 $ifExpr2)
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "TernaryExpression"), root_1);

                        adaptor.addChild(root_1, stream_ifClause2.nextTree());
                        adaptor.addChild(root_1, stream_ifExpr2.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 3 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:130:5: orExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_orExpression_in_ternaryExpression414);
                    orExpression26=orExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, orExpression26.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 9, ternaryExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "ternaryExpression"

    public static class orExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "orExpression"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:132:1: orExpression : exprs+= andExpression ( ( 'or' | '||' ) exprs+= andExpression )* -> { $exprs.size() == 1 }? -> ^( EXPRESSION[\"OrExpression\"] ) ;
    public final SJaqlParser.orExpression_return orExpression() throws RecognitionException {
        SJaqlParser.orExpression_return retval = new SJaqlParser.orExpression_return();
        retval.start = input.LT(1);
        int orExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token string_literal27=null;
        Token string_literal28=null;
        List list_exprs=null;
        RuleReturnScope exprs = null;
        EvaluationExpression string_literal27_tree=null;
        EvaluationExpression string_literal28_tree=null;
        RewriteRuleTokenStream stream_37=new RewriteRuleTokenStream(adaptor,"token 37");
        RewriteRuleTokenStream stream_38=new RewriteRuleTokenStream(adaptor,"token 38");
        RewriteRuleSubtreeStream stream_andExpression=new RewriteRuleSubtreeStream(adaptor,"rule andExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 10) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:133:3: (exprs+= andExpression ( ( 'or' | '||' ) exprs+= andExpression )* -> { $exprs.size() == 1 }? -> ^( EXPRESSION[\"OrExpression\"] ) )
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:133:5: exprs+= andExpression ( ( 'or' | '||' ) exprs+= andExpression )*
            {
            pushFollow(FOLLOW_andExpression_in_orExpression427);
            exprs=andExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_andExpression.add(exprs.getTree());
            if (list_exprs==null) list_exprs=new ArrayList();
            list_exprs.add(exprs.getTree());

            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:133:26: ( ( 'or' | '||' ) exprs+= andExpression )*
            loop8:
            do {
                int alt8=2;
                int LA8_0 = input.LA(1);

                if ( (LA8_0==37) ) {
                    int LA8_2 = input.LA(2);

                    if ( (synpred12_SJaql()) ) {
                        alt8=1;
                    }


                }
                else if ( (LA8_0==38) ) {
                    int LA8_3 = input.LA(2);

                    if ( (synpred12_SJaql()) ) {
                        alt8=1;
                    }


                }


                switch (alt8) {
            	case 1 :
            	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:133:27: ( 'or' | '||' ) exprs+= andExpression
            	    {
            	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:133:27: ( 'or' | '||' )
            	    int alt7=2;
            	    int LA7_0 = input.LA(1);

            	    if ( (LA7_0==37) ) {
            	        alt7=1;
            	    }
            	    else if ( (LA7_0==38) ) {
            	        alt7=2;
            	    }
            	    else {
            	        if (state.backtracking>0) {state.failed=true; return retval;}
            	        NoViableAltException nvae =
            	            new NoViableAltException("", 7, 0, input);

            	        throw nvae;
            	    }
            	    switch (alt7) {
            	        case 1 :
            	            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:133:28: 'or'
            	            {
            	            string_literal27=(Token)match(input,37,FOLLOW_37_in_orExpression431); if (state.failed) return retval; 
            	            if ( state.backtracking==0 ) stream_37.add(string_literal27);


            	            }
            	            break;
            	        case 2 :
            	            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:133:35: '||'
            	            {
            	            string_literal28=(Token)match(input,38,FOLLOW_38_in_orExpression435); if (state.failed) return retval; 
            	            if ( state.backtracking==0 ) stream_38.add(string_literal28);


            	            }
            	            break;

            	    }

            	    pushFollow(FOLLOW_andExpression_in_orExpression440);
            	    exprs=andExpression();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_andExpression.add(exprs.getTree());
            	    if (list_exprs==null) list_exprs=new ArrayList();
            	    list_exprs.add(exprs.getTree());


            	    }
            	    break;

            	default :
            	    break loop8;
                }
            } while (true);



            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 134:3: -> { $exprs.size() == 1 }?
            if ( list_exprs.size() == 1 ) {
                adaptor.addChild(root_0,  list_exprs.get(0) );

            }
            else // 135:3: -> ^( EXPRESSION[\"OrExpression\"] )
            {
                // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:135:6: ^( EXPRESSION[\"OrExpression\"] )
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "OrExpression"), root_1);

                adaptor.addChild(root_1,  list_exprs.toArray(new EvaluationExpression[list_exprs.size()]) );

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 10, orExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "orExpression"

    public static class andExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "andExpression"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:137:1: andExpression : exprs+= elementExpression ( ( 'and' | '&&' ) exprs+= elementExpression )* -> { $exprs.size() == 1 }? -> ^( EXPRESSION[\"AndExpression\"] ) ;
    public final SJaqlParser.andExpression_return andExpression() throws RecognitionException {
        SJaqlParser.andExpression_return retval = new SJaqlParser.andExpression_return();
        retval.start = input.LT(1);
        int andExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token string_literal29=null;
        Token string_literal30=null;
        List list_exprs=null;
        RuleReturnScope exprs = null;
        EvaluationExpression string_literal29_tree=null;
        EvaluationExpression string_literal30_tree=null;
        RewriteRuleTokenStream stream_40=new RewriteRuleTokenStream(adaptor,"token 40");
        RewriteRuleTokenStream stream_39=new RewriteRuleTokenStream(adaptor,"token 39");
        RewriteRuleSubtreeStream stream_elementExpression=new RewriteRuleSubtreeStream(adaptor,"rule elementExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 11) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:138:3: (exprs+= elementExpression ( ( 'and' | '&&' ) exprs+= elementExpression )* -> { $exprs.size() == 1 }? -> ^( EXPRESSION[\"AndExpression\"] ) )
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:138:5: exprs+= elementExpression ( ( 'and' | '&&' ) exprs+= elementExpression )*
            {
            pushFollow(FOLLOW_elementExpression_in_andExpression474);
            exprs=elementExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_elementExpression.add(exprs.getTree());
            if (list_exprs==null) list_exprs=new ArrayList();
            list_exprs.add(exprs.getTree());

            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:138:30: ( ( 'and' | '&&' ) exprs+= elementExpression )*
            loop10:
            do {
                int alt10=2;
                int LA10_0 = input.LA(1);

                if ( (LA10_0==39) ) {
                    int LA10_2 = input.LA(2);

                    if ( (synpred14_SJaql()) ) {
                        alt10=1;
                    }


                }
                else if ( (LA10_0==40) ) {
                    int LA10_3 = input.LA(2);

                    if ( (synpred14_SJaql()) ) {
                        alt10=1;
                    }


                }


                switch (alt10) {
            	case 1 :
            	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:138:31: ( 'and' | '&&' ) exprs+= elementExpression
            	    {
            	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:138:31: ( 'and' | '&&' )
            	    int alt9=2;
            	    int LA9_0 = input.LA(1);

            	    if ( (LA9_0==39) ) {
            	        alt9=1;
            	    }
            	    else if ( (LA9_0==40) ) {
            	        alt9=2;
            	    }
            	    else {
            	        if (state.backtracking>0) {state.failed=true; return retval;}
            	        NoViableAltException nvae =
            	            new NoViableAltException("", 9, 0, input);

            	        throw nvae;
            	    }
            	    switch (alt9) {
            	        case 1 :
            	            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:138:32: 'and'
            	            {
            	            string_literal29=(Token)match(input,39,FOLLOW_39_in_andExpression478); if (state.failed) return retval; 
            	            if ( state.backtracking==0 ) stream_39.add(string_literal29);


            	            }
            	            break;
            	        case 2 :
            	            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:138:40: '&&'
            	            {
            	            string_literal30=(Token)match(input,40,FOLLOW_40_in_andExpression482); if (state.failed) return retval; 
            	            if ( state.backtracking==0 ) stream_40.add(string_literal30);


            	            }
            	            break;

            	    }

            	    pushFollow(FOLLOW_elementExpression_in_andExpression487);
            	    exprs=elementExpression();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_elementExpression.add(exprs.getTree());
            	    if (list_exprs==null) list_exprs=new ArrayList();
            	    list_exprs.add(exprs.getTree());


            	    }
            	    break;

            	default :
            	    break loop10;
                }
            } while (true);



            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 139:3: -> { $exprs.size() == 1 }?
            if ( list_exprs.size() == 1 ) {
                adaptor.addChild(root_0,  list_exprs.get(0) );

            }
            else // 140:3: -> ^( EXPRESSION[\"AndExpression\"] )
            {
                // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:140:6: ^( EXPRESSION[\"AndExpression\"] )
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "AndExpression"), root_1);

                adaptor.addChild(root_1,  list_exprs.toArray(new EvaluationExpression[list_exprs.size()]) );

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 11, andExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "andExpression"

    public static class elementExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "elementExpression"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:142:1: elementExpression : elem= comparisonExpression ( (not= 'not' )? 'in' set= comparisonExpression )? -> { set == null }? $elem -> ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set) ;
    public final SJaqlParser.elementExpression_return elementExpression() throws RecognitionException {
        SJaqlParser.elementExpression_return retval = new SJaqlParser.elementExpression_return();
        retval.start = input.LT(1);
        int elementExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token not=null;
        Token string_literal31=null;
        SJaqlParser.comparisonExpression_return elem = null;

        SJaqlParser.comparisonExpression_return set = null;


        EvaluationExpression not_tree=null;
        EvaluationExpression string_literal31_tree=null;
        RewriteRuleTokenStream stream_42=new RewriteRuleTokenStream(adaptor,"token 42");
        RewriteRuleTokenStream stream_41=new RewriteRuleTokenStream(adaptor,"token 41");
        RewriteRuleSubtreeStream stream_comparisonExpression=new RewriteRuleSubtreeStream(adaptor,"rule comparisonExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 12) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:143:2: (elem= comparisonExpression ( (not= 'not' )? 'in' set= comparisonExpression )? -> { set == null }? $elem -> ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set) )
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:143:4: elem= comparisonExpression ( (not= 'not' )? 'in' set= comparisonExpression )?
            {
            pushFollow(FOLLOW_comparisonExpression_in_elementExpression521);
            elem=comparisonExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_comparisonExpression.add(elem.getTree());
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:143:30: ( (not= 'not' )? 'in' set= comparisonExpression )?
            int alt12=2;
            int LA12_0 = input.LA(1);

            if ( (LA12_0==41) ) {
                int LA12_1 = input.LA(2);

                if ( (synpred16_SJaql()) ) {
                    alt12=1;
                }
            }
            else if ( (LA12_0==42) ) {
                int LA12_2 = input.LA(2);

                if ( (synpred16_SJaql()) ) {
                    alt12=1;
                }
            }
            switch (alt12) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:143:31: (not= 'not' )? 'in' set= comparisonExpression
                    {
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:143:34: (not= 'not' )?
                    int alt11=2;
                    int LA11_0 = input.LA(1);

                    if ( (LA11_0==41) ) {
                        alt11=1;
                    }
                    switch (alt11) {
                        case 1 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: not= 'not'
                            {
                            not=(Token)match(input,41,FOLLOW_41_in_elementExpression526); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_41.add(not);


                            }
                            break;

                    }

                    string_literal31=(Token)match(input,42,FOLLOW_42_in_elementExpression529); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_42.add(string_literal31);

                    pushFollow(FOLLOW_comparisonExpression_in_elementExpression533);
                    set=comparisonExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_comparisonExpression.add(set.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: elem, set, elem
            // token labels: 
            // rule labels: elem, retval, set
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_elem=new RewriteRuleSubtreeStream(adaptor,"rule elem",elem!=null?elem.tree:null);
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_set=new RewriteRuleSubtreeStream(adaptor,"rule set",set!=null?set.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 144:2: -> { set == null }? $elem
            if ( set == null ) {
                adaptor.addChild(root_0, stream_elem.nextTree());

            }
            else // 145:2: -> ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set)
            {
                // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:145:5: ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ElementInSetExpression"), root_1);

                adaptor.addChild(root_1, stream_elem.nextTree());
                adaptor.addChild(root_1,  not == null ? ElementInSetExpression.Quantor.EXISTS_IN : ElementInSetExpression.Quantor.EXISTS_NOT_IN);
                adaptor.addChild(root_1, stream_set.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 12, elementExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "elementExpression"

    public static class comparisonExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "comparisonExpression"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:148:1: comparisonExpression : e1= arithmeticExpression ( (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression )? -> { $s == null }? $e1 -> { $s.getText().equals(\"!=\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) -> { $s.getText().equals(\"==\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) -> ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) ;
    public final SJaqlParser.comparisonExpression_return comparisonExpression() throws RecognitionException {
        SJaqlParser.comparisonExpression_return retval = new SJaqlParser.comparisonExpression_return();
        retval.start = input.LT(1);
        int comparisonExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token s=null;
        SJaqlParser.arithmeticExpression_return e1 = null;

        SJaqlParser.arithmeticExpression_return e2 = null;


        EvaluationExpression s_tree=null;
        RewriteRuleTokenStream stream_48=new RewriteRuleTokenStream(adaptor,"token 48");
        RewriteRuleTokenStream stream_45=new RewriteRuleTokenStream(adaptor,"token 45");
        RewriteRuleTokenStream stream_43=new RewriteRuleTokenStream(adaptor,"token 43");
        RewriteRuleTokenStream stream_44=new RewriteRuleTokenStream(adaptor,"token 44");
        RewriteRuleTokenStream stream_47=new RewriteRuleTokenStream(adaptor,"token 47");
        RewriteRuleTokenStream stream_46=new RewriteRuleTokenStream(adaptor,"token 46");
        RewriteRuleSubtreeStream stream_arithmeticExpression=new RewriteRuleSubtreeStream(adaptor,"rule arithmeticExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 13) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:149:2: (e1= arithmeticExpression ( (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression )? -> { $s == null }? $e1 -> { $s.getText().equals(\"!=\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) -> { $s.getText().equals(\"==\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) -> ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) )
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:149:4: e1= arithmeticExpression ( (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression )?
            {
            pushFollow(FOLLOW_arithmeticExpression_in_comparisonExpression574);
            e1=arithmeticExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_arithmeticExpression.add(e1.getTree());
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:149:28: ( (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression )?
            int alt14=2;
            switch ( input.LA(1) ) {
                case 43:
                    {
                    int LA14_1 = input.LA(2);

                    if ( (synpred22_SJaql()) ) {
                        alt14=1;
                    }
                    }
                    break;
                case 44:
                    {
                    int LA14_2 = input.LA(2);

                    if ( (synpred22_SJaql()) ) {
                        alt14=1;
                    }
                    }
                    break;
                case 45:
                    {
                    int LA14_3 = input.LA(2);

                    if ( (synpred22_SJaql()) ) {
                        alt14=1;
                    }
                    }
                    break;
                case 46:
                    {
                    int LA14_4 = input.LA(2);

                    if ( (synpred22_SJaql()) ) {
                        alt14=1;
                    }
                    }
                    break;
                case 47:
                    {
                    int LA14_5 = input.LA(2);

                    if ( (synpred22_SJaql()) ) {
                        alt14=1;
                    }
                    }
                    break;
                case 48:
                    {
                    int LA14_6 = input.LA(2);

                    if ( (synpred22_SJaql()) ) {
                        alt14=1;
                    }
                    }
                    break;
            }

            switch (alt14) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:149:29: (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression
                    {
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:149:29: (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' )
                    int alt13=6;
                    switch ( input.LA(1) ) {
                    case 43:
                        {
                        alt13=1;
                        }
                        break;
                    case 44:
                        {
                        alt13=2;
                        }
                        break;
                    case 45:
                        {
                        alt13=3;
                        }
                        break;
                    case 46:
                        {
                        alt13=4;
                        }
                        break;
                    case 47:
                        {
                        alt13=5;
                        }
                        break;
                    case 48:
                        {
                        alt13=6;
                        }
                        break;
                    default:
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 13, 0, input);

                        throw nvae;
                    }

                    switch (alt13) {
                        case 1 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:149:30: s= '<='
                            {
                            s=(Token)match(input,43,FOLLOW_43_in_comparisonExpression580); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_43.add(s);


                            }
                            break;
                        case 2 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:149:39: s= '>='
                            {
                            s=(Token)match(input,44,FOLLOW_44_in_comparisonExpression586); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_44.add(s);


                            }
                            break;
                        case 3 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:149:48: s= '<'
                            {
                            s=(Token)match(input,45,FOLLOW_45_in_comparisonExpression592); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_45.add(s);


                            }
                            break;
                        case 4 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:149:56: s= '>'
                            {
                            s=(Token)match(input,46,FOLLOW_46_in_comparisonExpression598); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_46.add(s);


                            }
                            break;
                        case 5 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:149:64: s= '=='
                            {
                            s=(Token)match(input,47,FOLLOW_47_in_comparisonExpression604); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_47.add(s);


                            }
                            break;
                        case 6 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:149:73: s= '!='
                            {
                            s=(Token)match(input,48,FOLLOW_48_in_comparisonExpression610); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_48.add(s);


                            }
                            break;

                    }

                    pushFollow(FOLLOW_arithmeticExpression_in_comparisonExpression615);
                    e2=arithmeticExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_arithmeticExpression.add(e2.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: e1, e1, e2, e2, e2, e1, e1
            // token labels: 
            // rule labels: retval, e1, e2
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_e1=new RewriteRuleSubtreeStream(adaptor,"rule e1",e1!=null?e1.tree:null);
            RewriteRuleSubtreeStream stream_e2=new RewriteRuleSubtreeStream(adaptor,"rule e2",e2!=null?e2.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 150:2: -> { $s == null }? $e1
            if ( s == null ) {
                adaptor.addChild(root_0, stream_e1.nextTree());

            }
            else // 151:3: -> { $s.getText().equals(\"!=\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
            if ( s.getText().equals("!=") ) {
                // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:151:38: ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ComparativeExpression"), root_1);

                adaptor.addChild(root_1, stream_e1.nextTree());
                adaptor.addChild(root_1, ComparativeExpression.BinaryOperator.NOT_EQUAL);
                adaptor.addChild(root_1, stream_e2.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }
            else // 152:3: -> { $s.getText().equals(\"==\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
            if ( s.getText().equals("==") ) {
                // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:152:38: ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ComparativeExpression"), root_1);

                adaptor.addChild(root_1, stream_e1.nextTree());
                adaptor.addChild(root_1, ComparativeExpression.BinaryOperator.EQUAL);
                adaptor.addChild(root_1, stream_e2.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }
            else // 153:2: -> ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
            {
                // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:153:6: ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ComparativeExpression"), root_1);

                adaptor.addChild(root_1, stream_e1.nextTree());
                adaptor.addChild(root_1, ComparativeExpression.BinaryOperator.valueOfSymbol((s!=null?s.getText():null)));
                adaptor.addChild(root_1, stream_e2.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 13, comparisonExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "comparisonExpression"

    public static class arithmeticExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "arithmeticExpression"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:155:1: arithmeticExpression : e1= multiplicationExpression ( (s= '+' | s= '-' ) e2= multiplicationExpression )? -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2) -> $e1;
    public final SJaqlParser.arithmeticExpression_return arithmeticExpression() throws RecognitionException {
        SJaqlParser.arithmeticExpression_return retval = new SJaqlParser.arithmeticExpression_return();
        retval.start = input.LT(1);
        int arithmeticExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token s=null;
        SJaqlParser.multiplicationExpression_return e1 = null;

        SJaqlParser.multiplicationExpression_return e2 = null;


        EvaluationExpression s_tree=null;
        RewriteRuleTokenStream stream_49=new RewriteRuleTokenStream(adaptor,"token 49");
        RewriteRuleTokenStream stream_50=new RewriteRuleTokenStream(adaptor,"token 50");
        RewriteRuleSubtreeStream stream_multiplicationExpression=new RewriteRuleSubtreeStream(adaptor,"rule multiplicationExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 14) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:156:2: (e1= multiplicationExpression ( (s= '+' | s= '-' ) e2= multiplicationExpression )? -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2) -> $e1)
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:156:4: e1= multiplicationExpression ( (s= '+' | s= '-' ) e2= multiplicationExpression )?
            {
            pushFollow(FOLLOW_multiplicationExpression_in_arithmeticExpression695);
            e1=multiplicationExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_multiplicationExpression.add(e1.getTree());
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:156:32: ( (s= '+' | s= '-' ) e2= multiplicationExpression )?
            int alt16=2;
            int LA16_0 = input.LA(1);

            if ( (LA16_0==49) ) {
                int LA16_1 = input.LA(2);

                if ( (synpred24_SJaql()) ) {
                    alt16=1;
                }
            }
            else if ( (LA16_0==50) ) {
                int LA16_2 = input.LA(2);

                if ( (synpred24_SJaql()) ) {
                    alt16=1;
                }
            }
            switch (alt16) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:156:33: (s= '+' | s= '-' ) e2= multiplicationExpression
                    {
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:156:33: (s= '+' | s= '-' )
                    int alt15=2;
                    int LA15_0 = input.LA(1);

                    if ( (LA15_0==49) ) {
                        alt15=1;
                    }
                    else if ( (LA15_0==50) ) {
                        alt15=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 15, 0, input);

                        throw nvae;
                    }
                    switch (alt15) {
                        case 1 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:156:34: s= '+'
                            {
                            s=(Token)match(input,49,FOLLOW_49_in_arithmeticExpression701); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_49.add(s);


                            }
                            break;
                        case 2 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:156:42: s= '-'
                            {
                            s=(Token)match(input,50,FOLLOW_50_in_arithmeticExpression707); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_50.add(s);


                            }
                            break;

                    }

                    pushFollow(FOLLOW_multiplicationExpression_in_arithmeticExpression712);
                    e2=multiplicationExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_multiplicationExpression.add(e2.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: e1, e1, e2
            // token labels: 
            // rule labels: retval, e1, e2
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_e1=new RewriteRuleSubtreeStream(adaptor,"rule e1",e1!=null?e1.tree:null);
            RewriteRuleSubtreeStream stream_e2=new RewriteRuleSubtreeStream(adaptor,"rule e2",e2!=null?e2.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 157:2: -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2)
            if ( s != null ) {
                // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:157:21: ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ArithmeticExpression"), root_1);

                adaptor.addChild(root_1, stream_e1.nextTree());
                adaptor.addChild(root_1,  s.getText().equals("+") ? ArithmeticExpression.ArithmeticOperator.ADDITION : ArithmeticExpression.ArithmeticOperator.SUBTRACTION);
                adaptor.addChild(root_1, stream_e2.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }
            else // 159:2: -> $e1
            {
                adaptor.addChild(root_0, stream_e1.nextTree());

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 14, arithmeticExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "arithmeticExpression"

    public static class multiplicationExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "multiplicationExpression"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:161:1: multiplicationExpression : e1= preincrementExpression ( (s= '*' | s= '/' ) e2= preincrementExpression )? -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2) -> $e1;
    public final SJaqlParser.multiplicationExpression_return multiplicationExpression() throws RecognitionException {
        SJaqlParser.multiplicationExpression_return retval = new SJaqlParser.multiplicationExpression_return();
        retval.start = input.LT(1);
        int multiplicationExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token s=null;
        SJaqlParser.preincrementExpression_return e1 = null;

        SJaqlParser.preincrementExpression_return e2 = null;


        EvaluationExpression s_tree=null;
        RewriteRuleTokenStream stream_STAR=new RewriteRuleTokenStream(adaptor,"token STAR");
        RewriteRuleTokenStream stream_51=new RewriteRuleTokenStream(adaptor,"token 51");
        RewriteRuleSubtreeStream stream_preincrementExpression=new RewriteRuleSubtreeStream(adaptor,"rule preincrementExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 15) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:162:2: (e1= preincrementExpression ( (s= '*' | s= '/' ) e2= preincrementExpression )? -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2) -> $e1)
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:162:4: e1= preincrementExpression ( (s= '*' | s= '/' ) e2= preincrementExpression )?
            {
            pushFollow(FOLLOW_preincrementExpression_in_multiplicationExpression755);
            e1=preincrementExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_preincrementExpression.add(e1.getTree());
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:162:30: ( (s= '*' | s= '/' ) e2= preincrementExpression )?
            int alt18=2;
            int LA18_0 = input.LA(1);

            if ( (LA18_0==STAR) ) {
                int LA18_1 = input.LA(2);

                if ( (synpred26_SJaql()) ) {
                    alt18=1;
                }
            }
            else if ( (LA18_0==51) ) {
                int LA18_2 = input.LA(2);

                if ( (synpred26_SJaql()) ) {
                    alt18=1;
                }
            }
            switch (alt18) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:162:31: (s= '*' | s= '/' ) e2= preincrementExpression
                    {
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:162:31: (s= '*' | s= '/' )
                    int alt17=2;
                    int LA17_0 = input.LA(1);

                    if ( (LA17_0==STAR) ) {
                        alt17=1;
                    }
                    else if ( (LA17_0==51) ) {
                        alt17=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 17, 0, input);

                        throw nvae;
                    }
                    switch (alt17) {
                        case 1 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:162:32: s= '*'
                            {
                            s=(Token)match(input,STAR,FOLLOW_STAR_in_multiplicationExpression761); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_STAR.add(s);


                            }
                            break;
                        case 2 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:162:40: s= '/'
                            {
                            s=(Token)match(input,51,FOLLOW_51_in_multiplicationExpression767); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_51.add(s);


                            }
                            break;

                    }

                    pushFollow(FOLLOW_preincrementExpression_in_multiplicationExpression772);
                    e2=preincrementExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_preincrementExpression.add(e2.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: e2, e1, e1
            // token labels: 
            // rule labels: retval, e1, e2
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_e1=new RewriteRuleSubtreeStream(adaptor,"rule e1",e1!=null?e1.tree:null);
            RewriteRuleSubtreeStream stream_e2=new RewriteRuleSubtreeStream(adaptor,"rule e2",e2!=null?e2.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 163:2: -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2)
            if ( s != null ) {
                // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:163:21: ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ArithmeticExpression"), root_1);

                adaptor.addChild(root_1, stream_e1.nextTree());
                adaptor.addChild(root_1,  s.getText().equals("*") ? ArithmeticExpression.ArithmeticOperator.MULTIPLICATION : ArithmeticExpression.ArithmeticOperator.DIVISION);
                adaptor.addChild(root_1, stream_e2.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }
            else // 165:2: -> $e1
            {
                adaptor.addChild(root_0, stream_e1.nextTree());

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 15, multiplicationExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "multiplicationExpression"

    public static class preincrementExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "preincrementExpression"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:167:1: preincrementExpression : ( '++' preincrementExpression | '--' preincrementExpression | unaryExpression );
    public final SJaqlParser.preincrementExpression_return preincrementExpression() throws RecognitionException {
        SJaqlParser.preincrementExpression_return retval = new SJaqlParser.preincrementExpression_return();
        retval.start = input.LT(1);
        int preincrementExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token string_literal32=null;
        Token string_literal34=null;
        SJaqlParser.preincrementExpression_return preincrementExpression33 = null;

        SJaqlParser.preincrementExpression_return preincrementExpression35 = null;

        SJaqlParser.unaryExpression_return unaryExpression36 = null;


        EvaluationExpression string_literal32_tree=null;
        EvaluationExpression string_literal34_tree=null;

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 16) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:168:2: ( '++' preincrementExpression | '--' preincrementExpression | unaryExpression )
            int alt19=3;
            switch ( input.LA(1) ) {
            case 52:
                {
                alt19=1;
                }
                break;
            case 53:
                {
                alt19=2;
                }
                break;
            case ID:
            case VAR:
            case STRING:
            case DECIMAL:
            case INTEGER:
            case UINT:
            case 30:
            case 54:
            case 55:
            case 58:
            case 60:
            case 61:
            case 62:
            case 63:
            case 65:
            case 66:
                {
                alt19=3;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 19, 0, input);

                throw nvae;
            }

            switch (alt19) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:168:4: '++' preincrementExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    string_literal32=(Token)match(input,52,FOLLOW_52_in_preincrementExpression813); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    string_literal32_tree = (EvaluationExpression)adaptor.create(string_literal32);
                    adaptor.addChild(root_0, string_literal32_tree);
                    }
                    pushFollow(FOLLOW_preincrementExpression_in_preincrementExpression815);
                    preincrementExpression33=preincrementExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, preincrementExpression33.getTree());

                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:169:4: '--' preincrementExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    string_literal34=(Token)match(input,53,FOLLOW_53_in_preincrementExpression820); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    string_literal34_tree = (EvaluationExpression)adaptor.create(string_literal34);
                    adaptor.addChild(root_0, string_literal34_tree);
                    }
                    pushFollow(FOLLOW_preincrementExpression_in_preincrementExpression822);
                    preincrementExpression35=preincrementExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, preincrementExpression35.getTree());

                    }
                    break;
                case 3 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:170:4: unaryExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_unaryExpression_in_preincrementExpression827);
                    unaryExpression36=unaryExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, unaryExpression36.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 16, preincrementExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "preincrementExpression"

    public static class unaryExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "unaryExpression"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:172:1: unaryExpression : ( '!' | '~' )? castExpression ;
    public final SJaqlParser.unaryExpression_return unaryExpression() throws RecognitionException {
        SJaqlParser.unaryExpression_return retval = new SJaqlParser.unaryExpression_return();
        retval.start = input.LT(1);
        int unaryExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token set37=null;
        SJaqlParser.castExpression_return castExpression38 = null;


        EvaluationExpression set37_tree=null;

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 17) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:173:2: ( ( '!' | '~' )? castExpression )
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:173:4: ( '!' | '~' )? castExpression
            {
            root_0 = (EvaluationExpression)adaptor.nil();

            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:173:4: ( '!' | '~' )?
            int alt20=2;
            int LA20_0 = input.LA(1);

            if ( ((LA20_0>=54 && LA20_0<=55)) ) {
                alt20=1;
            }
            switch (alt20) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:
                    {
                    set37=(Token)input.LT(1);
                    if ( (input.LA(1)>=54 && input.LA(1)<=55) ) {
                        input.consume();
                        if ( state.backtracking==0 ) adaptor.addChild(root_0, (EvaluationExpression)adaptor.create(set37));
                        state.errorRecovery=false;state.failed=false;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        throw mse;
                    }


                    }
                    break;

            }

            pushFollow(FOLLOW_castExpression_in_unaryExpression846);
            castExpression38=castExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, castExpression38.getTree());

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 17, unaryExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "unaryExpression"

    public static class castExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "castExpression"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:175:1: castExpression : ( '(' type= ID ')' expr= generalPathExpression | expr= generalPathExpression 'as' type= ID | expr= generalPathExpression ) -> { type != null }? -> $expr;
    public final SJaqlParser.castExpression_return castExpression() throws RecognitionException {
        SJaqlParser.castExpression_return retval = new SJaqlParser.castExpression_return();
        retval.start = input.LT(1);
        int castExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token type=null;
        Token char_literal39=null;
        Token char_literal40=null;
        Token string_literal41=null;
        SJaqlParser.generalPathExpression_return expr = null;


        EvaluationExpression type_tree=null;
        EvaluationExpression char_literal39_tree=null;
        EvaluationExpression char_literal40_tree=null;
        EvaluationExpression string_literal41_tree=null;
        RewriteRuleTokenStream stream_30=new RewriteRuleTokenStream(adaptor,"token 30");
        RewriteRuleTokenStream stream_56=new RewriteRuleTokenStream(adaptor,"token 56");
        RewriteRuleTokenStream stream_32=new RewriteRuleTokenStream(adaptor,"token 32");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_generalPathExpression=new RewriteRuleSubtreeStream(adaptor,"rule generalPathExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 18) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:176:2: ( ( '(' type= ID ')' expr= generalPathExpression | expr= generalPathExpression 'as' type= ID | expr= generalPathExpression ) -> { type != null }? -> $expr)
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:176:4: ( '(' type= ID ')' expr= generalPathExpression | expr= generalPathExpression 'as' type= ID | expr= generalPathExpression )
            {
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:176:4: ( '(' type= ID ')' expr= generalPathExpression | expr= generalPathExpression 'as' type= ID | expr= generalPathExpression )
            int alt21=3;
            alt21 = dfa21.predict(input);
            switch (alt21) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:176:5: '(' type= ID ')' expr= generalPathExpression
                    {
                    char_literal39=(Token)match(input,30,FOLLOW_30_in_castExpression856); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_30.add(char_literal39);

                    type=(Token)match(input,ID,FOLLOW_ID_in_castExpression860); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(type);

                    char_literal40=(Token)match(input,32,FOLLOW_32_in_castExpression862); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_32.add(char_literal40);

                    pushFollow(FOLLOW_generalPathExpression_in_castExpression866);
                    expr=generalPathExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_generalPathExpression.add(expr.getTree());

                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:177:4: expr= generalPathExpression 'as' type= ID
                    {
                    pushFollow(FOLLOW_generalPathExpression_in_castExpression873);
                    expr=generalPathExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_generalPathExpression.add(expr.getTree());
                    string_literal41=(Token)match(input,56,FOLLOW_56_in_castExpression875); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_56.add(string_literal41);

                    type=(Token)match(input,ID,FOLLOW_ID_in_castExpression879); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(type);


                    }
                    break;
                case 3 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:178:4: expr= generalPathExpression
                    {
                    pushFollow(FOLLOW_generalPathExpression_in_castExpression886);
                    expr=generalPathExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_generalPathExpression.add(expr.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: expr
            // token labels: 
            // rule labels: retval, expr
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_expr=new RewriteRuleSubtreeStream(adaptor,"rule expr",expr!=null?expr.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 179:2: -> { type != null }?
            if ( type != null ) {
                adaptor.addChild(root_0,  coerce((type!=null?type.getText():null), (expr!=null?((EvaluationExpression)expr.tree):null)) );

            }
            else // 180:2: -> $expr
            {
                adaptor.addChild(root_0, stream_expr.nextTree());

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 18, castExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "castExpression"

    public static class generalPathExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "generalPathExpression"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:182:1: generalPathExpression : ( ({...}? contextAwarePathExpression ) | pathExpression ) ;
    public final SJaqlParser.generalPathExpression_return generalPathExpression() throws RecognitionException {
        SJaqlParser.generalPathExpression_return retval = new SJaqlParser.generalPathExpression_return();
        retval.start = input.LT(1);
        int generalPathExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        SJaqlParser.contextAwarePathExpression_return contextAwarePathExpression42 = null;

        SJaqlParser.pathExpression_return pathExpression43 = null;



        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 19) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:183:2: ( ( ({...}? contextAwarePathExpression ) | pathExpression ) )
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:183:4: ( ({...}? contextAwarePathExpression ) | pathExpression )
            {
            root_0 = (EvaluationExpression)adaptor.nil();

            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:183:4: ( ({...}? contextAwarePathExpression ) | pathExpression )
            int alt22=2;
            int LA22_0 = input.LA(1);

            if ( (LA22_0==ID) ) {
                int LA22_1 = input.LA(2);

                if ( ((synpred33_SJaql()&&(((contextAwareExpression_scope)contextAwareExpression_stack.peek()).context != null))) ) {
                    alt22=1;
                }
                else if ( (true) ) {
                    alt22=2;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 22, 1, input);

                    throw nvae;
                }
            }
            else if ( ((LA22_0>=VAR && LA22_0<=STRING)||(LA22_0>=DECIMAL && LA22_0<=UINT)||LA22_0==30||LA22_0==58||(LA22_0>=60 && LA22_0<=63)||(LA22_0>=65 && LA22_0<=66)) ) {
                alt22=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 22, 0, input);

                throw nvae;
            }
            switch (alt22) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:183:5: ({...}? contextAwarePathExpression )
                    {
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:183:5: ({...}? contextAwarePathExpression )
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:183:6: {...}? contextAwarePathExpression
                    {
                    if ( !((((contextAwareExpression_scope)contextAwareExpression_stack.peek()).context != null)) ) {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        throw new FailedPredicateException(input, "generalPathExpression", "$contextAwareExpression::context != null");
                    }
                    pushFollow(FOLLOW_contextAwarePathExpression_in_generalPathExpression915);
                    contextAwarePathExpression42=contextAwarePathExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, contextAwarePathExpression42.getTree());

                    }


                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:183:80: pathExpression
                    {
                    pushFollow(FOLLOW_pathExpression_in_generalPathExpression920);
                    pathExpression43=pathExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, pathExpression43.getTree());

                    }
                    break;

            }


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 19, generalPathExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "generalPathExpression"

    protected static class contextAwarePathExpression_scope {
        List<EvaluationExpression> fragments;
    }
    protected Stack contextAwarePathExpression_stack = new Stack();

    public static class contextAwarePathExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "contextAwarePathExpression"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:185:1: contextAwarePathExpression : start= ID ( ( '.' (field= ID ) ) | arrayAccess )* -> ^( EXPRESSION[\"PathExpression\"] ) ;
    public final SJaqlParser.contextAwarePathExpression_return contextAwarePathExpression() throws RecognitionException {
        contextAwarePathExpression_stack.push(new contextAwarePathExpression_scope());
        SJaqlParser.contextAwarePathExpression_return retval = new SJaqlParser.contextAwarePathExpression_return();
        retval.start = input.LT(1);
        int contextAwarePathExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token start=null;
        Token field=null;
        Token char_literal44=null;
        SJaqlParser.arrayAccess_return arrayAccess45 = null;


        EvaluationExpression start_tree=null;
        EvaluationExpression field_tree=null;
        EvaluationExpression char_literal44_tree=null;
        RewriteRuleTokenStream stream_57=new RewriteRuleTokenStream(adaptor,"token 57");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_arrayAccess=new RewriteRuleSubtreeStream(adaptor,"rule arrayAccess");
         ((contextAwarePathExpression_scope)contextAwarePathExpression_stack.peek()).fragments = new ArrayList<EvaluationExpression>(); 
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 20) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:188:3: (start= ID ( ( '.' (field= ID ) ) | arrayAccess )* -> ^( EXPRESSION[\"PathExpression\"] ) )
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:188:5: start= ID ( ( '.' (field= ID ) ) | arrayAccess )*
            {
            start=(Token)match(input,ID,FOLLOW_ID_in_contextAwarePathExpression943); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(start);

            if ( state.backtracking==0 ) {
               ((contextAwarePathExpression_scope)contextAwarePathExpression_stack.peek()).fragments.add(((contextAwareExpression_scope)contextAwareExpression_stack.peek()).context); ((contextAwarePathExpression_scope)contextAwarePathExpression_stack.peek()).fragments.add(new ObjectAccess((start!=null?start.getText():null)));
            }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:189:5: ( ( '.' (field= ID ) ) | arrayAccess )*
            loop23:
            do {
                int alt23=3;
                alt23 = dfa23.predict(input);
                switch (alt23) {
            	case 1 :
            	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:189:7: ( '.' (field= ID ) )
            	    {
            	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:189:7: ( '.' (field= ID ) )
            	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:189:8: '.' (field= ID )
            	    {
            	    char_literal44=(Token)match(input,57,FOLLOW_57_in_contextAwarePathExpression954); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_57.add(char_literal44);

            	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:189:12: (field= ID )
            	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:189:13: field= ID
            	    {
            	    field=(Token)match(input,ID,FOLLOW_ID_in_contextAwarePathExpression959); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_ID.add(field);

            	    if ( state.backtracking==0 ) {
            	       ((contextAwarePathExpression_scope)contextAwarePathExpression_stack.peek()).fragments.add(new ObjectAccess((field!=null?field.getText():null))); 
            	    }

            	    }


            	    }


            	    }
            	    break;
            	case 2 :
            	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:190:11: arrayAccess
            	    {
            	    pushFollow(FOLLOW_arrayAccess_in_contextAwarePathExpression977);
            	    arrayAccess45=arrayAccess();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_arrayAccess.add(arrayAccess45.getTree());
            	    if ( state.backtracking==0 ) {
            	       ((contextAwarePathExpression_scope)contextAwarePathExpression_stack.peek()).fragments.add((arrayAccess45!=null?((EvaluationExpression)arrayAccess45.tree):null)); 
            	    }

            	    }
            	    break;

            	default :
            	    break loop23;
                }
            } while (true);



            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 190:93: -> ^( EXPRESSION[\"PathExpression\"] )
            {
                // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:190:97: ^( EXPRESSION[\"PathExpression\"] )
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "PathExpression"), root_1);

                adaptor.addChild(root_1,  ((contextAwarePathExpression_scope)contextAwarePathExpression_stack.peek()).fragments );

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 20, contextAwarePathExpression_StartIndex); }
            contextAwarePathExpression_stack.pop();
        }
        return retval;
    }
    // $ANTLR end "contextAwarePathExpression"

    protected static class pathExpression_scope {
        List<EvaluationExpression> fragments;
    }
    protected Stack pathExpression_stack = new Stack();

    public static class pathExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "pathExpression"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:192:1: pathExpression : ( valueExpression ( ( '.' (field= ID ) ) | arrayAccess )+ -> ^( EXPRESSION[\"PathExpression\"] ) | valueExpression );
    public final SJaqlParser.pathExpression_return pathExpression() throws RecognitionException {
        pathExpression_stack.push(new pathExpression_scope());
        SJaqlParser.pathExpression_return retval = new SJaqlParser.pathExpression_return();
        retval.start = input.LT(1);
        int pathExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token field=null;
        Token char_literal47=null;
        SJaqlParser.valueExpression_return valueExpression46 = null;

        SJaqlParser.arrayAccess_return arrayAccess48 = null;

        SJaqlParser.valueExpression_return valueExpression49 = null;


        EvaluationExpression field_tree=null;
        EvaluationExpression char_literal47_tree=null;
        RewriteRuleTokenStream stream_57=new RewriteRuleTokenStream(adaptor,"token 57");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_arrayAccess=new RewriteRuleSubtreeStream(adaptor,"rule arrayAccess");
        RewriteRuleSubtreeStream stream_valueExpression=new RewriteRuleSubtreeStream(adaptor,"rule valueExpression");
         ((pathExpression_scope)pathExpression_stack.peek()).fragments = new ArrayList<EvaluationExpression>(); 
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 21) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:195:3: ( valueExpression ( ( '.' (field= ID ) ) | arrayAccess )+ -> ^( EXPRESSION[\"PathExpression\"] ) | valueExpression )
            int alt25=2;
            alt25 = dfa25.predict(input);
            switch (alt25) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:196:5: valueExpression ( ( '.' (field= ID ) ) | arrayAccess )+
                    {
                    pushFollow(FOLLOW_valueExpression_in_pathExpression1019);
                    valueExpression46=valueExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_valueExpression.add(valueExpression46.getTree());
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:198:5: ( ( '.' (field= ID ) ) | arrayAccess )+
                    int cnt24=0;
                    loop24:
                    do {
                        int alt24=3;
                        int LA24_0 = input.LA(1);

                        if ( (LA24_0==57) ) {
                            int LA24_2 = input.LA(2);

                            if ( (synpred36_SJaql()) ) {
                                alt24=1;
                            }


                        }
                        else if ( (LA24_0==63) ) {
                            int LA24_3 = input.LA(2);

                            if ( (synpred37_SJaql()) ) {
                                alt24=2;
                            }


                        }


                        switch (alt24) {
                    	case 1 :
                    	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:198:7: ( '.' (field= ID ) )
                    	    {
                    	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:198:7: ( '.' (field= ID ) )
                    	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:198:8: '.' (field= ID )
                    	    {
                    	    char_literal47=(Token)match(input,57,FOLLOW_57_in_pathExpression1033); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_57.add(char_literal47);

                    	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:198:12: (field= ID )
                    	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:198:13: field= ID
                    	    {
                    	    field=(Token)match(input,ID,FOLLOW_ID_in_pathExpression1038); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_ID.add(field);

                    	    if ( state.backtracking==0 ) {
                    	       ((pathExpression_scope)pathExpression_stack.peek()).fragments.add(new ObjectAccess((field!=null?field.getText():null))); 
                    	    }

                    	    }


                    	    }


                    	    }
                    	    break;
                    	case 2 :
                    	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:199:11: arrayAccess
                    	    {
                    	    pushFollow(FOLLOW_arrayAccess_in_pathExpression1056);
                    	    arrayAccess48=arrayAccess();

                    	    state._fsp--;
                    	    if (state.failed) return retval;
                    	    if ( state.backtracking==0 ) stream_arrayAccess.add(arrayAccess48.getTree());
                    	    if ( state.backtracking==0 ) {
                    	       ((pathExpression_scope)pathExpression_stack.peek()).fragments.add((arrayAccess48!=null?((EvaluationExpression)arrayAccess48.tree):null)); 
                    	    }

                    	    }
                    	    break;

                    	default :
                    	    if ( cnt24 >= 1 ) break loop24;
                    	    if (state.backtracking>0) {state.failed=true; return retval;}
                                EarlyExitException eee =
                                    new EarlyExitException(24, input);
                                throw eee;
                        }
                        cnt24++;
                    } while (true);

                    if ( state.backtracking==0 ) {
                       ((pathExpression_scope)pathExpression_stack.peek()).fragments.add(0, (valueExpression46!=null?((EvaluationExpression)valueExpression46.tree):null)); 
                    }


                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 199:143: -> ^( EXPRESSION[\"PathExpression\"] )
                    {
                        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:199:147: ^( EXPRESSION[\"PathExpression\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "PathExpression"), root_1);

                        adaptor.addChild(root_1,  ((pathExpression_scope)pathExpression_stack.peek()).fragments );

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:201:5: valueExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_valueExpression_in_pathExpression1083);
                    valueExpression49=valueExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, valueExpression49.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 21, pathExpression_StartIndex); }
            pathExpression_stack.pop();
        }
        return retval;
    }
    // $ANTLR end "pathExpression"

    public static class valueExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "valueExpression"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:203:1: valueExpression : ( methodCall[MethodCall.NO_TARGET] | parenthesesExpression | literal | VAR -> | ID -> | arrayCreation | objectCreation | operatorExpression );
    public final SJaqlParser.valueExpression_return valueExpression() throws RecognitionException {
        SJaqlParser.valueExpression_return retval = new SJaqlParser.valueExpression_return();
        retval.start = input.LT(1);
        int valueExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token VAR53=null;
        Token ID54=null;
        SJaqlParser.methodCall_return methodCall50 = null;

        SJaqlParser.parenthesesExpression_return parenthesesExpression51 = null;

        SJaqlParser.literal_return literal52 = null;

        SJaqlParser.arrayCreation_return arrayCreation55 = null;

        SJaqlParser.objectCreation_return objectCreation56 = null;

        SJaqlParser.operatorExpression_return operatorExpression57 = null;


        EvaluationExpression VAR53_tree=null;
        EvaluationExpression ID54_tree=null;
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 22) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:204:2: ( methodCall[MethodCall.NO_TARGET] | parenthesesExpression | literal | VAR -> | ID -> | arrayCreation | objectCreation | operatorExpression )
            int alt26=8;
            alt26 = dfa26.predict(input);
            switch (alt26) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:204:4: methodCall[MethodCall.NO_TARGET]
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_methodCall_in_valueExpression1092);
                    methodCall50=methodCall(MethodCall.NO_TARGET);

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, methodCall50.getTree());

                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:205:4: parenthesesExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_parenthesesExpression_in_valueExpression1098);
                    parenthesesExpression51=parenthesesExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, parenthesesExpression51.getTree());

                    }
                    break;
                case 3 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:206:4: literal
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_literal_in_valueExpression1104);
                    literal52=literal();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, literal52.getTree());

                    }
                    break;
                case 4 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:207:4: VAR
                    {
                    VAR53=(Token)match(input,VAR,FOLLOW_VAR_in_valueExpression1110); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_VAR.add(VAR53);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 207:8: ->
                    {
                        adaptor.addChild(root_0,  makePath(VAR53) );

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 5 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:208:4: ID
                    {
                    ID54=(Token)match(input,ID,FOLLOW_ID_in_valueExpression1119); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(ID54);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 208:7: ->
                    {
                        adaptor.addChild(root_0,  getBinding(ID54, EvaluationExpression.class) );

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 6 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:209:4: arrayCreation
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_arrayCreation_in_valueExpression1128);
                    arrayCreation55=arrayCreation();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, arrayCreation55.getTree());

                    }
                    break;
                case 7 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:210:4: objectCreation
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_objectCreation_in_valueExpression1134);
                    objectCreation56=objectCreation();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, objectCreation56.getTree());

                    }
                    break;
                case 8 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:211:4: operatorExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_operatorExpression_in_valueExpression1140);
                    operatorExpression57=operatorExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, operatorExpression57.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 22, valueExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "valueExpression"

    public static class operatorExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "operatorExpression"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:213:1: operatorExpression : operator -> ^( EXPRESSION[\"ErroneousExpression\"] ) ;
    public final SJaqlParser.operatorExpression_return operatorExpression() throws RecognitionException {
        SJaqlParser.operatorExpression_return retval = new SJaqlParser.operatorExpression_return();
        retval.start = input.LT(1);
        int operatorExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        SJaqlParser.operator_return operator58 = null;


        RewriteRuleSubtreeStream stream_operator=new RewriteRuleSubtreeStream(adaptor,"rule operator");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 23) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:214:2: ( operator -> ^( EXPRESSION[\"ErroneousExpression\"] ) )
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:214:4: operator
            {
            pushFollow(FOLLOW_operator_in_operatorExpression1150);
            operator58=operator();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_operator.add(operator58.getTree());


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 214:13: -> ^( EXPRESSION[\"ErroneousExpression\"] )
            {
                // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:214:16: ^( EXPRESSION[\"ErroneousExpression\"] )
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ErroneousExpression"), root_1);

                adaptor.addChild(root_1,  "op" );

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 23, operatorExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "operatorExpression"

    public static class parenthesesExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "parenthesesExpression"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:216:1: parenthesesExpression : ( '(' expression ')' ) -> expression ;
    public final SJaqlParser.parenthesesExpression_return parenthesesExpression() throws RecognitionException {
        SJaqlParser.parenthesesExpression_return retval = new SJaqlParser.parenthesesExpression_return();
        retval.start = input.LT(1);
        int parenthesesExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token char_literal59=null;
        Token char_literal61=null;
        SJaqlParser.expression_return expression60 = null;


        EvaluationExpression char_literal59_tree=null;
        EvaluationExpression char_literal61_tree=null;
        RewriteRuleTokenStream stream_30=new RewriteRuleTokenStream(adaptor,"token 30");
        RewriteRuleTokenStream stream_32=new RewriteRuleTokenStream(adaptor,"token 32");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 24) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:217:2: ( ( '(' expression ')' ) -> expression )
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:217:4: ( '(' expression ')' )
            {
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:217:4: ( '(' expression ')' )
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:217:5: '(' expression ')'
            {
            char_literal59=(Token)match(input,30,FOLLOW_30_in_parenthesesExpression1171); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_30.add(char_literal59);

            pushFollow(FOLLOW_expression_in_parenthesesExpression1173);
            expression60=expression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_expression.add(expression60.getTree());
            char_literal61=(Token)match(input,32,FOLLOW_32_in_parenthesesExpression1175); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_32.add(char_literal61);


            }



            // AST REWRITE
            // elements: expression
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 217:25: -> expression
            {
                adaptor.addChild(root_0, stream_expression.nextTree());

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 24, parenthesesExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "parenthesesExpression"

    public static class methodCall_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "methodCall"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:219:1: methodCall[EvaluationExpression targetExpr] : name= ID '(' (param= expression ( ',' param= expression )* )? ')' ->;
    public final SJaqlParser.methodCall_return methodCall(EvaluationExpression targetExpr) throws RecognitionException {
        SJaqlParser.methodCall_return retval = new SJaqlParser.methodCall_return();
        retval.start = input.LT(1);
        int methodCall_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token name=null;
        Token char_literal62=null;
        Token char_literal63=null;
        Token char_literal64=null;
        SJaqlParser.expression_return param = null;


        EvaluationExpression name_tree=null;
        EvaluationExpression char_literal62_tree=null;
        EvaluationExpression char_literal63_tree=null;
        EvaluationExpression char_literal64_tree=null;
        RewriteRuleTokenStream stream_30=new RewriteRuleTokenStream(adaptor,"token 30");
        RewriteRuleTokenStream stream_32=new RewriteRuleTokenStream(adaptor,"token 32");
        RewriteRuleTokenStream stream_31=new RewriteRuleTokenStream(adaptor,"token 31");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
         List<EvaluationExpression> params = new ArrayList(); 
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 25) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:221:2: (name= ID '(' (param= expression ( ',' param= expression )* )? ')' ->)
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:221:4: name= ID '(' (param= expression ( ',' param= expression )* )? ')'
            {
            name=(Token)match(input,ID,FOLLOW_ID_in_methodCall1198); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);

            char_literal62=(Token)match(input,30,FOLLOW_30_in_methodCall1200); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_30.add(char_literal62);

            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:222:2: (param= expression ( ',' param= expression )* )?
            int alt28=2;
            int LA28_0 = input.LA(1);

            if ( ((LA28_0>=ID && LA28_0<=STRING)||(LA28_0>=DECIMAL && LA28_0<=UINT)||LA28_0==30||(LA28_0>=52 && LA28_0<=55)||LA28_0==58||(LA28_0>=60 && LA28_0<=63)||(LA28_0>=65 && LA28_0<=66)) ) {
                alt28=1;
            }
            switch (alt28) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:222:3: param= expression ( ',' param= expression )*
                    {
                    pushFollow(FOLLOW_expression_in_methodCall1207);
                    param=expression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_expression.add(param.getTree());
                    if ( state.backtracking==0 ) {
                       params.add((param!=null?((EvaluationExpression)param.tree):null)); 
                    }
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:223:2: ( ',' param= expression )*
                    loop27:
                    do {
                        int alt27=2;
                        int LA27_0 = input.LA(1);

                        if ( (LA27_0==31) ) {
                            alt27=1;
                        }


                        switch (alt27) {
                    	case 1 :
                    	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:223:3: ',' param= expression
                    	    {
                    	    char_literal63=(Token)match(input,31,FOLLOW_31_in_methodCall1213); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_31.add(char_literal63);

                    	    pushFollow(FOLLOW_expression_in_methodCall1217);
                    	    param=expression();

                    	    state._fsp--;
                    	    if (state.failed) return retval;
                    	    if ( state.backtracking==0 ) stream_expression.add(param.getTree());
                    	    if ( state.backtracking==0 ) {
                    	       params.add((param!=null?((EvaluationExpression)param.tree):null)); 
                    	    }

                    	    }
                    	    break;

                    	default :
                    	    break loop27;
                        }
                    } while (true);


                    }
                    break;

            }

            char_literal64=(Token)match(input,32,FOLLOW_32_in_methodCall1227); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_32.add(char_literal64);



            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 224:6: ->
            {
                adaptor.addChild(root_0,  createCheckedMethodCall(name, targetExpr, params.toArray(new EvaluationExpression[params.size()])) );

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 25, methodCall_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "methodCall"

    public static class fieldAssignment_return extends ParserRuleReturnScope {
        public ObjectCreation.Mapping mapping;
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "fieldAssignment"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:226:1: fieldAssignment returns [ObjectCreation.Mapping mapping] : ( VAR '.' STAR -> | VAR '.' ID -> | VAR -> | ID ':' expression ->);
    public final SJaqlParser.fieldAssignment_return fieldAssignment() throws RecognitionException {
        SJaqlParser.fieldAssignment_return retval = new SJaqlParser.fieldAssignment_return();
        retval.start = input.LT(1);
        int fieldAssignment_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token VAR65=null;
        Token char_literal66=null;
        Token STAR67=null;
        Token VAR68=null;
        Token char_literal69=null;
        Token ID70=null;
        Token VAR71=null;
        Token ID72=null;
        Token char_literal73=null;
        SJaqlParser.expression_return expression74 = null;


        EvaluationExpression VAR65_tree=null;
        EvaluationExpression char_literal66_tree=null;
        EvaluationExpression STAR67_tree=null;
        EvaluationExpression VAR68_tree=null;
        EvaluationExpression char_literal69_tree=null;
        EvaluationExpression ID70_tree=null;
        EvaluationExpression VAR71_tree=null;
        EvaluationExpression ID72_tree=null;
        EvaluationExpression char_literal73_tree=null;
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_STAR=new RewriteRuleTokenStream(adaptor,"token STAR");
        RewriteRuleTokenStream stream_57=new RewriteRuleTokenStream(adaptor,"token 57");
        RewriteRuleTokenStream stream_35=new RewriteRuleTokenStream(adaptor,"token 35");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 26) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:227:2: ( VAR '.' STAR -> | VAR '.' ID -> | VAR -> | ID ':' expression ->)
            int alt29=4;
            int LA29_0 = input.LA(1);

            if ( (LA29_0==VAR) ) {
                int LA29_1 = input.LA(2);

                if ( (LA29_1==57) ) {
                    int LA29_3 = input.LA(3);

                    if ( (LA29_3==STAR) ) {
                        alt29=1;
                    }
                    else if ( (LA29_3==ID) ) {
                        alt29=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 29, 3, input);

                        throw nvae;
                    }
                }
                else if ( (LA29_1==EOF||LA29_1==31||LA29_1==59) ) {
                    alt29=3;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 29, 1, input);

                    throw nvae;
                }
            }
            else if ( (LA29_0==ID) ) {
                alt29=4;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 29, 0, input);

                throw nvae;
            }
            switch (alt29) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:227:4: VAR '.' STAR
                    {
                    VAR65=(Token)match(input,VAR,FOLLOW_VAR_in_fieldAssignment1245); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_VAR.add(VAR65);

                    char_literal66=(Token)match(input,57,FOLLOW_57_in_fieldAssignment1247); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_57.add(char_literal66);

                    STAR67=(Token)match(input,STAR,FOLLOW_STAR_in_fieldAssignment1249); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STAR.add(STAR67);

                    if ( state.backtracking==0 ) {
                       ((objectCreation_scope)objectCreation_stack.peek()).mappings.add(new ObjectCreation.CopyFields(makePath(VAR65))); 
                    }


                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 227:99: ->
                    {
                        root_0 = null;
                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:228:5: VAR '.' ID
                    {
                    VAR68=(Token)match(input,VAR,FOLLOW_VAR_in_fieldAssignment1259); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_VAR.add(VAR68);

                    char_literal69=(Token)match(input,57,FOLLOW_57_in_fieldAssignment1261); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_57.add(char_literal69);

                    ID70=(Token)match(input,ID,FOLLOW_ID_in_fieldAssignment1263); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(ID70);

                    if ( state.backtracking==0 ) {
                       ((objectCreation_scope)objectCreation_stack.peek()).mappings.add(new ObjectCreation.Mapping((ID70!=null?ID70.getText():null), makePath(VAR68, (ID70!=null?ID70.getText():null)))); 
                    }


                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 228:115: ->
                    {
                        root_0 = null;
                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 3 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:229:5: VAR
                    {
                    VAR71=(Token)match(input,VAR,FOLLOW_VAR_in_fieldAssignment1273); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_VAR.add(VAR71);

                    if ( state.backtracking==0 ) {
                       ((objectCreation_scope)objectCreation_stack.peek()).mappings.add(new ObjectCreation.Mapping((VAR71!=null?VAR71.getText():null).substring(1), makePath(VAR71))); 
                    }


                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 229:112: ->
                    {
                        root_0 = null;
                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 4 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:230:4: ID ':' expression
                    {
                    ID72=(Token)match(input,ID,FOLLOW_ID_in_fieldAssignment1282); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(ID72);

                    char_literal73=(Token)match(input,35,FOLLOW_35_in_fieldAssignment1284); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_35.add(char_literal73);

                    pushFollow(FOLLOW_expression_in_fieldAssignment1286);
                    expression74=expression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_expression.add(expression74.getTree());
                    if ( state.backtracking==0 ) {
                       ((objectCreation_scope)objectCreation_stack.peek()).mappings.add(new ObjectCreation.Mapping((ID72!=null?ID72.getText():null), (expression74!=null?((EvaluationExpression)expression74.tree):null))); 
                    }


                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 230:113: ->
                    {
                        root_0 = null;
                    }

                    retval.tree = root_0;}
                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 26, fieldAssignment_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "fieldAssignment"

    protected static class objectCreation_scope {
        List<ObjectCreation.Mapping> mappings;
    }
    protected Stack objectCreation_stack = new Stack();

    public static class objectCreation_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "objectCreation"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:232:1: objectCreation : '{' ( fieldAssignment ( ',' fieldAssignment )* ( ',' )? )? '}' -> ^( EXPRESSION[\"ObjectCreation\"] ) ;
    public final SJaqlParser.objectCreation_return objectCreation() throws RecognitionException {
        objectCreation_stack.push(new objectCreation_scope());
        SJaqlParser.objectCreation_return retval = new SJaqlParser.objectCreation_return();
        retval.start = input.LT(1);
        int objectCreation_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token char_literal75=null;
        Token char_literal77=null;
        Token char_literal79=null;
        Token char_literal80=null;
        SJaqlParser.fieldAssignment_return fieldAssignment76 = null;

        SJaqlParser.fieldAssignment_return fieldAssignment78 = null;


        EvaluationExpression char_literal75_tree=null;
        EvaluationExpression char_literal77_tree=null;
        EvaluationExpression char_literal79_tree=null;
        EvaluationExpression char_literal80_tree=null;
        RewriteRuleTokenStream stream_59=new RewriteRuleTokenStream(adaptor,"token 59");
        RewriteRuleTokenStream stream_58=new RewriteRuleTokenStream(adaptor,"token 58");
        RewriteRuleTokenStream stream_31=new RewriteRuleTokenStream(adaptor,"token 31");
        RewriteRuleSubtreeStream stream_fieldAssignment=new RewriteRuleSubtreeStream(adaptor,"rule fieldAssignment");
         ((objectCreation_scope)objectCreation_stack.peek()).mappings = new ArrayList<ObjectCreation.Mapping>(); 
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 27) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:235:2: ( '{' ( fieldAssignment ( ',' fieldAssignment )* ( ',' )? )? '}' -> ^( EXPRESSION[\"ObjectCreation\"] ) )
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:235:4: '{' ( fieldAssignment ( ',' fieldAssignment )* ( ',' )? )? '}'
            {
            char_literal75=(Token)match(input,58,FOLLOW_58_in_objectCreation1308); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_58.add(char_literal75);

            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:235:8: ( fieldAssignment ( ',' fieldAssignment )* ( ',' )? )?
            int alt32=2;
            int LA32_0 = input.LA(1);

            if ( ((LA32_0>=ID && LA32_0<=VAR)) ) {
                alt32=1;
            }
            switch (alt32) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:235:9: fieldAssignment ( ',' fieldAssignment )* ( ',' )?
                    {
                    pushFollow(FOLLOW_fieldAssignment_in_objectCreation1311);
                    fieldAssignment76=fieldAssignment();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_fieldAssignment.add(fieldAssignment76.getTree());
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:235:25: ( ',' fieldAssignment )*
                    loop30:
                    do {
                        int alt30=2;
                        int LA30_0 = input.LA(1);

                        if ( (LA30_0==31) ) {
                            int LA30_1 = input.LA(2);

                            if ( ((LA30_1>=ID && LA30_1<=VAR)) ) {
                                alt30=1;
                            }


                        }


                        switch (alt30) {
                    	case 1 :
                    	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:235:26: ',' fieldAssignment
                    	    {
                    	    char_literal77=(Token)match(input,31,FOLLOW_31_in_objectCreation1314); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_31.add(char_literal77);

                    	    pushFollow(FOLLOW_fieldAssignment_in_objectCreation1316);
                    	    fieldAssignment78=fieldAssignment();

                    	    state._fsp--;
                    	    if (state.failed) return retval;
                    	    if ( state.backtracking==0 ) stream_fieldAssignment.add(fieldAssignment78.getTree());

                    	    }
                    	    break;

                    	default :
                    	    break loop30;
                        }
                    } while (true);

                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:235:48: ( ',' )?
                    int alt31=2;
                    int LA31_0 = input.LA(1);

                    if ( (LA31_0==31) ) {
                        alt31=1;
                    }
                    switch (alt31) {
                        case 1 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: ','
                            {
                            char_literal79=(Token)match(input,31,FOLLOW_31_in_objectCreation1320); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_31.add(char_literal79);


                            }
                            break;

                    }


                    }
                    break;

            }

            char_literal80=(Token)match(input,59,FOLLOW_59_in_objectCreation1325); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_59.add(char_literal80);



            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 235:59: -> ^( EXPRESSION[\"ObjectCreation\"] )
            {
                // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:235:62: ^( EXPRESSION[\"ObjectCreation\"] )
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ObjectCreation"), root_1);

                adaptor.addChild(root_1,  ((objectCreation_scope)objectCreation_stack.peek()).mappings );

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 27, objectCreation_StartIndex); }
            objectCreation_stack.pop();
        }
        return retval;
    }
    // $ANTLR end "objectCreation"

    public static class literal_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "literal"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:241:1: literal : (val= 'true' -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= 'false' -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= DECIMAL -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= STRING -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= INTEGER -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= UINT -> ^( EXPRESSION[\"ConstantExpression\"] ) | 'null' ->);
    public final SJaqlParser.literal_return literal() throws RecognitionException {
        SJaqlParser.literal_return retval = new SJaqlParser.literal_return();
        retval.start = input.LT(1);
        int literal_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token val=null;
        Token string_literal81=null;

        EvaluationExpression val_tree=null;
        EvaluationExpression string_literal81_tree=null;
        RewriteRuleTokenStream stream_INTEGER=new RewriteRuleTokenStream(adaptor,"token INTEGER");
        RewriteRuleTokenStream stream_DECIMAL=new RewriteRuleTokenStream(adaptor,"token DECIMAL");
        RewriteRuleTokenStream stream_62=new RewriteRuleTokenStream(adaptor,"token 62");
        RewriteRuleTokenStream stream_UINT=new RewriteRuleTokenStream(adaptor,"token UINT");
        RewriteRuleTokenStream stream_60=new RewriteRuleTokenStream(adaptor,"token 60");
        RewriteRuleTokenStream stream_61=new RewriteRuleTokenStream(adaptor,"token 61");
        RewriteRuleTokenStream stream_STRING=new RewriteRuleTokenStream(adaptor,"token STRING");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 28) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:242:2: (val= 'true' -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= 'false' -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= DECIMAL -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= STRING -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= INTEGER -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= UINT -> ^( EXPRESSION[\"ConstantExpression\"] ) | 'null' ->)
            int alt33=7;
            switch ( input.LA(1) ) {
            case 60:
                {
                alt33=1;
                }
                break;
            case 61:
                {
                alt33=2;
                }
                break;
            case DECIMAL:
                {
                alt33=3;
                }
                break;
            case STRING:
                {
                alt33=4;
                }
                break;
            case INTEGER:
                {
                alt33=5;
                }
                break;
            case UINT:
                {
                alt33=6;
                }
                break;
            case 62:
                {
                alt33=7;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 33, 0, input);

                throw nvae;
            }

            switch (alt33) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:242:4: val= 'true'
                    {
                    val=(Token)match(input,60,FOLLOW_60_in_literal1349); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_60.add(val);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 242:15: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:242:18: ^( EXPRESSION[\"ConstantExpression\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ConstantExpression"), root_1);

                        adaptor.addChild(root_1,  Boolean.TRUE );

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:243:4: val= 'false'
                    {
                    val=(Token)match(input,61,FOLLOW_61_in_literal1365); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_61.add(val);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 243:16: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:243:19: ^( EXPRESSION[\"ConstantExpression\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ConstantExpression"), root_1);

                        adaptor.addChild(root_1,  Boolean.FALSE );

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 3 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:244:4: val= DECIMAL
                    {
                    val=(Token)match(input,DECIMAL,FOLLOW_DECIMAL_in_literal1381); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_DECIMAL.add(val);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 244:16: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:244:19: ^( EXPRESSION[\"ConstantExpression\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ConstantExpression"), root_1);

                        adaptor.addChild(root_1,  new BigDecimal((val!=null?val.getText():null)) );

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 4 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:245:4: val= STRING
                    {
                    val=(Token)match(input,STRING,FOLLOW_STRING_in_literal1397); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STRING.add(val);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 245:15: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:245:18: ^( EXPRESSION[\"ConstantExpression\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ConstantExpression"), root_1);

                        adaptor.addChild(root_1,  val.getText() );

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 5 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:246:5: val= INTEGER
                    {
                    val=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_literal1414); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_INTEGER.add(val);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 246:17: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:246:20: ^( EXPRESSION[\"ConstantExpression\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ConstantExpression"), root_1);

                        adaptor.addChild(root_1,  parseInt((val!=null?val.getText():null)) );

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 6 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:247:5: val= UINT
                    {
                    val=(Token)match(input,UINT,FOLLOW_UINT_in_literal1431); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_UINT.add(val);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 247:14: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:247:17: ^( EXPRESSION[\"ConstantExpression\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ConstantExpression"), root_1);

                        adaptor.addChild(root_1,  parseInt((val!=null?val.getText():null)) );

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 7 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:248:5: 'null'
                    {
                    string_literal81=(Token)match(input,62,FOLLOW_62_in_literal1446); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_62.add(string_literal81);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 248:12: ->
                    {
                        adaptor.addChild(root_0,  EvaluationExpression.NULL );

                    }

                    retval.tree = root_0;}
                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 28, literal_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "literal"

    public static class arrayAccess_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "arrayAccess"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:250:1: arrayAccess : ( '[' STAR ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) | '[' (pos= INTEGER | pos= UINT ) ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) | '[' (start= INTEGER | start= UINT ) ':' (end= INTEGER | end= UINT ) ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) );
    public final SJaqlParser.arrayAccess_return arrayAccess() throws RecognitionException {
        SJaqlParser.arrayAccess_return retval = new SJaqlParser.arrayAccess_return();
        retval.start = input.LT(1);
        int arrayAccess_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token pos=null;
        Token start=null;
        Token end=null;
        Token char_literal82=null;
        Token STAR83=null;
        Token char_literal84=null;
        Token char_literal85=null;
        Token char_literal86=null;
        Token char_literal87=null;
        Token char_literal88=null;
        Token char_literal89=null;

        EvaluationExpression pos_tree=null;
        EvaluationExpression start_tree=null;
        EvaluationExpression end_tree=null;
        EvaluationExpression char_literal82_tree=null;
        EvaluationExpression STAR83_tree=null;
        EvaluationExpression char_literal84_tree=null;
        EvaluationExpression char_literal85_tree=null;
        EvaluationExpression char_literal86_tree=null;
        EvaluationExpression char_literal87_tree=null;
        EvaluationExpression char_literal88_tree=null;
        EvaluationExpression char_literal89_tree=null;
        RewriteRuleTokenStream stream_INTEGER=new RewriteRuleTokenStream(adaptor,"token INTEGER");
        RewriteRuleTokenStream stream_STAR=new RewriteRuleTokenStream(adaptor,"token STAR");
        RewriteRuleTokenStream stream_35=new RewriteRuleTokenStream(adaptor,"token 35");
        RewriteRuleTokenStream stream_64=new RewriteRuleTokenStream(adaptor,"token 64");
        RewriteRuleTokenStream stream_UINT=new RewriteRuleTokenStream(adaptor,"token UINT");
        RewriteRuleTokenStream stream_63=new RewriteRuleTokenStream(adaptor,"token 63");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 29) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:251:3: ( '[' STAR ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) | '[' (pos= INTEGER | pos= UINT ) ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) | '[' (start= INTEGER | start= UINT ) ':' (end= INTEGER | end= UINT ) ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) )
            int alt37=3;
            int LA37_0 = input.LA(1);

            if ( (LA37_0==63) ) {
                switch ( input.LA(2) ) {
                case STAR:
                    {
                    alt37=1;
                    }
                    break;
                case INTEGER:
                    {
                    int LA37_3 = input.LA(3);

                    if ( (LA37_3==64) ) {
                        alt37=2;
                    }
                    else if ( (LA37_3==35) ) {
                        alt37=3;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 37, 3, input);

                        throw nvae;
                    }
                    }
                    break;
                case UINT:
                    {
                    int LA37_4 = input.LA(3);

                    if ( (LA37_4==64) ) {
                        alt37=2;
                    }
                    else if ( (LA37_4==35) ) {
                        alt37=3;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 37, 4, input);

                        throw nvae;
                    }
                    }
                    break;
                default:
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 37, 1, input);

                    throw nvae;
                }

            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 37, 0, input);

                throw nvae;
            }
            switch (alt37) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:251:5: '[' STAR ']'
                    {
                    char_literal82=(Token)match(input,63,FOLLOW_63_in_arrayAccess1460); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_63.add(char_literal82);

                    STAR83=(Token)match(input,STAR,FOLLOW_STAR_in_arrayAccess1462); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STAR.add(STAR83);

                    char_literal84=(Token)match(input,64,FOLLOW_64_in_arrayAccess1464); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_64.add(char_literal84);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 252:3: -> ^( EXPRESSION[\"ArrayAccess\"] )
                    {
                        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:252:6: ^( EXPRESSION[\"ArrayAccess\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ArrayAccess"), root_1);

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:253:5: '[' (pos= INTEGER | pos= UINT ) ']'
                    {
                    char_literal85=(Token)match(input,63,FOLLOW_63_in_arrayAccess1482); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_63.add(char_literal85);

                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:253:9: (pos= INTEGER | pos= UINT )
                    int alt34=2;
                    int LA34_0 = input.LA(1);

                    if ( (LA34_0==INTEGER) ) {
                        alt34=1;
                    }
                    else if ( (LA34_0==UINT) ) {
                        alt34=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 34, 0, input);

                        throw nvae;
                    }
                    switch (alt34) {
                        case 1 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:253:10: pos= INTEGER
                            {
                            pos=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_arrayAccess1487); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_INTEGER.add(pos);


                            }
                            break;
                        case 2 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:253:24: pos= UINT
                            {
                            pos=(Token)match(input,UINT,FOLLOW_UINT_in_arrayAccess1493); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_UINT.add(pos);


                            }
                            break;

                    }

                    char_literal86=(Token)match(input,64,FOLLOW_64_in_arrayAccess1496); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_64.add(char_literal86);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 254:3: -> ^( EXPRESSION[\"ArrayAccess\"] )
                    {
                        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:254:6: ^( EXPRESSION[\"ArrayAccess\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ArrayAccess"), root_1);

                        adaptor.addChild(root_1,  Integer.valueOf((pos!=null?pos.getText():null)) );

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 3 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:255:5: '[' (start= INTEGER | start= UINT ) ':' (end= INTEGER | end= UINT ) ']'
                    {
                    char_literal87=(Token)match(input,63,FOLLOW_63_in_arrayAccess1514); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_63.add(char_literal87);

                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:255:9: (start= INTEGER | start= UINT )
                    int alt35=2;
                    int LA35_0 = input.LA(1);

                    if ( (LA35_0==INTEGER) ) {
                        alt35=1;
                    }
                    else if ( (LA35_0==UINT) ) {
                        alt35=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 35, 0, input);

                        throw nvae;
                    }
                    switch (alt35) {
                        case 1 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:255:10: start= INTEGER
                            {
                            start=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_arrayAccess1519); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_INTEGER.add(start);


                            }
                            break;
                        case 2 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:255:26: start= UINT
                            {
                            start=(Token)match(input,UINT,FOLLOW_UINT_in_arrayAccess1525); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_UINT.add(start);


                            }
                            break;

                    }

                    char_literal88=(Token)match(input,35,FOLLOW_35_in_arrayAccess1528); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_35.add(char_literal88);

                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:255:42: (end= INTEGER | end= UINT )
                    int alt36=2;
                    int LA36_0 = input.LA(1);

                    if ( (LA36_0==INTEGER) ) {
                        alt36=1;
                    }
                    else if ( (LA36_0==UINT) ) {
                        alt36=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 36, 0, input);

                        throw nvae;
                    }
                    switch (alt36) {
                        case 1 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:255:43: end= INTEGER
                            {
                            end=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_arrayAccess1533); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_INTEGER.add(end);


                            }
                            break;
                        case 2 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:255:57: end= UINT
                            {
                            end=(Token)match(input,UINT,FOLLOW_UINT_in_arrayAccess1539); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_UINT.add(end);


                            }
                            break;

                    }

                    char_literal89=(Token)match(input,64,FOLLOW_64_in_arrayAccess1542); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_64.add(char_literal89);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 256:3: -> ^( EXPRESSION[\"ArrayAccess\"] )
                    {
                        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:256:6: ^( EXPRESSION[\"ArrayAccess\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ArrayAccess"), root_1);

                        adaptor.addChild(root_1,  Integer.valueOf((start!=null?start.getText():null)) );
                        adaptor.addChild(root_1,  Integer.valueOf((end!=null?end.getText():null)) );

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 29, arrayAccess_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "arrayAccess"

    public static class arrayCreation_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "arrayCreation"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:258:1: arrayCreation : '[' elems+= expression ( ',' elems+= expression )* ( ',' )? ']' -> ^( EXPRESSION[\"ArrayCreation\"] ) ;
    public final SJaqlParser.arrayCreation_return arrayCreation() throws RecognitionException {
        SJaqlParser.arrayCreation_return retval = new SJaqlParser.arrayCreation_return();
        retval.start = input.LT(1);
        int arrayCreation_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token char_literal90=null;
        Token char_literal91=null;
        Token char_literal92=null;
        Token char_literal93=null;
        List list_elems=null;
        RuleReturnScope elems = null;
        EvaluationExpression char_literal90_tree=null;
        EvaluationExpression char_literal91_tree=null;
        EvaluationExpression char_literal92_tree=null;
        EvaluationExpression char_literal93_tree=null;
        RewriteRuleTokenStream stream_31=new RewriteRuleTokenStream(adaptor,"token 31");
        RewriteRuleTokenStream stream_64=new RewriteRuleTokenStream(adaptor,"token 64");
        RewriteRuleTokenStream stream_63=new RewriteRuleTokenStream(adaptor,"token 63");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 30) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:259:2: ( '[' elems+= expression ( ',' elems+= expression )* ( ',' )? ']' -> ^( EXPRESSION[\"ArrayCreation\"] ) )
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:259:5: '[' elems+= expression ( ',' elems+= expression )* ( ',' )? ']'
            {
            char_literal90=(Token)match(input,63,FOLLOW_63_in_arrayCreation1567); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_63.add(char_literal90);

            pushFollow(FOLLOW_expression_in_arrayCreation1571);
            elems=expression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_expression.add(elems.getTree());
            if (list_elems==null) list_elems=new ArrayList();
            list_elems.add(elems.getTree());

            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:259:27: ( ',' elems+= expression )*
            loop38:
            do {
                int alt38=2;
                int LA38_0 = input.LA(1);

                if ( (LA38_0==31) ) {
                    int LA38_1 = input.LA(2);

                    if ( ((LA38_1>=ID && LA38_1<=STRING)||(LA38_1>=DECIMAL && LA38_1<=UINT)||LA38_1==30||(LA38_1>=52 && LA38_1<=55)||LA38_1==58||(LA38_1>=60 && LA38_1<=63)||(LA38_1>=65 && LA38_1<=66)) ) {
                        alt38=1;
                    }


                }


                switch (alt38) {
            	case 1 :
            	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:259:28: ',' elems+= expression
            	    {
            	    char_literal91=(Token)match(input,31,FOLLOW_31_in_arrayCreation1574); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_31.add(char_literal91);

            	    pushFollow(FOLLOW_expression_in_arrayCreation1578);
            	    elems=expression();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_expression.add(elems.getTree());
            	    if (list_elems==null) list_elems=new ArrayList();
            	    list_elems.add(elems.getTree());


            	    }
            	    break;

            	default :
            	    break loop38;
                }
            } while (true);

            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:259:52: ( ',' )?
            int alt39=2;
            int LA39_0 = input.LA(1);

            if ( (LA39_0==31) ) {
                alt39=1;
            }
            switch (alt39) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: ','
                    {
                    char_literal92=(Token)match(input,31,FOLLOW_31_in_arrayCreation1582); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_31.add(char_literal92);


                    }
                    break;

            }

            char_literal93=(Token)match(input,64,FOLLOW_64_in_arrayCreation1585); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_64.add(char_literal93);



            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 259:61: -> ^( EXPRESSION[\"ArrayCreation\"] )
            {
                // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:259:64: ^( EXPRESSION[\"ArrayCreation\"] )
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ArrayCreation"), root_1);

                adaptor.addChild(root_1,  list_elems.toArray(new EvaluationExpression[list_elems.size()]) );

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 30, arrayCreation_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "arrayCreation"

    protected static class operator_scope {
        List<String> inputNames;
        java.util.BitSet hasExplicitName;
        List<List<ExpressionTag>> inputTags;
        Operator result;
    }
    protected Stack operator_stack = new Stack();

    public static class operator_return extends ParserRuleReturnScope {
        public Operator op=null;
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "operator"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:261:1: operator returns [Operator op=null] : opRule= ( readOperator | writeOperator | genericOperator ) ;
    public final SJaqlParser.operator_return operator() throws RecognitionException {
        operator_stack.push(new operator_scope());
        SJaqlParser.operator_return retval = new SJaqlParser.operator_return();
        retval.start = input.LT(1);
        int operator_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token opRule=null;
        SJaqlParser.readOperator_return readOperator94 = null;

        SJaqlParser.writeOperator_return writeOperator95 = null;

        SJaqlParser.genericOperator_return genericOperator96 = null;


        EvaluationExpression opRule_tree=null;

         
          ((operator_scope)operator_stack.peek()).inputNames = new ArrayList<String>();
          ((operator_scope)operator_stack.peek()).inputTags = new ArrayList<List<ExpressionTag>>();
          ((operator_scope)operator_stack.peek()).hasExplicitName = new java.util.BitSet();

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 31) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:273:2: (opRule= ( readOperator | writeOperator | genericOperator ) )
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:273:4: opRule= ( readOperator | writeOperator | genericOperator )
            {
            root_0 = (EvaluationExpression)adaptor.nil();

            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:273:11: ( readOperator | writeOperator | genericOperator )
            int alt40=3;
            switch ( input.LA(1) ) {
            case 65:
                {
                alt40=1;
                }
                break;
            case 66:
                {
                alt40=2;
                }
                break;
            case ID:
                {
                alt40=3;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 40, 0, input);

                throw nvae;
            }

            switch (alt40) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:273:12: readOperator
                    {
                    pushFollow(FOLLOW_readOperator_in_operator1619);
                    readOperator94=readOperator();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, readOperator94.getTree());

                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:273:27: writeOperator
                    {
                    pushFollow(FOLLOW_writeOperator_in_operator1623);
                    writeOperator95=writeOperator();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, writeOperator95.getTree());

                    }
                    break;
                case 3 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:273:43: genericOperator
                    {
                    pushFollow(FOLLOW_genericOperator_in_operator1627);
                    genericOperator96=genericOperator();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, genericOperator96.getTree());

                    }
                    break;

            }

            if ( state.backtracking==0 ) {
               
                retval.op = ((operator_scope)operator_stack.peek()).result;

            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 31, operator_StartIndex); }
            operator_stack.pop();
        }
        return retval;
    }
    // $ANTLR end "operator"

    public static class readOperator_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "readOperator"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:278:1: readOperator : 'read' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' ) ->;
    public final SJaqlParser.readOperator_return readOperator() throws RecognitionException {
        SJaqlParser.readOperator_return retval = new SJaqlParser.readOperator_return();
        retval.start = input.LT(1);
        int readOperator_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token loc=null;
        Token file=null;
        Token string_literal97=null;
        Token char_literal98=null;
        Token char_literal99=null;

        EvaluationExpression loc_tree=null;
        EvaluationExpression file_tree=null;
        EvaluationExpression string_literal97_tree=null;
        EvaluationExpression char_literal98_tree=null;
        EvaluationExpression char_literal99_tree=null;
        RewriteRuleTokenStream stream_30=new RewriteRuleTokenStream(adaptor,"token 30");
        RewriteRuleTokenStream stream_32=new RewriteRuleTokenStream(adaptor,"token 32");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_65=new RewriteRuleTokenStream(adaptor,"token 65");
        RewriteRuleTokenStream stream_STRING=new RewriteRuleTokenStream(adaptor,"token STRING");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 32) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:279:2: ( 'read' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' ) ->)
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:279:4: 'read' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' )
            {
            string_literal97=(Token)match(input,65,FOLLOW_65_in_readOperator1641); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_65.add(string_literal97);

            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:279:11: ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' )
            int alt42=2;
            int LA42_0 = input.LA(1);

            if ( (LA42_0==ID) ) {
                int LA42_1 = input.LA(2);

                if ( (LA42_1==30) ) {
                    alt42=2;
                }
                else if ( (LA42_1==STRING) ) {
                    alt42=1;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 42, 1, input);

                    throw nvae;
                }
            }
            else if ( (LA42_0==STRING) ) {
                alt42=1;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 42, 0, input);

                throw nvae;
            }
            switch (alt42) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:279:12: (loc= ID )? file= STRING
                    {
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:279:15: (loc= ID )?
                    int alt41=2;
                    int LA41_0 = input.LA(1);

                    if ( (LA41_0==ID) ) {
                        alt41=1;
                    }
                    switch (alt41) {
                        case 1 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: loc= ID
                            {
                            loc=(Token)match(input,ID,FOLLOW_ID_in_readOperator1646); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_ID.add(loc);


                            }
                            break;

                    }

                    file=(Token)match(input,STRING,FOLLOW_STRING_in_readOperator1651); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STRING.add(file);


                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:279:34: loc= ID '(' file= STRING ')'
                    {
                    loc=(Token)match(input,ID,FOLLOW_ID_in_readOperator1657); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(loc);

                    char_literal98=(Token)match(input,30,FOLLOW_30_in_readOperator1659); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_30.add(char_literal98);

                    file=(Token)match(input,STRING,FOLLOW_STRING_in_readOperator1663); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STRING.add(file);

                    char_literal99=(Token)match(input,32,FOLLOW_32_in_readOperator1665); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_32.add(char_literal99);


                    }
                    break;

            }

            if ( state.backtracking==0 ) {
               ((operator_scope)operator_stack.peek()).result = new Source(JsonInputFormat.class, (file!=null?file.getText():null)); 
            }


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 279:133: ->
            {
                root_0 = null;
            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 32, readOperator_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "readOperator"

    public static class writeOperator_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "writeOperator"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:281:1: writeOperator : 'write' from= VAR 'to' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' ) ->;
    public final SJaqlParser.writeOperator_return writeOperator() throws RecognitionException {
        SJaqlParser.writeOperator_return retval = new SJaqlParser.writeOperator_return();
        retval.start = input.LT(1);
        int writeOperator_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token from=null;
        Token loc=null;
        Token file=null;
        Token string_literal100=null;
        Token string_literal101=null;
        Token char_literal102=null;
        Token char_literal103=null;

        EvaluationExpression from_tree=null;
        EvaluationExpression loc_tree=null;
        EvaluationExpression file_tree=null;
        EvaluationExpression string_literal100_tree=null;
        EvaluationExpression string_literal101_tree=null;
        EvaluationExpression char_literal102_tree=null;
        EvaluationExpression char_literal103_tree=null;
        RewriteRuleTokenStream stream_67=new RewriteRuleTokenStream(adaptor,"token 67");
        RewriteRuleTokenStream stream_66=new RewriteRuleTokenStream(adaptor,"token 66");
        RewriteRuleTokenStream stream_30=new RewriteRuleTokenStream(adaptor,"token 30");
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_32=new RewriteRuleTokenStream(adaptor,"token 32");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_STRING=new RewriteRuleTokenStream(adaptor,"token STRING");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 33) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:282:2: ( 'write' from= VAR 'to' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' ) ->)
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:282:4: 'write' from= VAR 'to' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' )
            {
            string_literal100=(Token)match(input,66,FOLLOW_66_in_writeOperator1679); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_66.add(string_literal100);

            from=(Token)match(input,VAR,FOLLOW_VAR_in_writeOperator1683); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_VAR.add(from);

            string_literal101=(Token)match(input,67,FOLLOW_67_in_writeOperator1685); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_67.add(string_literal101);

            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:282:26: ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' )
            int alt44=2;
            int LA44_0 = input.LA(1);

            if ( (LA44_0==ID) ) {
                int LA44_1 = input.LA(2);

                if ( (LA44_1==30) ) {
                    alt44=2;
                }
                else if ( (LA44_1==STRING) ) {
                    alt44=1;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 44, 1, input);

                    throw nvae;
                }
            }
            else if ( (LA44_0==STRING) ) {
                alt44=1;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 44, 0, input);

                throw nvae;
            }
            switch (alt44) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:282:27: (loc= ID )? file= STRING
                    {
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:282:30: (loc= ID )?
                    int alt43=2;
                    int LA43_0 = input.LA(1);

                    if ( (LA43_0==ID) ) {
                        alt43=1;
                    }
                    switch (alt43) {
                        case 1 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: loc= ID
                            {
                            loc=(Token)match(input,ID,FOLLOW_ID_in_writeOperator1690); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_ID.add(loc);


                            }
                            break;

                    }

                    file=(Token)match(input,STRING,FOLLOW_STRING_in_writeOperator1695); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STRING.add(file);


                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:282:49: loc= ID '(' file= STRING ')'
                    {
                    loc=(Token)match(input,ID,FOLLOW_ID_in_writeOperator1701); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(loc);

                    char_literal102=(Token)match(input,30,FOLLOW_30_in_writeOperator1703); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_30.add(char_literal102);

                    file=(Token)match(input,STRING,FOLLOW_STRING_in_writeOperator1707); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STRING.add(file);

                    char_literal103=(Token)match(input,32,FOLLOW_32_in_writeOperator1709); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_32.add(char_literal103);


                    }
                    break;

            }

            if ( state.backtracking==0 ) {
               
              	Sink sink = new Sink(JsonOutputFormat.class, (file!=null?file.getText():null));
                ((operator_scope)operator_stack.peek()).result = sink;
                sink.setInputs(getBinding(from, Operator.class));
                this.sinks.add(sink);

            }


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 288:3: ->
            {
                root_0 = null;
            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 33, writeOperator_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "writeOperator"

    protected static class genericOperator_scope {
        OperatorFactory.OperatorInfo operatorInfo;
    }
    protected Stack genericOperator_stack = new Stack();

    public static class genericOperator_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "genericOperator"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:290:1: genericOperator : name= ID {...}? => ( operatorFlag )* input ( ',' input )* ( operatorOption )* ->;
    public final SJaqlParser.genericOperator_return genericOperator() throws RecognitionException {
        genericOperator_stack.push(new genericOperator_scope());
        SJaqlParser.genericOperator_return retval = new SJaqlParser.genericOperator_return();
        retval.start = input.LT(1);
        int genericOperator_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token name=null;
        Token char_literal106=null;
        SJaqlParser.operatorFlag_return operatorFlag104 = null;

        SJaqlParser.input_return input105 = null;

        SJaqlParser.input_return input107 = null;

        SJaqlParser.operatorOption_return operatorOption108 = null;


        EvaluationExpression name_tree=null;
        EvaluationExpression char_literal106_tree=null;
        RewriteRuleTokenStream stream_31=new RewriteRuleTokenStream(adaptor,"token 31");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_operatorOption=new RewriteRuleSubtreeStream(adaptor,"rule operatorOption");
        RewriteRuleSubtreeStream stream_input=new RewriteRuleSubtreeStream(adaptor,"rule input");
        RewriteRuleSubtreeStream stream_operatorFlag=new RewriteRuleSubtreeStream(adaptor,"rule operatorFlag");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 34) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:293:3: (name= ID {...}? => ( operatorFlag )* input ( ',' input )* ( operatorOption )* ->)
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:293:5: name= ID {...}? => ( operatorFlag )* input ( ',' input )* ( operatorOption )*
            {
            name=(Token)match(input,ID,FOLLOW_ID_in_genericOperator1729); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);

            if ( !(( (((genericOperator_scope)genericOperator_stack.peek()).operatorInfo = findOperatorGreedily(name)) != null )) ) {
                if (state.backtracking>0) {state.failed=true; return retval;}
                throw new FailedPredicateException(input, "genericOperator", " ($genericOperator::operatorInfo = findOperatorGreedily($name)) != null ");
            }
            if ( state.backtracking==0 ) {
               
                ((operator_scope)operator_stack.peek()).result = ((genericOperator_scope)genericOperator_stack.peek()).operatorInfo.newInstance();

            }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:297:1: ( operatorFlag )*
            loop45:
            do {
                int alt45=2;
                int LA45_0 = input.LA(1);

                if ( (LA45_0==ID) ) {
                    alt45=1;
                }


                switch (alt45) {
            	case 1 :
            	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: operatorFlag
            	    {
            	    pushFollow(FOLLOW_operatorFlag_in_genericOperator1737);
            	    operatorFlag104=operatorFlag();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_operatorFlag.add(operatorFlag104.getTree());

            	    }
            	    break;

            	default :
            	    break loop45;
                }
            } while (true);

            pushFollow(FOLLOW_input_in_genericOperator1740);
            input105=input();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_input.add(input105.getTree());
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:298:7: ( ',' input )*
            loop46:
            do {
                int alt46=2;
                int LA46_0 = input.LA(1);

                if ( (LA46_0==31) ) {
                    int LA46_2 = input.LA(2);

                    if ( (synpred74_SJaql()) ) {
                        alt46=1;
                    }


                }


                switch (alt46) {
            	case 1 :
            	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:298:8: ',' input
            	    {
            	    char_literal106=(Token)match(input,31,FOLLOW_31_in_genericOperator1743); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_31.add(char_literal106);

            	    pushFollow(FOLLOW_input_in_genericOperator1745);
            	    input107=input();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_input.add(input107.getTree());

            	    }
            	    break;

            	default :
            	    break loop46;
                }
            } while (true);

            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:299:1: ( operatorOption )*
            loop47:
            do {
                int alt47=2;
                int LA47_0 = input.LA(1);

                if ( (LA47_0==ID) ) {
                    int LA47_2 = input.LA(2);

                    if ( (synpred75_SJaql()) ) {
                        alt47=1;
                    }


                }


                switch (alt47) {
            	case 1 :
            	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: operatorOption
            	    {
            	    pushFollow(FOLLOW_operatorOption_in_genericOperator1750);
            	    operatorOption108=operatorOption();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_operatorOption.add(operatorOption108.getTree());

            	    }
            	    break;

            	default :
            	    break loop47;
                }
            } while (true);



            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 299:17: ->
            {
                root_0 = null;
            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 34, genericOperator_StartIndex); }
            genericOperator_stack.pop();
        }
        return retval;
    }
    // $ANTLR end "genericOperator"

    protected static class operatorOption_scope {
        String optionName;
    }
    protected Stack operatorOption_stack = new Stack();

    public static class operatorOption_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "operatorOption"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:301:1: operatorOption : name= ID ({...}?moreName= ID )? expr= contextAwareExpression[null] ->;
    public final SJaqlParser.operatorOption_return operatorOption() throws RecognitionException {
        operatorOption_stack.push(new operatorOption_scope());
        SJaqlParser.operatorOption_return retval = new SJaqlParser.operatorOption_return();
        retval.start = input.LT(1);
        int operatorOption_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token name=null;
        Token moreName=null;
        SJaqlParser.contextAwareExpression_return expr = null;


        EvaluationExpression name_tree=null;
        EvaluationExpression moreName_tree=null;
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_contextAwareExpression=new RewriteRuleSubtreeStream(adaptor,"rule contextAwareExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 35) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:305:2: (name= ID ({...}?moreName= ID )? expr= contextAwareExpression[null] ->)
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:305:4: name= ID ({...}?moreName= ID )? expr= contextAwareExpression[null]
            {
            name=(Token)match(input,ID,FOLLOW_ID_in_operatorOption1770); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);

            if ( state.backtracking==0 ) {
               ((operatorOption_scope)operatorOption_stack.peek()).optionName = (name!=null?name.getText():null); 
            }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:306:1: ({...}?moreName= ID )?
            int alt48=2;
            alt48 = dfa48.predict(input);
            switch (alt48) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:306:2: {...}?moreName= ID
                    {
                    if ( !((!((genericOperator_scope)genericOperator_stack.peek()).operatorInfo.hasProperty(((operatorOption_scope)operatorOption_stack.peek()).optionName))) ) {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        throw new FailedPredicateException(input, "operatorOption", "!$genericOperator::operatorInfo.hasProperty($operatorOption::optionName)");
                    }
                    moreName=(Token)match(input,ID,FOLLOW_ID_in_operatorOption1779); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(moreName);

                    if ( state.backtracking==0 ) {
                       ((operatorOption_scope)operatorOption_stack.peek()).optionName = (name!=null?name.getText():null) + " " + (moreName!=null?moreName.getText():null);
                    }

                    }
                    break;

            }

            pushFollow(FOLLOW_contextAwareExpression_in_operatorOption1789);
            expr=contextAwareExpression(null);

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_contextAwareExpression.add(expr.getTree());
            if ( state.backtracking==0 ) {
               ((genericOperator_scope)genericOperator_stack.peek()).operatorInfo.setProperty(((operatorOption_scope)operatorOption_stack.peek()).optionName, ((operator_scope)operator_stack.peek()).result, (expr!=null?((EvaluationExpression)expr.tree):null)); 
            }


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 308:143: ->
            {
                root_0 = null;
            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 35, operatorOption_StartIndex); }
            operatorOption_stack.pop();
        }
        return retval;
    }
    // $ANTLR end "operatorOption"

    protected static class operatorFlag_scope {
        String flagName;
    }
    protected Stack operatorFlag_stack = new Stack();

    public static class operatorFlag_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "operatorFlag"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:310:1: operatorFlag : name= ID ({...}?moreName= ID )? ->;
    public final SJaqlParser.operatorFlag_return operatorFlag() throws RecognitionException {
        operatorFlag_stack.push(new operatorFlag_scope());
        SJaqlParser.operatorFlag_return retval = new SJaqlParser.operatorFlag_return();
        retval.start = input.LT(1);
        int operatorFlag_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token name=null;
        Token moreName=null;

        EvaluationExpression name_tree=null;
        EvaluationExpression moreName_tree=null;
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 36) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:314:3: (name= ID ({...}?moreName= ID )? ->)
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:314:5: name= ID ({...}?moreName= ID )?
            {
            name=(Token)match(input,ID,FOLLOW_ID_in_operatorFlag1810); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);

            if ( state.backtracking==0 ) {
               ((operatorFlag_scope)operatorFlag_stack.peek()).flagName = (name!=null?name.getText():null); 
            }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:315:1: ({...}?moreName= ID )?
            int alt49=2;
            int LA49_0 = input.LA(1);

            if ( (LA49_0==ID) ) {
                int LA49_1 = input.LA(2);

                if ( ((synpred77_SJaql()&&(!((genericOperator_scope)genericOperator_stack.peek()).operatorInfo.hasFlag(((operatorFlag_scope)operatorFlag_stack.peek()).flagName)))) ) {
                    alt49=1;
                }
            }
            switch (alt49) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:315:2: {...}?moreName= ID
                    {
                    if ( !((!((genericOperator_scope)genericOperator_stack.peek()).operatorInfo.hasFlag(((operatorFlag_scope)operatorFlag_stack.peek()).flagName))) ) {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        throw new FailedPredicateException(input, "operatorFlag", "!$genericOperator::operatorInfo.hasFlag($operatorFlag::flagName)");
                    }
                    moreName=(Token)match(input,ID,FOLLOW_ID_in_operatorFlag1820); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(moreName);

                    if ( state.backtracking==0 ) {
                       ((operatorFlag_scope)operatorFlag_stack.peek()).flagName = (name!=null?name.getText():null) + " " + (moreName!=null?moreName.getText():null);
                    }

                    }
                    break;

            }

            if ( state.backtracking==0 ) {
               setPropertySafely(((genericOperator_scope)genericOperator_stack.peek()).operatorInfo, ((operator_scope)operator_stack.peek()).result, ((operatorFlag_scope)operatorFlag_stack.peek()).flagName, true, name); 
            }


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 317:112: ->
            {
                root_0 = null;
            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 36, operatorFlag_StartIndex); }
            operatorFlag_stack.pop();
        }
        return retval;
    }
    // $ANTLR end "operatorFlag"

    public static class input_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "input"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:319:1: input : (preserveFlag= 'preserve' )? (name= VAR 'in' )? from= VAR (inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::inputNames.size() - 1)] )? ->;
    public final SJaqlParser.input_return input() throws RecognitionException {
        SJaqlParser.input_return retval = new SJaqlParser.input_return();
        retval.start = input.LT(1);
        int input_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token preserveFlag=null;
        Token name=null;
        Token from=null;
        Token inputOption=null;
        Token string_literal109=null;
        SJaqlParser.contextAwareExpression_return expr = null;


        EvaluationExpression preserveFlag_tree=null;
        EvaluationExpression name_tree=null;
        EvaluationExpression from_tree=null;
        EvaluationExpression inputOption_tree=null;
        EvaluationExpression string_literal109_tree=null;
        RewriteRuleTokenStream stream_68=new RewriteRuleTokenStream(adaptor,"token 68");
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_42=new RewriteRuleTokenStream(adaptor,"token 42");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_contextAwareExpression=new RewriteRuleSubtreeStream(adaptor,"rule contextAwareExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 37) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:320:2: ( (preserveFlag= 'preserve' )? (name= VAR 'in' )? from= VAR (inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::inputNames.size() - 1)] )? ->)
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:320:4: (preserveFlag= 'preserve' )? (name= VAR 'in' )? from= VAR (inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::inputNames.size() - 1)] )?
            {
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:320:16: (preserveFlag= 'preserve' )?
            int alt50=2;
            int LA50_0 = input.LA(1);

            if ( (LA50_0==68) ) {
                alt50=1;
            }
            switch (alt50) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: preserveFlag= 'preserve'
                    {
                    preserveFlag=(Token)match(input,68,FOLLOW_68_in_input1842); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_68.add(preserveFlag);


                    }
                    break;

            }

            if ( state.backtracking==0 ) {
            }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:320:32: (name= VAR 'in' )?
            int alt51=2;
            int LA51_0 = input.LA(1);

            if ( (LA51_0==VAR) ) {
                int LA51_1 = input.LA(2);

                if ( (LA51_1==42) ) {
                    int LA51_2 = input.LA(3);

                    if ( (LA51_2==VAR) ) {
                        int LA51_4 = input.LA(4);

                        if ( (synpred79_SJaql()) ) {
                            alt51=1;
                        }
                    }
                }
            }
            switch (alt51) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:320:33: name= VAR 'in'
                    {
                    name=(Token)match(input,VAR,FOLLOW_VAR_in_input1850); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_VAR.add(name);

                    string_literal109=(Token)match(input,42,FOLLOW_42_in_input1852); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_42.add(string_literal109);


                    }
                    break;

            }

            from=(Token)match(input,VAR,FOLLOW_VAR_in_input1858); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_VAR.add(from);

            if ( state.backtracking==0 ) {
               
                int inputIndex = ((operator_scope)operator_stack.peek()).inputNames.size();
                ((operator_scope)operator_stack.peek()).result.setInput(inputIndex, getBinding(from, Operator.class));
                ((operator_scope)operator_stack.peek()).inputNames.add(name != null ? name.getText() : from.getText());
                ((operator_scope)operator_stack.peek()).hasExplicitName.set(inputIndex, name != null); 
                ((operator_scope)operator_stack.peek()).inputTags.add(preserveFlag == null ? new ArrayList<ExpressionTag>() : Arrays.asList(ExpressionTag.RETAIN));

            }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:328:1: (inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::inputNames.size() - 1)] )?
            int alt52=2;
            alt52 = dfa52.predict(input);
            switch (alt52) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:328:2: inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::inputNames.size() - 1)]
                    {
                    inputOption=(Token)match(input,ID,FOLLOW_ID_in_input1866); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(inputOption);

                    if ( !((((genericOperator_scope)genericOperator_stack.peek()).operatorInfo.hasInputProperty((inputOption!=null?inputOption.getText():null)))) ) {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        throw new FailedPredicateException(input, "input", "$genericOperator::operatorInfo.hasInputProperty($inputOption.text)");
                    }
                    pushFollow(FOLLOW_contextAwareExpression_in_input1875);
                    expr=contextAwareExpression(new InputSelection(((operator_scope)operator_stack.peek()).inputNames.size() - 1));

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_contextAwareExpression.add(expr.getTree());
                    if ( state.backtracking==0 ) {
                       ((genericOperator_scope)genericOperator_stack.peek()).operatorInfo.setInputProperty((inputOption!=null?inputOption.getText():null), ((operator_scope)operator_stack.peek()).result, ((operator_scope)operator_stack.peek()).inputNames.size()-1, (expr!=null?((EvaluationExpression)expr.tree):null)); 
                    }

                    }
                    break;

            }



            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 330:1: ->
            {
                root_0 = null;
            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 37, input_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "input"

    // $ANTLR start synpred9_SJaql
    public final void synpred9_SJaql_fragment() throws RecognitionException {   
        SJaqlParser.orExpression_return ifClause = null;

        SJaqlParser.expression_return ifExpr = null;

        SJaqlParser.expression_return elseExpr = null;


        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:126:4: (ifClause= orExpression ( '?' (ifExpr= expression )? ':' elseExpr= expression ) )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:126:4: ifClause= orExpression ( '?' (ifExpr= expression )? ':' elseExpr= expression )
        {
        pushFollow(FOLLOW_orExpression_in_synpred9_SJaql351);
        ifClause=orExpression();

        state._fsp--;
        if (state.failed) return ;
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:126:26: ( '?' (ifExpr= expression )? ':' elseExpr= expression )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:126:27: '?' (ifExpr= expression )? ':' elseExpr= expression
        {
        match(input,34,FOLLOW_34_in_synpred9_SJaql354); if (state.failed) return ;
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:126:37: (ifExpr= expression )?
        int alt54=2;
        int LA54_0 = input.LA(1);

        if ( ((LA54_0>=ID && LA54_0<=STRING)||(LA54_0>=DECIMAL && LA54_0<=UINT)||LA54_0==30||(LA54_0>=52 && LA54_0<=55)||LA54_0==58||(LA54_0>=60 && LA54_0<=63)||(LA54_0>=65 && LA54_0<=66)) ) {
            alt54=1;
        }
        switch (alt54) {
            case 1 :
                // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: ifExpr= expression
                {
                pushFollow(FOLLOW_expression_in_synpred9_SJaql358);
                ifExpr=expression();

                state._fsp--;
                if (state.failed) return ;

                }
                break;

        }

        match(input,35,FOLLOW_35_in_synpred9_SJaql361); if (state.failed) return ;
        pushFollow(FOLLOW_expression_in_synpred9_SJaql365);
        elseExpr=expression();

        state._fsp--;
        if (state.failed) return ;

        }


        }
    }
    // $ANTLR end synpred9_SJaql

    // $ANTLR start synpred10_SJaql
    public final void synpred10_SJaql_fragment() throws RecognitionException {   
        SJaqlParser.orExpression_return ifExpr2 = null;

        SJaqlParser.expression_return ifClause2 = null;


        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:128:4: (ifExpr2= orExpression 'if' ifClause2= expression )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:128:4: ifExpr2= orExpression 'if' ifClause2= expression
        {
        pushFollow(FOLLOW_orExpression_in_synpred10_SJaql388);
        ifExpr2=orExpression();

        state._fsp--;
        if (state.failed) return ;
        match(input,36,FOLLOW_36_in_synpred10_SJaql390); if (state.failed) return ;
        pushFollow(FOLLOW_expression_in_synpred10_SJaql394);
        ifClause2=expression();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred10_SJaql

    // $ANTLR start synpred12_SJaql
    public final void synpred12_SJaql_fragment() throws RecognitionException {   
        List list_exprs=null;
        RuleReturnScope exprs = null;
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:133:27: ( ( 'or' | '||' ) exprs+= andExpression )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:133:27: ( 'or' | '||' ) exprs+= andExpression
        {
        if ( (input.LA(1)>=37 && input.LA(1)<=38) ) {
            input.consume();
            state.errorRecovery=false;state.failed=false;
        }
        else {
            if (state.backtracking>0) {state.failed=true; return ;}
            MismatchedSetException mse = new MismatchedSetException(null,input);
            throw mse;
        }

        pushFollow(FOLLOW_andExpression_in_synpred12_SJaql440);
        exprs=andExpression();

        state._fsp--;
        if (state.failed) return ;
        if (list_exprs==null) list_exprs=new ArrayList();
        list_exprs.add(exprs);


        }
    }
    // $ANTLR end synpred12_SJaql

    // $ANTLR start synpred14_SJaql
    public final void synpred14_SJaql_fragment() throws RecognitionException {   
        List list_exprs=null;
        RuleReturnScope exprs = null;
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:138:31: ( ( 'and' | '&&' ) exprs+= elementExpression )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:138:31: ( 'and' | '&&' ) exprs+= elementExpression
        {
        if ( (input.LA(1)>=39 && input.LA(1)<=40) ) {
            input.consume();
            state.errorRecovery=false;state.failed=false;
        }
        else {
            if (state.backtracking>0) {state.failed=true; return ;}
            MismatchedSetException mse = new MismatchedSetException(null,input);
            throw mse;
        }

        pushFollow(FOLLOW_elementExpression_in_synpred14_SJaql487);
        exprs=elementExpression();

        state._fsp--;
        if (state.failed) return ;
        if (list_exprs==null) list_exprs=new ArrayList();
        list_exprs.add(exprs);


        }
    }
    // $ANTLR end synpred14_SJaql

    // $ANTLR start synpred16_SJaql
    public final void synpred16_SJaql_fragment() throws RecognitionException {   
        Token not=null;
        SJaqlParser.comparisonExpression_return set = null;


        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:143:31: ( (not= 'not' )? 'in' set= comparisonExpression )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:143:31: (not= 'not' )? 'in' set= comparisonExpression
        {
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:143:34: (not= 'not' )?
        int alt55=2;
        int LA55_0 = input.LA(1);

        if ( (LA55_0==41) ) {
            alt55=1;
        }
        switch (alt55) {
            case 1 :
                // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: not= 'not'
                {
                not=(Token)match(input,41,FOLLOW_41_in_synpred16_SJaql526); if (state.failed) return ;

                }
                break;

        }

        match(input,42,FOLLOW_42_in_synpred16_SJaql529); if (state.failed) return ;
        pushFollow(FOLLOW_comparisonExpression_in_synpred16_SJaql533);
        set=comparisonExpression();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred16_SJaql

    // $ANTLR start synpred22_SJaql
    public final void synpred22_SJaql_fragment() throws RecognitionException {   
        Token s=null;
        SJaqlParser.arithmeticExpression_return e2 = null;


        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:149:29: ( (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:149:29: (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression
        {
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:149:29: (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' )
        int alt56=6;
        switch ( input.LA(1) ) {
        case 43:
            {
            alt56=1;
            }
            break;
        case 44:
            {
            alt56=2;
            }
            break;
        case 45:
            {
            alt56=3;
            }
            break;
        case 46:
            {
            alt56=4;
            }
            break;
        case 47:
            {
            alt56=5;
            }
            break;
        case 48:
            {
            alt56=6;
            }
            break;
        default:
            if (state.backtracking>0) {state.failed=true; return ;}
            NoViableAltException nvae =
                new NoViableAltException("", 56, 0, input);

            throw nvae;
        }

        switch (alt56) {
            case 1 :
                // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:149:30: s= '<='
                {
                s=(Token)match(input,43,FOLLOW_43_in_synpred22_SJaql580); if (state.failed) return ;

                }
                break;
            case 2 :
                // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:149:39: s= '>='
                {
                s=(Token)match(input,44,FOLLOW_44_in_synpred22_SJaql586); if (state.failed) return ;

                }
                break;
            case 3 :
                // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:149:48: s= '<'
                {
                s=(Token)match(input,45,FOLLOW_45_in_synpred22_SJaql592); if (state.failed) return ;

                }
                break;
            case 4 :
                // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:149:56: s= '>'
                {
                s=(Token)match(input,46,FOLLOW_46_in_synpred22_SJaql598); if (state.failed) return ;

                }
                break;
            case 5 :
                // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:149:64: s= '=='
                {
                s=(Token)match(input,47,FOLLOW_47_in_synpred22_SJaql604); if (state.failed) return ;

                }
                break;
            case 6 :
                // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:149:73: s= '!='
                {
                s=(Token)match(input,48,FOLLOW_48_in_synpred22_SJaql610); if (state.failed) return ;

                }
                break;

        }

        pushFollow(FOLLOW_arithmeticExpression_in_synpred22_SJaql615);
        e2=arithmeticExpression();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred22_SJaql

    // $ANTLR start synpred24_SJaql
    public final void synpred24_SJaql_fragment() throws RecognitionException {   
        Token s=null;
        SJaqlParser.multiplicationExpression_return e2 = null;


        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:156:33: ( (s= '+' | s= '-' ) e2= multiplicationExpression )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:156:33: (s= '+' | s= '-' ) e2= multiplicationExpression
        {
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:156:33: (s= '+' | s= '-' )
        int alt57=2;
        int LA57_0 = input.LA(1);

        if ( (LA57_0==49) ) {
            alt57=1;
        }
        else if ( (LA57_0==50) ) {
            alt57=2;
        }
        else {
            if (state.backtracking>0) {state.failed=true; return ;}
            NoViableAltException nvae =
                new NoViableAltException("", 57, 0, input);

            throw nvae;
        }
        switch (alt57) {
            case 1 :
                // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:156:34: s= '+'
                {
                s=(Token)match(input,49,FOLLOW_49_in_synpred24_SJaql701); if (state.failed) return ;

                }
                break;
            case 2 :
                // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:156:42: s= '-'
                {
                s=(Token)match(input,50,FOLLOW_50_in_synpred24_SJaql707); if (state.failed) return ;

                }
                break;

        }

        pushFollow(FOLLOW_multiplicationExpression_in_synpred24_SJaql712);
        e2=multiplicationExpression();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred24_SJaql

    // $ANTLR start synpred26_SJaql
    public final void synpred26_SJaql_fragment() throws RecognitionException {   
        Token s=null;
        SJaqlParser.preincrementExpression_return e2 = null;


        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:162:31: ( (s= '*' | s= '/' ) e2= preincrementExpression )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:162:31: (s= '*' | s= '/' ) e2= preincrementExpression
        {
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:162:31: (s= '*' | s= '/' )
        int alt58=2;
        int LA58_0 = input.LA(1);

        if ( (LA58_0==STAR) ) {
            alt58=1;
        }
        else if ( (LA58_0==51) ) {
            alt58=2;
        }
        else {
            if (state.backtracking>0) {state.failed=true; return ;}
            NoViableAltException nvae =
                new NoViableAltException("", 58, 0, input);

            throw nvae;
        }
        switch (alt58) {
            case 1 :
                // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:162:32: s= '*'
                {
                s=(Token)match(input,STAR,FOLLOW_STAR_in_synpred26_SJaql761); if (state.failed) return ;

                }
                break;
            case 2 :
                // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:162:40: s= '/'
                {
                s=(Token)match(input,51,FOLLOW_51_in_synpred26_SJaql767); if (state.failed) return ;

                }
                break;

        }

        pushFollow(FOLLOW_preincrementExpression_in_synpred26_SJaql772);
        e2=preincrementExpression();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred26_SJaql

    // $ANTLR start synpred31_SJaql
    public final void synpred31_SJaql_fragment() throws RecognitionException {   
        Token type=null;
        SJaqlParser.generalPathExpression_return expr = null;


        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:176:5: ( '(' type= ID ')' expr= generalPathExpression )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:176:5: '(' type= ID ')' expr= generalPathExpression
        {
        match(input,30,FOLLOW_30_in_synpred31_SJaql856); if (state.failed) return ;
        type=(Token)match(input,ID,FOLLOW_ID_in_synpred31_SJaql860); if (state.failed) return ;
        match(input,32,FOLLOW_32_in_synpred31_SJaql862); if (state.failed) return ;
        pushFollow(FOLLOW_generalPathExpression_in_synpred31_SJaql866);
        expr=generalPathExpression();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred31_SJaql

    // $ANTLR start synpred32_SJaql
    public final void synpred32_SJaql_fragment() throws RecognitionException {   
        Token type=null;
        SJaqlParser.generalPathExpression_return expr = null;


        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:177:4: (expr= generalPathExpression 'as' type= ID )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:177:4: expr= generalPathExpression 'as' type= ID
        {
        pushFollow(FOLLOW_generalPathExpression_in_synpred32_SJaql873);
        expr=generalPathExpression();

        state._fsp--;
        if (state.failed) return ;
        match(input,56,FOLLOW_56_in_synpred32_SJaql875); if (state.failed) return ;
        type=(Token)match(input,ID,FOLLOW_ID_in_synpred32_SJaql879); if (state.failed) return ;

        }
    }
    // $ANTLR end synpred32_SJaql

    // $ANTLR start synpred33_SJaql
    public final void synpred33_SJaql_fragment() throws RecognitionException {   
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:183:5: ( ({...}? contextAwarePathExpression ) )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:183:5: ({...}? contextAwarePathExpression )
        {
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:183:5: ({...}? contextAwarePathExpression )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:183:6: {...}? contextAwarePathExpression
        {
        if ( !((((contextAwareExpression_scope)contextAwareExpression_stack.peek()).context != null)) ) {
            if (state.backtracking>0) {state.failed=true; return ;}
            throw new FailedPredicateException(input, "synpred33_SJaql", "$contextAwareExpression::context != null");
        }
        pushFollow(FOLLOW_contextAwarePathExpression_in_synpred33_SJaql915);
        contextAwarePathExpression();

        state._fsp--;
        if (state.failed) return ;

        }


        }
    }
    // $ANTLR end synpred33_SJaql

    // $ANTLR start synpred34_SJaql
    public final void synpred34_SJaql_fragment() throws RecognitionException {   
        Token field=null;

        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:189:7: ( ( '.' (field= ID ) ) )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:189:7: ( '.' (field= ID ) )
        {
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:189:7: ( '.' (field= ID ) )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:189:8: '.' (field= ID )
        {
        match(input,57,FOLLOW_57_in_synpred34_SJaql954); if (state.failed) return ;
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:189:12: (field= ID )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:189:13: field= ID
        {
        field=(Token)match(input,ID,FOLLOW_ID_in_synpred34_SJaql959); if (state.failed) return ;

        }


        }


        }
    }
    // $ANTLR end synpred34_SJaql

    // $ANTLR start synpred35_SJaql
    public final void synpred35_SJaql_fragment() throws RecognitionException {   
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:190:11: ( arrayAccess )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:190:11: arrayAccess
        {
        pushFollow(FOLLOW_arrayAccess_in_synpred35_SJaql977);
        arrayAccess();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred35_SJaql

    // $ANTLR start synpred36_SJaql
    public final void synpred36_SJaql_fragment() throws RecognitionException {   
        Token field=null;

        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:198:7: ( ( '.' (field= ID ) ) )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:198:7: ( '.' (field= ID ) )
        {
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:198:7: ( '.' (field= ID ) )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:198:8: '.' (field= ID )
        {
        match(input,57,FOLLOW_57_in_synpred36_SJaql1033); if (state.failed) return ;
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:198:12: (field= ID )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:198:13: field= ID
        {
        field=(Token)match(input,ID,FOLLOW_ID_in_synpred36_SJaql1038); if (state.failed) return ;

        }


        }


        }
    }
    // $ANTLR end synpred36_SJaql

    // $ANTLR start synpred37_SJaql
    public final void synpred37_SJaql_fragment() throws RecognitionException {   
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:199:11: ( arrayAccess )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:199:11: arrayAccess
        {
        pushFollow(FOLLOW_arrayAccess_in_synpred37_SJaql1056);
        arrayAccess();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred37_SJaql

    // $ANTLR start synpred38_SJaql
    public final void synpred38_SJaql_fragment() throws RecognitionException {   
        Token field=null;

        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:196:5: ( valueExpression ( ( '.' (field= ID ) ) | arrayAccess )+ )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:196:5: valueExpression ( ( '.' (field= ID ) ) | arrayAccess )+
        {
        pushFollow(FOLLOW_valueExpression_in_synpred38_SJaql1019);
        valueExpression();

        state._fsp--;
        if (state.failed) return ;
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:198:5: ( ( '.' (field= ID ) ) | arrayAccess )+
        int cnt59=0;
        loop59:
        do {
            int alt59=3;
            int LA59_0 = input.LA(1);

            if ( (LA59_0==57) ) {
                alt59=1;
            }
            else if ( (LA59_0==63) ) {
                alt59=2;
            }


            switch (alt59) {
        	case 1 :
        	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:198:7: ( '.' (field= ID ) )
        	    {
        	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:198:7: ( '.' (field= ID ) )
        	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:198:8: '.' (field= ID )
        	    {
        	    match(input,57,FOLLOW_57_in_synpred38_SJaql1033); if (state.failed) return ;
        	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:198:12: (field= ID )
        	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:198:13: field= ID
        	    {
        	    field=(Token)match(input,ID,FOLLOW_ID_in_synpred38_SJaql1038); if (state.failed) return ;

        	    }


        	    }


        	    }
        	    break;
        	case 2 :
        	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:199:11: arrayAccess
        	    {
        	    pushFollow(FOLLOW_arrayAccess_in_synpred38_SJaql1056);
        	    arrayAccess();

        	    state._fsp--;
        	    if (state.failed) return ;

        	    }
        	    break;

        	default :
        	    if ( cnt59 >= 1 ) break loop59;
        	    if (state.backtracking>0) {state.failed=true; return ;}
                    EarlyExitException eee =
                        new EarlyExitException(59, input);
                    throw eee;
            }
            cnt59++;
        } while (true);


        }
    }
    // $ANTLR end synpred38_SJaql

    // $ANTLR start synpred43_SJaql
    public final void synpred43_SJaql_fragment() throws RecognitionException {   
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:208:4: ( ID )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:208:4: ID
        {
        match(input,ID,FOLLOW_ID_in_synpred43_SJaql1119); if (state.failed) return ;

        }
    }
    // $ANTLR end synpred43_SJaql

    // $ANTLR start synpred74_SJaql
    public final void synpred74_SJaql_fragment() throws RecognitionException {   
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:298:8: ( ',' input )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:298:8: ',' input
        {
        match(input,31,FOLLOW_31_in_synpred74_SJaql1743); if (state.failed) return ;
        pushFollow(FOLLOW_input_in_synpred74_SJaql1745);
        input();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred74_SJaql

    // $ANTLR start synpred75_SJaql
    public final void synpred75_SJaql_fragment() throws RecognitionException {   
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:299:1: ( operatorOption )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:299:1: operatorOption
        {
        pushFollow(FOLLOW_operatorOption_in_synpred75_SJaql1750);
        operatorOption();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred75_SJaql

    // $ANTLR start synpred76_SJaql
    public final void synpred76_SJaql_fragment() throws RecognitionException {   
        Token moreName=null;

        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:306:2: ({...}?moreName= ID )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:306:2: {...}?moreName= ID
        {
        if ( !((!((genericOperator_scope)genericOperator_stack.peek()).operatorInfo.hasProperty(((operatorOption_scope)operatorOption_stack.peek()).optionName))) ) {
            if (state.backtracking>0) {state.failed=true; return ;}
            throw new FailedPredicateException(input, "synpred76_SJaql", "!$genericOperator::operatorInfo.hasProperty($operatorOption::optionName)");
        }
        moreName=(Token)match(input,ID,FOLLOW_ID_in_synpred76_SJaql1779); if (state.failed) return ;

        }
    }
    // $ANTLR end synpred76_SJaql

    // $ANTLR start synpred77_SJaql
    public final void synpred77_SJaql_fragment() throws RecognitionException {   
        Token moreName=null;

        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:315:2: ({...}?moreName= ID )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:315:2: {...}?moreName= ID
        {
        if ( !((!((genericOperator_scope)genericOperator_stack.peek()).operatorInfo.hasFlag(((operatorFlag_scope)operatorFlag_stack.peek()).flagName))) ) {
            if (state.backtracking>0) {state.failed=true; return ;}
            throw new FailedPredicateException(input, "synpred77_SJaql", "!$genericOperator::operatorInfo.hasFlag($operatorFlag::flagName)");
        }
        moreName=(Token)match(input,ID,FOLLOW_ID_in_synpred77_SJaql1820); if (state.failed) return ;

        }
    }
    // $ANTLR end synpred77_SJaql

    // $ANTLR start synpred79_SJaql
    public final void synpred79_SJaql_fragment() throws RecognitionException {   
        Token name=null;

        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:320:33: (name= VAR 'in' )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:320:33: name= VAR 'in'
        {
        name=(Token)match(input,VAR,FOLLOW_VAR_in_synpred79_SJaql1850); if (state.failed) return ;
        match(input,42,FOLLOW_42_in_synpred79_SJaql1852); if (state.failed) return ;

        }
    }
    // $ANTLR end synpred79_SJaql

    // $ANTLR start synpred80_SJaql
    public final void synpred80_SJaql_fragment() throws RecognitionException {   
        Token inputOption=null;
        SJaqlParser.contextAwareExpression_return expr = null;


        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:328:2: (inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::inputNames.size() - 1)] )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:328:2: inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::inputNames.size() - 1)]
        {
        inputOption=(Token)match(input,ID,FOLLOW_ID_in_synpred80_SJaql1866); if (state.failed) return ;
        if ( !((((genericOperator_scope)genericOperator_stack.peek()).operatorInfo.hasInputProperty((inputOption!=null?inputOption.getText():null)))) ) {
            if (state.backtracking>0) {state.failed=true; return ;}
            throw new FailedPredicateException(input, "synpred80_SJaql", "$genericOperator::operatorInfo.hasInputProperty($inputOption.text)");
        }
        pushFollow(FOLLOW_contextAwareExpression_in_synpred80_SJaql1875);
        expr=contextAwareExpression(new InputSelection(((operator_scope)operator_stack.peek()).inputNames.size() - 1));

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred80_SJaql

    // Delegated rules

    public final boolean synpred22_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred22_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred26_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred26_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred74_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred74_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred37_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred37_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred14_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred14_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred12_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred12_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred77_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred77_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred43_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred43_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred24_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred24_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred79_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred79_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred36_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred36_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred9_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred9_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred31_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred31_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred76_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred76_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred16_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred16_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred35_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred35_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred32_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred32_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred34_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred34_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred10_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred10_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred80_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred80_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred33_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred33_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred38_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred38_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred75_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred75_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }


    protected DFA6 dfa6 = new DFA6(this);
    protected DFA21 dfa21 = new DFA21(this);
    protected DFA23 dfa23 = new DFA23(this);
    protected DFA25 dfa25 = new DFA25(this);
    protected DFA26 dfa26 = new DFA26(this);
    protected DFA48 dfa48 = new DFA48(this);
    protected DFA52 dfa52 = new DFA52(this);
    static final String DFA6_eotS =
        "\25\uffff";
    static final String DFA6_eofS =
        "\25\uffff";
    static final String DFA6_minS =
        "\1\6\21\0\3\uffff";
    static final String DFA6_maxS =
        "\1\102\21\0\3\uffff";
    static final String DFA6_acceptS =
        "\22\uffff\1\1\1\2\1\3";
    static final String DFA6_specialS =
        "\1\uffff\1\0\1\1\1\2\1\3\1\4\1\5\1\6\1\7\1\10\1\11\1\12\1\13\1\14"+
        "\1\15\1\16\1\17\1\20\3\uffff}>";
    static final String[] DFA6_transitionS = {
            "\1\5\1\15\1\11\1\uffff\1\10\1\12\1\13\21\uffff\1\4\25\uffff"+
            "\1\1\1\2\2\3\2\uffff\1\17\1\uffff\1\6\1\7\1\14\1\16\1\uffff"+
            "\1\20\1\21",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "",
            "",
            ""
    };

    static final short[] DFA6_eot = DFA.unpackEncodedString(DFA6_eotS);
    static final short[] DFA6_eof = DFA.unpackEncodedString(DFA6_eofS);
    static final char[] DFA6_min = DFA.unpackEncodedStringToUnsignedChars(DFA6_minS);
    static final char[] DFA6_max = DFA.unpackEncodedStringToUnsignedChars(DFA6_maxS);
    static final short[] DFA6_accept = DFA.unpackEncodedString(DFA6_acceptS);
    static final short[] DFA6_special = DFA.unpackEncodedString(DFA6_specialS);
    static final short[][] DFA6_transition;

    static {
        int numStates = DFA6_transitionS.length;
        DFA6_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA6_transition[i] = DFA.unpackEncodedString(DFA6_transitionS[i]);
        }
    }

    class DFA6 extends DFA {

        public DFA6(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 6;
            this.eot = DFA6_eot;
            this.eof = DFA6_eof;
            this.min = DFA6_min;
            this.max = DFA6_max;
            this.accept = DFA6_accept;
            this.special = DFA6_special;
            this.transition = DFA6_transition;
        }
        public String getDescription() {
            return "125:1: ternaryExpression : (ifClause= orExpression ( '?' (ifExpr= expression )? ':' elseExpr= expression ) -> ^( EXPRESSION[\"TernaryExpression\"] $ifClause) | ifExpr2= orExpression 'if' ifClause2= expression -> ^( EXPRESSION[\"TernaryExpression\"] $ifClause2 $ifExpr2) | orExpression );";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA6_1 = input.LA(1);

                         
                        int index6_1 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred9_SJaql()) ) {s = 18;}

                        else if ( (synpred10_SJaql()) ) {s = 19;}

                        else if ( (true) ) {s = 20;}

                         
                        input.seek(index6_1);
                        if ( s>=0 ) return s;
                        break;
                    case 1 : 
                        int LA6_2 = input.LA(1);

                         
                        int index6_2 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred9_SJaql()) ) {s = 18;}

                        else if ( (synpred10_SJaql()) ) {s = 19;}

                        else if ( (true) ) {s = 20;}

                         
                        input.seek(index6_2);
                        if ( s>=0 ) return s;
                        break;
                    case 2 : 
                        int LA6_3 = input.LA(1);

                         
                        int index6_3 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred9_SJaql()) ) {s = 18;}

                        else if ( (synpred10_SJaql()) ) {s = 19;}

                        else if ( (true) ) {s = 20;}

                         
                        input.seek(index6_3);
                        if ( s>=0 ) return s;
                        break;
                    case 3 : 
                        int LA6_4 = input.LA(1);

                         
                        int index6_4 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred9_SJaql()) ) {s = 18;}

                        else if ( (synpred10_SJaql()) ) {s = 19;}

                        else if ( (true) ) {s = 20;}

                         
                        input.seek(index6_4);
                        if ( s>=0 ) return s;
                        break;
                    case 4 : 
                        int LA6_5 = input.LA(1);

                         
                        int index6_5 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (((synpred9_SJaql()&&(((contextAwareExpression_scope)contextAwareExpression_stack.peek()).context != null))||(synpred9_SJaql()&&(((contextAwareExpression_scope)contextAwareExpression_stack.peek()).context != null))||synpred9_SJaql())) ) {s = 18;}

                        else if ( (((synpred10_SJaql()&&(((contextAwareExpression_scope)contextAwareExpression_stack.peek()).context != null))||synpred10_SJaql()||(synpred10_SJaql()&&(((contextAwareExpression_scope)contextAwareExpression_stack.peek()).context != null)))) ) {s = 19;}

                        else if ( (true) ) {s = 20;}

                         
                        input.seek(index6_5);
                        if ( s>=0 ) return s;
                        break;
                    case 5 : 
                        int LA6_6 = input.LA(1);

                         
                        int index6_6 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred9_SJaql()) ) {s = 18;}

                        else if ( (synpred10_SJaql()) ) {s = 19;}

                        else if ( (true) ) {s = 20;}

                         
                        input.seek(index6_6);
                        if ( s>=0 ) return s;
                        break;
                    case 6 : 
                        int LA6_7 = input.LA(1);

                         
                        int index6_7 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred9_SJaql()) ) {s = 18;}

                        else if ( (synpred10_SJaql()) ) {s = 19;}

                        else if ( (true) ) {s = 20;}

                         
                        input.seek(index6_7);
                        if ( s>=0 ) return s;
                        break;
                    case 7 : 
                        int LA6_8 = input.LA(1);

                         
                        int index6_8 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred9_SJaql()) ) {s = 18;}

                        else if ( (synpred10_SJaql()) ) {s = 19;}

                        else if ( (true) ) {s = 20;}

                         
                        input.seek(index6_8);
                        if ( s>=0 ) return s;
                        break;
                    case 8 : 
                        int LA6_9 = input.LA(1);

                         
                        int index6_9 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred9_SJaql()) ) {s = 18;}

                        else if ( (synpred10_SJaql()) ) {s = 19;}

                        else if ( (true) ) {s = 20;}

                         
                        input.seek(index6_9);
                        if ( s>=0 ) return s;
                        break;
                    case 9 : 
                        int LA6_10 = input.LA(1);

                         
                        int index6_10 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred9_SJaql()) ) {s = 18;}

                        else if ( (synpred10_SJaql()) ) {s = 19;}

                        else if ( (true) ) {s = 20;}

                         
                        input.seek(index6_10);
                        if ( s>=0 ) return s;
                        break;
                    case 10 : 
                        int LA6_11 = input.LA(1);

                         
                        int index6_11 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred9_SJaql()) ) {s = 18;}

                        else if ( (synpred10_SJaql()) ) {s = 19;}

                        else if ( (true) ) {s = 20;}

                         
                        input.seek(index6_11);
                        if ( s>=0 ) return s;
                        break;
                    case 11 : 
                        int LA6_12 = input.LA(1);

                         
                        int index6_12 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred9_SJaql()) ) {s = 18;}

                        else if ( (synpred10_SJaql()) ) {s = 19;}

                        else if ( (true) ) {s = 20;}

                         
                        input.seek(index6_12);
                        if ( s>=0 ) return s;
                        break;
                    case 12 : 
                        int LA6_13 = input.LA(1);

                         
                        int index6_13 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred9_SJaql()) ) {s = 18;}

                        else if ( (synpred10_SJaql()) ) {s = 19;}

                        else if ( (true) ) {s = 20;}

                         
                        input.seek(index6_13);
                        if ( s>=0 ) return s;
                        break;
                    case 13 : 
                        int LA6_14 = input.LA(1);

                         
                        int index6_14 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred9_SJaql()) ) {s = 18;}

                        else if ( (synpred10_SJaql()) ) {s = 19;}

                        else if ( (true) ) {s = 20;}

                         
                        input.seek(index6_14);
                        if ( s>=0 ) return s;
                        break;
                    case 14 : 
                        int LA6_15 = input.LA(1);

                         
                        int index6_15 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred9_SJaql()) ) {s = 18;}

                        else if ( (synpred10_SJaql()) ) {s = 19;}

                        else if ( (true) ) {s = 20;}

                         
                        input.seek(index6_15);
                        if ( s>=0 ) return s;
                        break;
                    case 15 : 
                        int LA6_16 = input.LA(1);

                         
                        int index6_16 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred9_SJaql()) ) {s = 18;}

                        else if ( (synpred10_SJaql()) ) {s = 19;}

                        else if ( (true) ) {s = 20;}

                         
                        input.seek(index6_16);
                        if ( s>=0 ) return s;
                        break;
                    case 16 : 
                        int LA6_17 = input.LA(1);

                         
                        int index6_17 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred9_SJaql()) ) {s = 18;}

                        else if ( (synpred10_SJaql()) ) {s = 19;}

                        else if ( (true) ) {s = 20;}

                         
                        input.seek(index6_17);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 6, _s, input);
            error(nvae);
            throw nvae;
        }
    }
    static final String DFA21_eotS =
        "\22\uffff";
    static final String DFA21_eofS =
        "\22\uffff";
    static final String DFA21_minS =
        "\1\6\16\0\3\uffff";
    static final String DFA21_maxS =
        "\1\102\16\0\3\uffff";
    static final String DFA21_acceptS =
        "\17\uffff\1\1\1\2\1\3";
    static final String DFA21_specialS =
        "\1\uffff\1\0\1\1\1\2\1\3\1\4\1\5\1\6\1\7\1\10\1\11\1\12\1\13\1\14"+
        "\1\15\3\uffff}>";
    static final String[] DFA21_transitionS = {
            "\1\2\1\12\1\6\1\uffff\1\5\1\7\1\10\21\uffff\1\1\33\uffff\1\14"+
            "\1\uffff\1\3\1\4\1\11\1\13\1\uffff\1\15\1\16",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "",
            "",
            ""
    };

    static final short[] DFA21_eot = DFA.unpackEncodedString(DFA21_eotS);
    static final short[] DFA21_eof = DFA.unpackEncodedString(DFA21_eofS);
    static final char[] DFA21_min = DFA.unpackEncodedStringToUnsignedChars(DFA21_minS);
    static final char[] DFA21_max = DFA.unpackEncodedStringToUnsignedChars(DFA21_maxS);
    static final short[] DFA21_accept = DFA.unpackEncodedString(DFA21_acceptS);
    static final short[] DFA21_special = DFA.unpackEncodedString(DFA21_specialS);
    static final short[][] DFA21_transition;

    static {
        int numStates = DFA21_transitionS.length;
        DFA21_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA21_transition[i] = DFA.unpackEncodedString(DFA21_transitionS[i]);
        }
    }

    class DFA21 extends DFA {

        public DFA21(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 21;
            this.eot = DFA21_eot;
            this.eof = DFA21_eof;
            this.min = DFA21_min;
            this.max = DFA21_max;
            this.accept = DFA21_accept;
            this.special = DFA21_special;
            this.transition = DFA21_transition;
        }
        public String getDescription() {
            return "176:4: ( '(' type= ID ')' expr= generalPathExpression | expr= generalPathExpression 'as' type= ID | expr= generalPathExpression )";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA21_1 = input.LA(1);

                         
                        int index21_1 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred31_SJaql()) ) {s = 15;}

                        else if ( (synpred32_SJaql()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index21_1);
                        if ( s>=0 ) return s;
                        break;
                    case 1 : 
                        int LA21_2 = input.LA(1);

                         
                        int index21_2 = input.index();
                        input.rewind();
                        s = -1;
                        if ( ((synpred32_SJaql()||(synpred32_SJaql()&&(((contextAwareExpression_scope)contextAwareExpression_stack.peek()).context != null)))) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index21_2);
                        if ( s>=0 ) return s;
                        break;
                    case 2 : 
                        int LA21_3 = input.LA(1);

                         
                        int index21_3 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred32_SJaql()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index21_3);
                        if ( s>=0 ) return s;
                        break;
                    case 3 : 
                        int LA21_4 = input.LA(1);

                         
                        int index21_4 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred32_SJaql()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index21_4);
                        if ( s>=0 ) return s;
                        break;
                    case 4 : 
                        int LA21_5 = input.LA(1);

                         
                        int index21_5 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred32_SJaql()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index21_5);
                        if ( s>=0 ) return s;
                        break;
                    case 5 : 
                        int LA21_6 = input.LA(1);

                         
                        int index21_6 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred32_SJaql()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index21_6);
                        if ( s>=0 ) return s;
                        break;
                    case 6 : 
                        int LA21_7 = input.LA(1);

                         
                        int index21_7 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred32_SJaql()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index21_7);
                        if ( s>=0 ) return s;
                        break;
                    case 7 : 
                        int LA21_8 = input.LA(1);

                         
                        int index21_8 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred32_SJaql()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index21_8);
                        if ( s>=0 ) return s;
                        break;
                    case 8 : 
                        int LA21_9 = input.LA(1);

                         
                        int index21_9 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred32_SJaql()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index21_9);
                        if ( s>=0 ) return s;
                        break;
                    case 9 : 
                        int LA21_10 = input.LA(1);

                         
                        int index21_10 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred32_SJaql()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index21_10);
                        if ( s>=0 ) return s;
                        break;
                    case 10 : 
                        int LA21_11 = input.LA(1);

                         
                        int index21_11 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred32_SJaql()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index21_11);
                        if ( s>=0 ) return s;
                        break;
                    case 11 : 
                        int LA21_12 = input.LA(1);

                         
                        int index21_12 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred32_SJaql()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index21_12);
                        if ( s>=0 ) return s;
                        break;
                    case 12 : 
                        int LA21_13 = input.LA(1);

                         
                        int index21_13 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred32_SJaql()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index21_13);
                        if ( s>=0 ) return s;
                        break;
                    case 13 : 
                        int LA21_14 = input.LA(1);

                         
                        int index21_14 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred32_SJaql()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index21_14);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 21, _s, input);
            error(nvae);
            throw nvae;
        }
    }
    static final String DFA23_eotS =
        "\20\uffff";
    static final String DFA23_eofS =
        "\1\1\17\uffff";
    static final String DFA23_minS =
        "\1\6\1\uffff\1\6\1\11\1\0\1\100\2\43\1\uffff\2\0\1\13\1\uffff\2"+
        "\100\1\0";
    static final String DFA23_maxS =
        "\1\100\1\uffff\1\6\1\14\1\0\3\100\1\uffff\2\0\1\14\1\uffff\2\100"+
        "\1\0";
    static final String DFA23_acceptS =
        "\1\uffff\1\3\6\uffff\1\1\3\uffff\1\2\3\uffff";
    static final String DFA23_specialS =
        "\4\uffff\1\0\4\uffff\1\3\1\2\4\uffff\1\1}>";
    static final String[] DFA23_transitionS = {
            "\1\1\2\uffff\1\1\20\uffff\1\1\4\uffff\2\1\1\uffff\22\1\4\uffff"+
            "\1\1\1\2\1\uffff\1\1\3\uffff\1\3\1\1",
            "",
            "\1\4",
            "\1\5\1\uffff\1\6\1\7",
            "\1\uffff",
            "\1\11",
            "\1\13\34\uffff\1\12",
            "\1\13\34\uffff\1\12",
            "",
            "\1\uffff",
            "\1\uffff",
            "\1\15\1\16",
            "",
            "\1\17",
            "\1\17",
            "\1\uffff"
    };

    static final short[] DFA23_eot = DFA.unpackEncodedString(DFA23_eotS);
    static final short[] DFA23_eof = DFA.unpackEncodedString(DFA23_eofS);
    static final char[] DFA23_min = DFA.unpackEncodedStringToUnsignedChars(DFA23_minS);
    static final char[] DFA23_max = DFA.unpackEncodedStringToUnsignedChars(DFA23_maxS);
    static final short[] DFA23_accept = DFA.unpackEncodedString(DFA23_acceptS);
    static final short[] DFA23_special = DFA.unpackEncodedString(DFA23_specialS);
    static final short[][] DFA23_transition;

    static {
        int numStates = DFA23_transitionS.length;
        DFA23_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA23_transition[i] = DFA.unpackEncodedString(DFA23_transitionS[i]);
        }
    }

    class DFA23 extends DFA {

        public DFA23(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 23;
            this.eot = DFA23_eot;
            this.eof = DFA23_eof;
            this.min = DFA23_min;
            this.max = DFA23_max;
            this.accept = DFA23_accept;
            this.special = DFA23_special;
            this.transition = DFA23_transition;
        }
        public String getDescription() {
            return "()* loopback of 189:5: ( ( '.' (field= ID ) ) | arrayAccess )*";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA23_4 = input.LA(1);

                         
                        int index23_4 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred34_SJaql()) ) {s = 8;}

                        else if ( (true) ) {s = 1;}

                         
                        input.seek(index23_4);
                        if ( s>=0 ) return s;
                        break;
                    case 1 : 
                        int LA23_15 = input.LA(1);

                         
                        int index23_15 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred35_SJaql()) ) {s = 12;}

                        else if ( (true) ) {s = 1;}

                         
                        input.seek(index23_15);
                        if ( s>=0 ) return s;
                        break;
                    case 2 : 
                        int LA23_10 = input.LA(1);

                         
                        int index23_10 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred35_SJaql()) ) {s = 12;}

                        else if ( (true) ) {s = 1;}

                         
                        input.seek(index23_10);
                        if ( s>=0 ) return s;
                        break;
                    case 3 : 
                        int LA23_9 = input.LA(1);

                         
                        int index23_9 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred35_SJaql()) ) {s = 12;}

                        else if ( (true) ) {s = 1;}

                         
                        input.seek(index23_9);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 23, _s, input);
            error(nvae);
            throw nvae;
        }
    }
    static final String DFA25_eotS =
        "\21\uffff";
    static final String DFA25_eofS =
        "\21\uffff";
    static final String DFA25_minS =
        "\1\6\16\0\2\uffff";
    static final String DFA25_maxS =
        "\1\102\16\0\2\uffff";
    static final String DFA25_acceptS =
        "\17\uffff\1\1\1\2";
    static final String DFA25_specialS =
        "\1\uffff\1\0\1\1\1\2\1\3\1\4\1\5\1\6\1\7\1\10\1\11\1\12\1\13\1\14"+
        "\1\15\2\uffff}>";
    static final String[] DFA25_transitionS = {
            "\1\1\1\12\1\6\1\uffff\1\5\1\7\1\10\21\uffff\1\2\33\uffff\1\14"+
            "\1\uffff\1\3\1\4\1\11\1\13\1\uffff\1\15\1\16",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "",
            ""
    };

    static final short[] DFA25_eot = DFA.unpackEncodedString(DFA25_eotS);
    static final short[] DFA25_eof = DFA.unpackEncodedString(DFA25_eofS);
    static final char[] DFA25_min = DFA.unpackEncodedStringToUnsignedChars(DFA25_minS);
    static final char[] DFA25_max = DFA.unpackEncodedStringToUnsignedChars(DFA25_maxS);
    static final short[] DFA25_accept = DFA.unpackEncodedString(DFA25_acceptS);
    static final short[] DFA25_special = DFA.unpackEncodedString(DFA25_specialS);
    static final short[][] DFA25_transition;

    static {
        int numStates = DFA25_transitionS.length;
        DFA25_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA25_transition[i] = DFA.unpackEncodedString(DFA25_transitionS[i]);
        }
    }

    class DFA25 extends DFA {

        public DFA25(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 25;
            this.eot = DFA25_eot;
            this.eof = DFA25_eof;
            this.min = DFA25_min;
            this.max = DFA25_max;
            this.accept = DFA25_accept;
            this.special = DFA25_special;
            this.transition = DFA25_transition;
        }
        public String getDescription() {
            return "192:1: pathExpression : ( valueExpression ( ( '.' (field= ID ) ) | arrayAccess )+ -> ^( EXPRESSION[\"PathExpression\"] ) | valueExpression );";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA25_1 = input.LA(1);

                         
                        int index25_1 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred38_SJaql()) ) {s = 15;}

                        else if ( (true) ) {s = 16;}

                         
                        input.seek(index25_1);
                        if ( s>=0 ) return s;
                        break;
                    case 1 : 
                        int LA25_2 = input.LA(1);

                         
                        int index25_2 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred38_SJaql()) ) {s = 15;}

                        else if ( (true) ) {s = 16;}

                         
                        input.seek(index25_2);
                        if ( s>=0 ) return s;
                        break;
                    case 2 : 
                        int LA25_3 = input.LA(1);

                         
                        int index25_3 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred38_SJaql()) ) {s = 15;}

                        else if ( (true) ) {s = 16;}

                         
                        input.seek(index25_3);
                        if ( s>=0 ) return s;
                        break;
                    case 3 : 
                        int LA25_4 = input.LA(1);

                         
                        int index25_4 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred38_SJaql()) ) {s = 15;}

                        else if ( (true) ) {s = 16;}

                         
                        input.seek(index25_4);
                        if ( s>=0 ) return s;
                        break;
                    case 4 : 
                        int LA25_5 = input.LA(1);

                         
                        int index25_5 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred38_SJaql()) ) {s = 15;}

                        else if ( (true) ) {s = 16;}

                         
                        input.seek(index25_5);
                        if ( s>=0 ) return s;
                        break;
                    case 5 : 
                        int LA25_6 = input.LA(1);

                         
                        int index25_6 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred38_SJaql()) ) {s = 15;}

                        else if ( (true) ) {s = 16;}

                         
                        input.seek(index25_6);
                        if ( s>=0 ) return s;
                        break;
                    case 6 : 
                        int LA25_7 = input.LA(1);

                         
                        int index25_7 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred38_SJaql()) ) {s = 15;}

                        else if ( (true) ) {s = 16;}

                         
                        input.seek(index25_7);
                        if ( s>=0 ) return s;
                        break;
                    case 7 : 
                        int LA25_8 = input.LA(1);

                         
                        int index25_8 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred38_SJaql()) ) {s = 15;}

                        else if ( (true) ) {s = 16;}

                         
                        input.seek(index25_8);
                        if ( s>=0 ) return s;
                        break;
                    case 8 : 
                        int LA25_9 = input.LA(1);

                         
                        int index25_9 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred38_SJaql()) ) {s = 15;}

                        else if ( (true) ) {s = 16;}

                         
                        input.seek(index25_9);
                        if ( s>=0 ) return s;
                        break;
                    case 9 : 
                        int LA25_10 = input.LA(1);

                         
                        int index25_10 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred38_SJaql()) ) {s = 15;}

                        else if ( (true) ) {s = 16;}

                         
                        input.seek(index25_10);
                        if ( s>=0 ) return s;
                        break;
                    case 10 : 
                        int LA25_11 = input.LA(1);

                         
                        int index25_11 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred38_SJaql()) ) {s = 15;}

                        else if ( (true) ) {s = 16;}

                         
                        input.seek(index25_11);
                        if ( s>=0 ) return s;
                        break;
                    case 11 : 
                        int LA25_12 = input.LA(1);

                         
                        int index25_12 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred38_SJaql()) ) {s = 15;}

                        else if ( (true) ) {s = 16;}

                         
                        input.seek(index25_12);
                        if ( s>=0 ) return s;
                        break;
                    case 12 : 
                        int LA25_13 = input.LA(1);

                         
                        int index25_13 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred38_SJaql()) ) {s = 15;}

                        else if ( (true) ) {s = 16;}

                         
                        input.seek(index25_13);
                        if ( s>=0 ) return s;
                        break;
                    case 13 : 
                        int LA25_14 = input.LA(1);

                         
                        int index25_14 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred38_SJaql()) ) {s = 15;}

                        else if ( (true) ) {s = 16;}

                         
                        input.seek(index25_14);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 25, _s, input);
            error(nvae);
            throw nvae;
        }
    }
    static final String DFA26_eotS =
        "\15\uffff";
    static final String DFA26_eofS =
        "\1\uffff\1\11\13\uffff";
    static final String DFA26_minS =
        "\2\6\10\uffff\1\6\2\0";
    static final String DFA26_maxS =
        "\1\102\1\104\10\uffff\1\104\2\0";
    static final String DFA26_acceptS =
        "\2\uffff\1\2\1\3\1\4\1\6\1\7\1\10\1\1\1\5\3\uffff";
    static final String DFA26_specialS =
        "\13\uffff\1\0\1\1}>";
    static final String[] DFA26_transitionS = {
            "\1\1\1\4\1\3\1\uffff\3\3\21\uffff\1\2\33\uffff\1\6\1\uffff\3"+
            "\3\1\5\1\uffff\2\7",
            "\1\12\1\7\1\uffff\1\11\20\uffff\1\11\3\uffff\1\10\2\11\1\uffff"+
            "\22\11\4\uffff\2\11\1\uffff\1\11\3\uffff\2\11\3\uffff\1\7",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "\1\13\1\14\1\11\1\uffff\3\11\21\uffff\1\11\25\uffff\4\11\2"+
            "\uffff\1\11\1\uffff\4\11\1\uffff\2\11\1\uffff\1\7",
            "\1\uffff",
            "\1\uffff"
    };

    static final short[] DFA26_eot = DFA.unpackEncodedString(DFA26_eotS);
    static final short[] DFA26_eof = DFA.unpackEncodedString(DFA26_eofS);
    static final char[] DFA26_min = DFA.unpackEncodedStringToUnsignedChars(DFA26_minS);
    static final char[] DFA26_max = DFA.unpackEncodedStringToUnsignedChars(DFA26_maxS);
    static final short[] DFA26_accept = DFA.unpackEncodedString(DFA26_acceptS);
    static final short[] DFA26_special = DFA.unpackEncodedString(DFA26_specialS);
    static final short[][] DFA26_transition;

    static {
        int numStates = DFA26_transitionS.length;
        DFA26_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA26_transition[i] = DFA.unpackEncodedString(DFA26_transitionS[i]);
        }
    }

    class DFA26 extends DFA {

        public DFA26(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 26;
            this.eot = DFA26_eot;
            this.eof = DFA26_eof;
            this.min = DFA26_min;
            this.max = DFA26_max;
            this.accept = DFA26_accept;
            this.special = DFA26_special;
            this.transition = DFA26_transition;
        }
        public String getDescription() {
            return "203:1: valueExpression : ( methodCall[MethodCall.NO_TARGET] | parenthesesExpression | literal | VAR -> | ID -> | arrayCreation | objectCreation | operatorExpression );";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA26_11 = input.LA(1);

                         
                        int index26_11 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred43_SJaql()) ) {s = 9;}

                        else if ( (true) ) {s = 7;}

                         
                        input.seek(index26_11);
                        if ( s>=0 ) return s;
                        break;
                    case 1 : 
                        int LA26_12 = input.LA(1);

                         
                        int index26_12 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred43_SJaql()) ) {s = 9;}

                        else if ( (true) ) {s = 7;}

                         
                        input.seek(index26_12);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 26, _s, input);
            error(nvae);
            throw nvae;
        }
    }
    static final String DFA48_eotS =
        "\23\uffff";
    static final String DFA48_eofS =
        "\23\uffff";
    static final String DFA48_minS =
        "\1\6\1\0\21\uffff";
    static final String DFA48_maxS =
        "\1\102\1\0\21\uffff";
    static final String DFA48_acceptS =
        "\2\uffff\1\2\17\uffff\1\1";
    static final String DFA48_specialS =
        "\1\uffff\1\0\21\uffff}>";
    static final String[] DFA48_transitionS = {
            "\1\1\2\2\1\uffff\3\2\21\uffff\1\2\25\uffff\4\2\2\uffff\1\2\1"+
            "\uffff\4\2\1\uffff\2\2",
            "\1\uffff",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            ""
    };

    static final short[] DFA48_eot = DFA.unpackEncodedString(DFA48_eotS);
    static final short[] DFA48_eof = DFA.unpackEncodedString(DFA48_eofS);
    static final char[] DFA48_min = DFA.unpackEncodedStringToUnsignedChars(DFA48_minS);
    static final char[] DFA48_max = DFA.unpackEncodedStringToUnsignedChars(DFA48_maxS);
    static final short[] DFA48_accept = DFA.unpackEncodedString(DFA48_acceptS);
    static final short[] DFA48_special = DFA.unpackEncodedString(DFA48_specialS);
    static final short[][] DFA48_transition;

    static {
        int numStates = DFA48_transitionS.length;
        DFA48_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA48_transition[i] = DFA.unpackEncodedString(DFA48_transitionS[i]);
        }
    }

    class DFA48 extends DFA {

        public DFA48(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 48;
            this.eot = DFA48_eot;
            this.eof = DFA48_eof;
            this.min = DFA48_min;
            this.max = DFA48_max;
            this.accept = DFA48_accept;
            this.special = DFA48_special;
            this.transition = DFA48_transition;
        }
        public String getDescription() {
            return "306:1: ({...}?moreName= ID )?";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA48_1 = input.LA(1);

                         
                        int index48_1 = input.index();
                        input.rewind();
                        s = -1;
                        if ( ((synpred76_SJaql()&&(!((genericOperator_scope)genericOperator_stack.peek()).operatorInfo.hasProperty(((operatorOption_scope)operatorOption_stack.peek()).optionName)))) ) {s = 18;}

                        else if ( (true) ) {s = 2;}

                         
                        input.seek(index48_1);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 48, _s, input);
            error(nvae);
            throw nvae;
        }
    }
    static final String DFA52_eotS =
        "\37\uffff";
    static final String DFA52_eofS =
        "\1\2\36\uffff";
    static final String DFA52_minS =
        "\1\6\1\0\35\uffff";
    static final String DFA52_maxS =
        "\1\100\1\0\35\uffff";
    static final String DFA52_acceptS =
        "\2\uffff\1\2\33\uffff\1\1";
    static final String DFA52_specialS =
        "\1\uffff\1\0\35\uffff}>";
    static final String[] DFA52_transitionS = {
            "\1\1\2\uffff\1\2\20\uffff\1\2\4\uffff\2\2\1\uffff\22\2\4\uffff"+
            "\2\2\1\uffff\1\2\3\uffff\2\2",
            "\1\uffff",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            ""
    };

    static final short[] DFA52_eot = DFA.unpackEncodedString(DFA52_eotS);
    static final short[] DFA52_eof = DFA.unpackEncodedString(DFA52_eofS);
    static final char[] DFA52_min = DFA.unpackEncodedStringToUnsignedChars(DFA52_minS);
    static final char[] DFA52_max = DFA.unpackEncodedStringToUnsignedChars(DFA52_maxS);
    static final short[] DFA52_accept = DFA.unpackEncodedString(DFA52_acceptS);
    static final short[] DFA52_special = DFA.unpackEncodedString(DFA52_specialS);
    static final short[][] DFA52_transition;

    static {
        int numStates = DFA52_transitionS.length;
        DFA52_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA52_transition[i] = DFA.unpackEncodedString(DFA52_transitionS[i]);
        }
    }

    class DFA52 extends DFA {

        public DFA52(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 52;
            this.eot = DFA52_eot;
            this.eof = DFA52_eof;
            this.min = DFA52_min;
            this.max = DFA52_max;
            this.accept = DFA52_accept;
            this.special = DFA52_special;
            this.transition = DFA52_transition;
        }
        public String getDescription() {
            return "328:1: (inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::inputNames.size() - 1)] )?";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA52_1 = input.LA(1);

                         
                        int index52_1 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred80_SJaql()) ) {s = 30;}

                        else if ( (true) ) {s = 2;}

                         
                        input.seek(index52_1);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 52, _s, input);
            error(nvae);
            throw nvae;
        }
    }
 

    public static final BitSet FOLLOW_statement_in_script124 = new BitSet(new long[]{0x0000000004000000L});
    public static final BitSet FOLLOW_26_in_script127 = new BitSet(new long[]{0x00000000080000C0L,0x0000000000000006L});
    public static final BitSet FOLLOW_statement_in_script129 = new BitSet(new long[]{0x0000000004000000L});
    public static final BitSet FOLLOW_26_in_script133 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_assignment_in_statement145 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_operator_in_statement149 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_packageImport_in_statement153 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_functionDefinition_in_statement157 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_javaudf_in_statement161 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_27_in_packageImport176 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_packageImport180 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_assignment195 = new BitSet(new long[]{0x0000000010000000L});
    public static final BitSet FOLLOW_28_in_assignment197 = new BitSet(new long[]{0x0000000000000040L,0x0000000000000006L});
    public static final BitSet FOLLOW_operator_in_assignment201 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_functionDefinition223 = new BitSet(new long[]{0x0000000010000000L});
    public static final BitSet FOLLOW_28_in_functionDefinition225 = new BitSet(new long[]{0x0000000020000000L});
    public static final BitSet FOLLOW_29_in_functionDefinition227 = new BitSet(new long[]{0x0000000040000000L});
    public static final BitSet FOLLOW_30_in_functionDefinition229 = new BitSet(new long[]{0x0000000100000040L});
    public static final BitSet FOLLOW_ID_in_functionDefinition238 = new BitSet(new long[]{0x0000000180000000L});
    public static final BitSet FOLLOW_31_in_functionDefinition245 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_functionDefinition249 = new BitSet(new long[]{0x0000000180000000L});
    public static final BitSet FOLLOW_32_in_functionDefinition260 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_contextAwareExpression_in_functionDefinition272 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_javaudf290 = new BitSet(new long[]{0x0000000010000000L});
    public static final BitSet FOLLOW_28_in_javaudf292 = new BitSet(new long[]{0x0000000200000000L});
    public static final BitSet FOLLOW_33_in_javaudf294 = new BitSet(new long[]{0x0000000040000000L});
    public static final BitSet FOLLOW_30_in_javaudf296 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_STRING_in_javaudf300 = new BitSet(new long[]{0x0000000100000000L});
    public static final BitSet FOLLOW_32_in_javaudf302 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_expression_in_contextAwareExpression330 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ternaryExpression_in_expression340 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_orExpression_in_ternaryExpression351 = new BitSet(new long[]{0x0000000400000000L});
    public static final BitSet FOLLOW_34_in_ternaryExpression354 = new BitSet(new long[]{0xF4F0000840001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_expression_in_ternaryExpression358 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_35_in_ternaryExpression361 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_expression_in_ternaryExpression365 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_orExpression_in_ternaryExpression388 = new BitSet(new long[]{0x0000001000000000L});
    public static final BitSet FOLLOW_36_in_ternaryExpression390 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_expression_in_ternaryExpression394 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_orExpression_in_ternaryExpression414 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_andExpression_in_orExpression427 = new BitSet(new long[]{0x0000006000000002L});
    public static final BitSet FOLLOW_37_in_orExpression431 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_38_in_orExpression435 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_andExpression_in_orExpression440 = new BitSet(new long[]{0x0000006000000002L});
    public static final BitSet FOLLOW_elementExpression_in_andExpression474 = new BitSet(new long[]{0x0000018000000002L});
    public static final BitSet FOLLOW_39_in_andExpression478 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_40_in_andExpression482 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_elementExpression_in_andExpression487 = new BitSet(new long[]{0x0000018000000002L});
    public static final BitSet FOLLOW_comparisonExpression_in_elementExpression521 = new BitSet(new long[]{0x0000060000000002L});
    public static final BitSet FOLLOW_41_in_elementExpression526 = new BitSet(new long[]{0x0000040000000000L});
    public static final BitSet FOLLOW_42_in_elementExpression529 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_comparisonExpression_in_elementExpression533 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_arithmeticExpression_in_comparisonExpression574 = new BitSet(new long[]{0x0001F80000000002L});
    public static final BitSet FOLLOW_43_in_comparisonExpression580 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_44_in_comparisonExpression586 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_45_in_comparisonExpression592 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_46_in_comparisonExpression598 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_47_in_comparisonExpression604 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_48_in_comparisonExpression610 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_arithmeticExpression_in_comparisonExpression615 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_multiplicationExpression_in_arithmeticExpression695 = new BitSet(new long[]{0x0006000000000002L});
    public static final BitSet FOLLOW_49_in_arithmeticExpression701 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_50_in_arithmeticExpression707 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_multiplicationExpression_in_arithmeticExpression712 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_preincrementExpression_in_multiplicationExpression755 = new BitSet(new long[]{0x0008000000000202L});
    public static final BitSet FOLLOW_STAR_in_multiplicationExpression761 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_51_in_multiplicationExpression767 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_preincrementExpression_in_multiplicationExpression772 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_52_in_preincrementExpression813 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_preincrementExpression_in_preincrementExpression815 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_53_in_preincrementExpression820 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_preincrementExpression_in_preincrementExpression822 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_unaryExpression_in_preincrementExpression827 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_unaryExpression837 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_castExpression_in_unaryExpression846 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_30_in_castExpression856 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_castExpression860 = new BitSet(new long[]{0x0000000100000000L});
    public static final BitSet FOLLOW_32_in_castExpression862 = new BitSet(new long[]{0xF400000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_generalPathExpression_in_castExpression866 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_generalPathExpression_in_castExpression873 = new BitSet(new long[]{0x0100000000000000L});
    public static final BitSet FOLLOW_56_in_castExpression875 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_castExpression879 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_generalPathExpression_in_castExpression886 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_contextAwarePathExpression_in_generalPathExpression915 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_pathExpression_in_generalPathExpression920 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_contextAwarePathExpression943 = new BitSet(new long[]{0x8200000000000002L});
    public static final BitSet FOLLOW_57_in_contextAwarePathExpression954 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_contextAwarePathExpression959 = new BitSet(new long[]{0x8200000000000002L});
    public static final BitSet FOLLOW_arrayAccess_in_contextAwarePathExpression977 = new BitSet(new long[]{0x8200000000000002L});
    public static final BitSet FOLLOW_valueExpression_in_pathExpression1019 = new BitSet(new long[]{0x8200000000000000L});
    public static final BitSet FOLLOW_57_in_pathExpression1033 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_pathExpression1038 = new BitSet(new long[]{0x8200000000000002L});
    public static final BitSet FOLLOW_arrayAccess_in_pathExpression1056 = new BitSet(new long[]{0x8200000000000002L});
    public static final BitSet FOLLOW_valueExpression_in_pathExpression1083 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_methodCall_in_valueExpression1092 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_parenthesesExpression_in_valueExpression1098 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_literal_in_valueExpression1104 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_valueExpression1110 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_valueExpression1119 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_arrayCreation_in_valueExpression1128 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_objectCreation_in_valueExpression1134 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_operatorExpression_in_valueExpression1140 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_operator_in_operatorExpression1150 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_30_in_parenthesesExpression1171 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_expression_in_parenthesesExpression1173 = new BitSet(new long[]{0x0000000100000000L});
    public static final BitSet FOLLOW_32_in_parenthesesExpression1175 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_methodCall1198 = new BitSet(new long[]{0x0000000040000000L});
    public static final BitSet FOLLOW_30_in_methodCall1200 = new BitSet(new long[]{0xF4F0000140001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_expression_in_methodCall1207 = new BitSet(new long[]{0x0000000180000000L});
    public static final BitSet FOLLOW_31_in_methodCall1213 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_expression_in_methodCall1217 = new BitSet(new long[]{0x0000000180000000L});
    public static final BitSet FOLLOW_32_in_methodCall1227 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_fieldAssignment1245 = new BitSet(new long[]{0x0200000000000000L});
    public static final BitSet FOLLOW_57_in_fieldAssignment1247 = new BitSet(new long[]{0x0000000000000200L});
    public static final BitSet FOLLOW_STAR_in_fieldAssignment1249 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_fieldAssignment1259 = new BitSet(new long[]{0x0200000000000000L});
    public static final BitSet FOLLOW_57_in_fieldAssignment1261 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_fieldAssignment1263 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_fieldAssignment1273 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_fieldAssignment1282 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_35_in_fieldAssignment1284 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_expression_in_fieldAssignment1286 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_58_in_objectCreation1308 = new BitSet(new long[]{0x08000000000000C0L});
    public static final BitSet FOLLOW_fieldAssignment_in_objectCreation1311 = new BitSet(new long[]{0x0800000080000000L});
    public static final BitSet FOLLOW_31_in_objectCreation1314 = new BitSet(new long[]{0x00000000000000C0L});
    public static final BitSet FOLLOW_fieldAssignment_in_objectCreation1316 = new BitSet(new long[]{0x0800000080000000L});
    public static final BitSet FOLLOW_31_in_objectCreation1320 = new BitSet(new long[]{0x0800000000000000L});
    public static final BitSet FOLLOW_59_in_objectCreation1325 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_60_in_literal1349 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_61_in_literal1365 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_DECIMAL_in_literal1381 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_STRING_in_literal1397 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_INTEGER_in_literal1414 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_UINT_in_literal1431 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_62_in_literal1446 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_63_in_arrayAccess1460 = new BitSet(new long[]{0x0000000000000200L});
    public static final BitSet FOLLOW_STAR_in_arrayAccess1462 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_64_in_arrayAccess1464 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_63_in_arrayAccess1482 = new BitSet(new long[]{0x0000000000001800L});
    public static final BitSet FOLLOW_INTEGER_in_arrayAccess1487 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_UINT_in_arrayAccess1493 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_64_in_arrayAccess1496 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_63_in_arrayAccess1514 = new BitSet(new long[]{0x0000000000001800L});
    public static final BitSet FOLLOW_INTEGER_in_arrayAccess1519 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_UINT_in_arrayAccess1525 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_35_in_arrayAccess1528 = new BitSet(new long[]{0x0000000000001800L});
    public static final BitSet FOLLOW_INTEGER_in_arrayAccess1533 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_UINT_in_arrayAccess1539 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_64_in_arrayAccess1542 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_63_in_arrayCreation1567 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_expression_in_arrayCreation1571 = new BitSet(new long[]{0x0000000080000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_31_in_arrayCreation1574 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_expression_in_arrayCreation1578 = new BitSet(new long[]{0x0000000080000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_31_in_arrayCreation1582 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_64_in_arrayCreation1585 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_readOperator_in_operator1619 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_writeOperator_in_operator1623 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_genericOperator_in_operator1627 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_65_in_readOperator1641 = new BitSet(new long[]{0x0000000000000140L});
    public static final BitSet FOLLOW_ID_in_readOperator1646 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_STRING_in_readOperator1651 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_readOperator1657 = new BitSet(new long[]{0x0000000040000000L});
    public static final BitSet FOLLOW_30_in_readOperator1659 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_STRING_in_readOperator1663 = new BitSet(new long[]{0x0000000100000000L});
    public static final BitSet FOLLOW_32_in_readOperator1665 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_66_in_writeOperator1679 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_VAR_in_writeOperator1683 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000008L});
    public static final BitSet FOLLOW_67_in_writeOperator1685 = new BitSet(new long[]{0x0000000000000140L});
    public static final BitSet FOLLOW_ID_in_writeOperator1690 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_STRING_in_writeOperator1695 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_writeOperator1701 = new BitSet(new long[]{0x0000000040000000L});
    public static final BitSet FOLLOW_30_in_writeOperator1703 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_STRING_in_writeOperator1707 = new BitSet(new long[]{0x0000000100000000L});
    public static final BitSet FOLLOW_32_in_writeOperator1709 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_genericOperator1729 = new BitSet(new long[]{0x00000000000000C0L,0x0000000000000010L});
    public static final BitSet FOLLOW_operatorFlag_in_genericOperator1737 = new BitSet(new long[]{0x00000000000000C0L,0x0000000000000010L});
    public static final BitSet FOLLOW_input_in_genericOperator1740 = new BitSet(new long[]{0x0000000080000042L});
    public static final BitSet FOLLOW_31_in_genericOperator1743 = new BitSet(new long[]{0x00000000000000C0L,0x0000000000000010L});
    public static final BitSet FOLLOW_input_in_genericOperator1745 = new BitSet(new long[]{0x0000000080000042L});
    public static final BitSet FOLLOW_operatorOption_in_genericOperator1750 = new BitSet(new long[]{0x0000000000000042L});
    public static final BitSet FOLLOW_ID_in_operatorOption1770 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_ID_in_operatorOption1779 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_contextAwareExpression_in_operatorOption1789 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_operatorFlag1810 = new BitSet(new long[]{0x0000000000000042L});
    public static final BitSet FOLLOW_ID_in_operatorFlag1820 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_68_in_input1842 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_VAR_in_input1850 = new BitSet(new long[]{0x0000040000000000L});
    public static final BitSet FOLLOW_42_in_input1852 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_VAR_in_input1858 = new BitSet(new long[]{0x0000000000000042L});
    public static final BitSet FOLLOW_ID_in_input1866 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_contextAwareExpression_in_input1875 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_orExpression_in_synpred9_SJaql351 = new BitSet(new long[]{0x0000000400000000L});
    public static final BitSet FOLLOW_34_in_synpred9_SJaql354 = new BitSet(new long[]{0xF4F0000840001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_expression_in_synpred9_SJaql358 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_35_in_synpred9_SJaql361 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_expression_in_synpred9_SJaql365 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_orExpression_in_synpred10_SJaql388 = new BitSet(new long[]{0x0000001000000000L});
    public static final BitSet FOLLOW_36_in_synpred10_SJaql390 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_expression_in_synpred10_SJaql394 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_synpred12_SJaql430 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_andExpression_in_synpred12_SJaql440 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_synpred14_SJaql477 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_elementExpression_in_synpred14_SJaql487 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_41_in_synpred16_SJaql526 = new BitSet(new long[]{0x0000040000000000L});
    public static final BitSet FOLLOW_42_in_synpred16_SJaql529 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_comparisonExpression_in_synpred16_SJaql533 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_43_in_synpred22_SJaql580 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_44_in_synpred22_SJaql586 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_45_in_synpred22_SJaql592 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_46_in_synpred22_SJaql598 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_47_in_synpred22_SJaql604 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_48_in_synpred22_SJaql610 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_arithmeticExpression_in_synpred22_SJaql615 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_49_in_synpred24_SJaql701 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_50_in_synpred24_SJaql707 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_multiplicationExpression_in_synpred24_SJaql712 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_STAR_in_synpred26_SJaql761 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_51_in_synpred26_SJaql767 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_preincrementExpression_in_synpred26_SJaql772 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_30_in_synpred31_SJaql856 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_synpred31_SJaql860 = new BitSet(new long[]{0x0000000100000000L});
    public static final BitSet FOLLOW_32_in_synpred31_SJaql862 = new BitSet(new long[]{0xF400000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_generalPathExpression_in_synpred31_SJaql866 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_generalPathExpression_in_synpred32_SJaql873 = new BitSet(new long[]{0x0100000000000000L});
    public static final BitSet FOLLOW_56_in_synpred32_SJaql875 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_synpred32_SJaql879 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_contextAwarePathExpression_in_synpred33_SJaql915 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_57_in_synpred34_SJaql954 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_synpred34_SJaql959 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_arrayAccess_in_synpred35_SJaql977 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_57_in_synpred36_SJaql1033 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_synpred36_SJaql1038 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_arrayAccess_in_synpred37_SJaql1056 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_valueExpression_in_synpred38_SJaql1019 = new BitSet(new long[]{0x8200000000000000L});
    public static final BitSet FOLLOW_57_in_synpred38_SJaql1033 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_synpred38_SJaql1038 = new BitSet(new long[]{0x8200000000000002L});
    public static final BitSet FOLLOW_arrayAccess_in_synpred38_SJaql1056 = new BitSet(new long[]{0x8200000000000002L});
    public static final BitSet FOLLOW_ID_in_synpred43_SJaql1119 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_31_in_synpred74_SJaql1743 = new BitSet(new long[]{0x00000000000000C0L,0x0000000000000010L});
    public static final BitSet FOLLOW_input_in_synpred74_SJaql1745 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_operatorOption_in_synpred75_SJaql1750 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_synpred76_SJaql1779 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_synpred77_SJaql1820 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_synpred79_SJaql1850 = new BitSet(new long[]{0x0000040000000000L});
    public static final BitSet FOLLOW_42_in_synpred79_SJaql1852 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_synpred80_SJaql1866 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_contextAwareExpression_in_synpred80_SJaql1875 = new BitSet(new long[]{0x0000000000000002L});

}