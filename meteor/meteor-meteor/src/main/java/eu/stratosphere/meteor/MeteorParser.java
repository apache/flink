// $ANTLR 3.3 Nov 30, 2010 12:46:29 /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g 2012-07-25 14:35:53
 
package eu.stratosphere.meteor; 

import eu.stratosphere.sopremo.operator.*;
import eu.stratosphere.sopremo.io.*;
import eu.stratosphere.sopremo.query.*;
import eu.stratosphere.sopremo.pact.*;
import eu.stratosphere.sopremo.expressions.*;
import eu.stratosphere.sopremo.function.*;
import java.math.*;
import java.util.IdentityHashMap;


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import org.antlr.runtime.tree.*;

public class MeteorParser extends MeteorParserBase {
    public static final String[] tokenNames = new String[] {
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "EXPRESSION", "OPERATOR", "ID", "VAR", "STRING", "STAR", "DECIMAL", "INTEGER", "UINT", "LOWER_LETTER", "UPPER_LETTER", "DIGIT", "SIGN", "COMMENT", "APOSTROPHE", "QUOTATION", "WS", "UNICODE_ESC", "OCTAL_ESC", "ESC_SEQ", "HEX_DIGIT", "EXPONENT", "';'", "'using'", "','", "'='", "'fn'", "'('", "')'", "'javaudf'", "'?'", "':'", "'if'", "'or'", "'||'", "'and'", "'&&'", "'not'", "'in'", "'<='", "'>='", "'<'", "'>'", "'=='", "'!='", "'+'", "'-'", "'/'", "'++'", "'--'", "'!'", "'~'", "'as'", "'.'", "'{'", "'}'", "'true'", "'false'", "'null'", "'['", "']'", "'read'", "'from'", "'write'", "'to'", "'preserve'"
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
    public static final int T__69=69;
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


        public MeteorParser(TokenStream input) {
            this(input, new RecognizerSharedState());
        }
        public MeteorParser(TokenStream input, RecognizerSharedState state) {
            super(input, state);
            this.state.ruleMemo = new HashMap[121+1];
             
             
        }
        
    protected TreeAdaptor adaptor = new CommonTreeAdaptor();

    public void setTreeAdaptor(TreeAdaptor adaptor) {
        this.adaptor = adaptor;
    }
    public TreeAdaptor getTreeAdaptor() {
        return adaptor;
    }

    public String[] getTokenNames() { return MeteorParser.tokenNames; }
    public String getGrammarFileName() { return "/home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g"; }


      private boolean setInnerOutput(Token VAR, Operator<?> op) {
    	  JsonStreamExpression output = new JsonStreamExpression(((operator_scope)operator_stack.peek()).result.getOutput(((objectCreation_scope)objectCreation_stack.peek()).mappings.size()));
    	  ((objectCreation_scope)objectCreation_stack.peek()).mappings.add(new ObjectCreation.TagMapping(output, new JsonStreamExpression(op)));
    	  getVariableRegistry().getRegistry(1).put(VAR.getText(), output);
    	  return true;
    	}

      public void parseSinks() throws RecognitionException {
        script();
      }
      
      private EvaluationExpression makePath(Token inputVar, String... path) {
          EvaluationExpression selection = getVariable(inputVar).toInputSelection(((operator_scope)operator_stack.peek()).result);
          
          List<EvaluationExpression> accesses = new ArrayList<EvaluationExpression>();
          accesses.add((EvaluationExpression) selection);
          for (String fragment : path)
            accesses.add(new ObjectAccess(fragment));
          return PathExpression.wrapIfNecessary(accesses);
        }


    public static class script_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "script"
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:59:1: script : ( statement ';' )+ ->;
    public final MeteorParser.script_return script() throws RecognitionException {
        MeteorParser.script_return retval = new MeteorParser.script_return();
        retval.start = input.LT(1);
        int script_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token char_literal2=null;
        MeteorParser.statement_return statement1 = null;


        EvaluationExpression char_literal2_tree=null;
        RewriteRuleTokenStream stream_26=new RewriteRuleTokenStream(adaptor,"token 26");
        RewriteRuleSubtreeStream stream_statement=new RewriteRuleSubtreeStream(adaptor,"rule statement");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 1) ) { return retval; }
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:60:2: ( ( statement ';' )+ ->)
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:60:5: ( statement ';' )+
            {
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:60:5: ( statement ';' )+
            int cnt1=0;
            loop1:
            do {
                int alt1=2;
                int LA1_0 = input.LA(1);

                if ( ((LA1_0>=ID && LA1_0<=VAR)||LA1_0==27||LA1_0==65||LA1_0==67) ) {
                    alt1=1;
                }


                switch (alt1) {
            	case 1 :
            	    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:60:6: statement ';'
            	    {
            	    pushFollow(FOLLOW_statement_in_script125);
            	    statement1=statement();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_statement.add(statement1.getTree());
            	    char_literal2=(Token)match(input,26,FOLLOW_26_in_script127); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_26.add(char_literal2);


            	    }
            	    break;

            	default :
            	    if ( cnt1 >= 1 ) break loop1;
            	    if (state.backtracking>0) {state.failed=true; return retval;}
                        EarlyExitException eee =
                            new EarlyExitException(1, input);
                        throw eee;
                }
                cnt1++;
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
            // 60:22: ->
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
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:62:1: statement : ( assignment | operator | packageImport | functionDefinition | javaudf ) ->;
    public final MeteorParser.statement_return statement() throws RecognitionException {
        MeteorParser.statement_return retval = new MeteorParser.statement_return();
        retval.start = input.LT(1);
        int statement_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        MeteorParser.assignment_return assignment3 = null;

        MeteorParser.operator_return operator4 = null;

        MeteorParser.packageImport_return packageImport5 = null;

        MeteorParser.functionDefinition_return functionDefinition6 = null;

        MeteorParser.javaudf_return javaudf7 = null;


        RewriteRuleSubtreeStream stream_assignment=new RewriteRuleSubtreeStream(adaptor,"rule assignment");
        RewriteRuleSubtreeStream stream_functionDefinition=new RewriteRuleSubtreeStream(adaptor,"rule functionDefinition");
        RewriteRuleSubtreeStream stream_javaudf=new RewriteRuleSubtreeStream(adaptor,"rule javaudf");
        RewriteRuleSubtreeStream stream_operator=new RewriteRuleSubtreeStream(adaptor,"rule operator");
        RewriteRuleSubtreeStream stream_packageImport=new RewriteRuleSubtreeStream(adaptor,"rule packageImport");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 2) ) { return retval; }
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:63:2: ( ( assignment | operator | packageImport | functionDefinition | javaudf ) ->)
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:63:4: ( assignment | operator | packageImport | functionDefinition | javaudf )
            {
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:63:4: ( assignment | operator | packageImport | functionDefinition | javaudf )
            int alt2=5;
            switch ( input.LA(1) ) {
            case VAR:
                {
                alt2=1;
                }
                break;
            case 65:
            case 67:
                {
                alt2=2;
                }
                break;
            case ID:
                {
                int LA2_3 = input.LA(2);

                if ( ((LA2_3>=ID && LA2_3<=VAR)||LA2_3==35||LA2_3==63||LA2_3==69) ) {
                    alt2=2;
                }
                else if ( (LA2_3==29) ) {
                    int LA2_5 = input.LA(3);

                    if ( (LA2_5==30) ) {
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
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:63:5: assignment
                    {
                    pushFollow(FOLLOW_assignment_in_statement141);
                    assignment3=assignment();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_assignment.add(assignment3.getTree());

                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:63:18: operator
                    {
                    pushFollow(FOLLOW_operator_in_statement145);
                    operator4=operator();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_operator.add(operator4.getTree());

                    }
                    break;
                case 3 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:63:29: packageImport
                    {
                    pushFollow(FOLLOW_packageImport_in_statement149);
                    packageImport5=packageImport();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_packageImport.add(packageImport5.getTree());

                    }
                    break;
                case 4 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:63:45: functionDefinition
                    {
                    pushFollow(FOLLOW_functionDefinition_in_statement153);
                    functionDefinition6=functionDefinition();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_functionDefinition.add(functionDefinition6.getTree());

                    }
                    break;
                case 5 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:63:66: javaudf
                    {
                    pushFollow(FOLLOW_javaudf_in_statement157);
                    javaudf7=javaudf();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_javaudf.add(javaudf7.getTree());

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
            // 63:75: ->
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
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:65:1: packageImport : 'using' packageName= ID ',' (additionalPackage= ID )* ->;
    public final MeteorParser.packageImport_return packageImport() throws RecognitionException {
        MeteorParser.packageImport_return retval = new MeteorParser.packageImport_return();
        retval.start = input.LT(1);
        int packageImport_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token packageName=null;
        Token additionalPackage=null;
        Token string_literal8=null;
        Token char_literal9=null;

        EvaluationExpression packageName_tree=null;
        EvaluationExpression additionalPackage_tree=null;
        EvaluationExpression string_literal8_tree=null;
        EvaluationExpression char_literal9_tree=null;
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_27=new RewriteRuleTokenStream(adaptor,"token 27");
        RewriteRuleTokenStream stream_28=new RewriteRuleTokenStream(adaptor,"token 28");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 3) ) { return retval; }
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:66:3: ( 'using' packageName= ID ',' (additionalPackage= ID )* ->)
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:66:6: 'using' packageName= ID ',' (additionalPackage= ID )*
            {
            string_literal8=(Token)match(input,27,FOLLOW_27_in_packageImport172); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_27.add(string_literal8);

            packageName=(Token)match(input,ID,FOLLOW_ID_in_packageImport176); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(packageName);

            if ( state.backtracking==0 ) {
               getPackageManager().importPackage((packageName!=null?packageName.getText():null)); 
            }
            char_literal9=(Token)match(input,28,FOLLOW_28_in_packageImport180); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_28.add(char_literal9);

            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:67:6: (additionalPackage= ID )*
            loop3:
            do {
                int alt3=2;
                int LA3_0 = input.LA(1);

                if ( (LA3_0==ID) ) {
                    alt3=1;
                }


                switch (alt3) {
            	case 1 :
            	    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:67:7: additionalPackage= ID
            	    {
            	    additionalPackage=(Token)match(input,ID,FOLLOW_ID_in_packageImport191); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_ID.add(additionalPackage);

            	    if ( state.backtracking==0 ) {
            	       getPackageManager().importPackage((additionalPackage!=null?additionalPackage.getText():null)); 
            	    }

            	    }
            	    break;

            	default :
            	    break loop3;
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
            // 67:94: ->
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
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:69:1: assignment : target= VAR '=' source= operator ->;
    public final MeteorParser.assignment_return assignment() throws RecognitionException {
        MeteorParser.assignment_return retval = new MeteorParser.assignment_return();
        retval.start = input.LT(1);
        int assignment_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token target=null;
        Token char_literal10=null;
        MeteorParser.operator_return source = null;


        EvaluationExpression target_tree=null;
        EvaluationExpression char_literal10_tree=null;
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_29=new RewriteRuleTokenStream(adaptor,"token 29");
        RewriteRuleSubtreeStream stream_operator=new RewriteRuleSubtreeStream(adaptor,"rule operator");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 4) ) { return retval; }
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:70:2: (target= VAR '=' source= operator ->)
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:70:4: target= VAR '=' source= operator
            {
            target=(Token)match(input,VAR,FOLLOW_VAR_in_assignment209); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_VAR.add(target);

            char_literal10=(Token)match(input,29,FOLLOW_29_in_assignment211); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_29.add(char_literal10);

            pushFollow(FOLLOW_operator_in_assignment215);
            source=operator();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_operator.add(source.getTree());
            if ( state.backtracking==0 ) {
               putVariable(target, new JsonStreamExpression((source!=null?source.op:null))); 
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
            // 70:99: ->
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
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:72:1: functionDefinition : name= ID '=' 'fn' '(' (param= ID ( ',' param= ID )* )? ')' def= contextAwareExpression[null] ->;
    public final MeteorParser.functionDefinition_return functionDefinition() throws RecognitionException {
        MeteorParser.functionDefinition_return retval = new MeteorParser.functionDefinition_return();
        retval.start = input.LT(1);
        int functionDefinition_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token name=null;
        Token param=null;
        Token char_literal11=null;
        Token string_literal12=null;
        Token char_literal13=null;
        Token char_literal14=null;
        Token char_literal15=null;
        MeteorParser.contextAwareExpression_return def = null;


        EvaluationExpression name_tree=null;
        EvaluationExpression param_tree=null;
        EvaluationExpression char_literal11_tree=null;
        EvaluationExpression string_literal12_tree=null;
        EvaluationExpression char_literal13_tree=null;
        EvaluationExpression char_literal14_tree=null;
        EvaluationExpression char_literal15_tree=null;
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
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:74:3: (name= ID '=' 'fn' '(' (param= ID ( ',' param= ID )* )? ')' def= contextAwareExpression[null] ->)
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:74:5: name= ID '=' 'fn' '(' (param= ID ( ',' param= ID )* )? ')' def= contextAwareExpression[null]
            {
            name=(Token)match(input,ID,FOLLOW_ID_in_functionDefinition237); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);

            char_literal11=(Token)match(input,29,FOLLOW_29_in_functionDefinition239); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_29.add(char_literal11);

            string_literal12=(Token)match(input,30,FOLLOW_30_in_functionDefinition241); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_30.add(string_literal12);

            char_literal13=(Token)match(input,31,FOLLOW_31_in_functionDefinition243); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_31.add(char_literal13);

            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:75:3: (param= ID ( ',' param= ID )* )?
            int alt5=2;
            int LA5_0 = input.LA(1);

            if ( (LA5_0==ID) ) {
                alt5=1;
            }
            switch (alt5) {
                case 1 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:75:4: param= ID ( ',' param= ID )*
                    {
                    param=(Token)match(input,ID,FOLLOW_ID_in_functionDefinition252); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(param);

                    if ( state.backtracking==0 ) {
                       params.add(param); 
                    }
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:76:3: ( ',' param= ID )*
                    loop4:
                    do {
                        int alt4=2;
                        int LA4_0 = input.LA(1);

                        if ( (LA4_0==28) ) {
                            alt4=1;
                        }


                        switch (alt4) {
                    	case 1 :
                    	    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:76:4: ',' param= ID
                    	    {
                    	    char_literal14=(Token)match(input,28,FOLLOW_28_in_functionDefinition259); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_28.add(char_literal14);

                    	    param=(Token)match(input,ID,FOLLOW_ID_in_functionDefinition263); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_ID.add(param);

                    	    if ( state.backtracking==0 ) {
                    	       params.add(param); 
                    	    }

                    	    }
                    	    break;

                    	default :
                    	    break loop4;
                        }
                    } while (true);


                    }
                    break;

            }

            char_literal15=(Token)match(input,32,FOLLOW_32_in_functionDefinition274); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_32.add(char_literal15);

            if ( state.backtracking==0 ) {
               
                  addScope();
                  for(int index = 0; index < params.size(); index++) 
                    putVariable(params.get(index), new JsonStreamExpression(null, index)); 
                
            }
            pushFollow(FOLLOW_contextAwareExpression_in_functionDefinition286);
            def=contextAwareExpression(null);

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_contextAwareExpression.add(def.getTree());
            if ( state.backtracking==0 ) {
               
                  addFunction(name.getText(), new ExpressionFunction(params.size(), def.tree));
                  removeScope(); 
                
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
            // 87:5: ->
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
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:89:1: javaudf : name= ID '=' 'javaudf' '(' path= STRING ')' ->;
    public final MeteorParser.javaudf_return javaudf() throws RecognitionException {
        MeteorParser.javaudf_return retval = new MeteorParser.javaudf_return();
        retval.start = input.LT(1);
        int javaudf_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token name=null;
        Token path=null;
        Token char_literal16=null;
        Token string_literal17=null;
        Token char_literal18=null;
        Token char_literal19=null;

        EvaluationExpression name_tree=null;
        EvaluationExpression path_tree=null;
        EvaluationExpression char_literal16_tree=null;
        EvaluationExpression string_literal17_tree=null;
        EvaluationExpression char_literal18_tree=null;
        EvaluationExpression char_literal19_tree=null;
        RewriteRuleTokenStream stream_32=new RewriteRuleTokenStream(adaptor,"token 32");
        RewriteRuleTokenStream stream_31=new RewriteRuleTokenStream(adaptor,"token 31");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_33=new RewriteRuleTokenStream(adaptor,"token 33");
        RewriteRuleTokenStream stream_STRING=new RewriteRuleTokenStream(adaptor,"token STRING");
        RewriteRuleTokenStream stream_29=new RewriteRuleTokenStream(adaptor,"token 29");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 6) ) { return retval; }
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:90:3: (name= ID '=' 'javaudf' '(' path= STRING ')' ->)
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:90:5: name= ID '=' 'javaudf' '(' path= STRING ')'
            {
            name=(Token)match(input,ID,FOLLOW_ID_in_javaudf307); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);

            char_literal16=(Token)match(input,29,FOLLOW_29_in_javaudf309); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_29.add(char_literal16);

            string_literal17=(Token)match(input,33,FOLLOW_33_in_javaudf311); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_33.add(string_literal17);

            char_literal18=(Token)match(input,31,FOLLOW_31_in_javaudf313); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_31.add(char_literal18);

            path=(Token)match(input,STRING,FOLLOW_STRING_in_javaudf317); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_STRING.add(path);

            char_literal19=(Token)match(input,32,FOLLOW_32_in_javaudf319); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_32.add(char_literal19);

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
            // 91:53: ->
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
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:93:1: contextAwareExpression[EvaluationExpression contextExpression] : expression ;
    public final MeteorParser.contextAwareExpression_return contextAwareExpression(EvaluationExpression contextExpression) throws RecognitionException {
        contextAwareExpression_stack.push(new contextAwareExpression_scope());
        MeteorParser.contextAwareExpression_return retval = new MeteorParser.contextAwareExpression_return();
        retval.start = input.LT(1);
        int contextAwareExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        MeteorParser.expression_return expression20 = null;



         ((contextAwareExpression_scope)contextAwareExpression_stack.peek()).context = contextExpression; 
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 7) ) { return retval; }
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:96:3: ( expression )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:96:5: expression
            {
            root_0 = (EvaluationExpression)adaptor.nil();

            pushFollow(FOLLOW_expression_in_contextAwareExpression347);
            expression20=expression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, expression20.getTree());

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
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:98:1: expression : ( ternaryExpression | operatorExpression );
    public final MeteorParser.expression_return expression() throws RecognitionException {
        MeteorParser.expression_return retval = new MeteorParser.expression_return();
        retval.start = input.LT(1);
        int expression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        MeteorParser.ternaryExpression_return ternaryExpression21 = null;

        MeteorParser.operatorExpression_return operatorExpression22 = null;



        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 8) ) { return retval; }
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:99:3: ( ternaryExpression | operatorExpression )
            int alt6=2;
            alt6 = dfa6.predict(input);
            switch (alt6) {
                case 1 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:99:5: ternaryExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_ternaryExpression_in_expression357);
                    ternaryExpression21=ternaryExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, ternaryExpression21.getTree());

                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:100:5: operatorExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_operatorExpression_in_expression363);
                    operatorExpression22=operatorExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, operatorExpression22.getTree());

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
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:102:1: ternaryExpression : (ifClause= orExpression ( '?' (ifExpr= expression )? ':' elseExpr= expression ) -> ^( EXPRESSION[\"TernaryExpression\"] $ifClause) | ifExpr2= orExpression 'if' ifClause2= expression -> ^( EXPRESSION[\"TernaryExpression\"] $ifClause2 $ifExpr2) | orExpression );
    public final MeteorParser.ternaryExpression_return ternaryExpression() throws RecognitionException {
        MeteorParser.ternaryExpression_return retval = new MeteorParser.ternaryExpression_return();
        retval.start = input.LT(1);
        int ternaryExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token char_literal23=null;
        Token char_literal24=null;
        Token string_literal25=null;
        MeteorParser.orExpression_return ifClause = null;

        MeteorParser.expression_return ifExpr = null;

        MeteorParser.expression_return elseExpr = null;

        MeteorParser.orExpression_return ifExpr2 = null;

        MeteorParser.expression_return ifClause2 = null;

        MeteorParser.orExpression_return orExpression26 = null;


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
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:103:2: (ifClause= orExpression ( '?' (ifExpr= expression )? ':' elseExpr= expression ) -> ^( EXPRESSION[\"TernaryExpression\"] $ifClause) | ifExpr2= orExpression 'if' ifClause2= expression -> ^( EXPRESSION[\"TernaryExpression\"] $ifClause2 $ifExpr2) | orExpression )
            int alt8=3;
            alt8 = dfa8.predict(input);
            switch (alt8) {
                case 1 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:103:4: ifClause= orExpression ( '?' (ifExpr= expression )? ':' elseExpr= expression )
                    {
                    pushFollow(FOLLOW_orExpression_in_ternaryExpression374);
                    ifClause=orExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_orExpression.add(ifClause.getTree());
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:103:26: ( '?' (ifExpr= expression )? ':' elseExpr= expression )
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:103:27: '?' (ifExpr= expression )? ':' elseExpr= expression
                    {
                    char_literal23=(Token)match(input,34,FOLLOW_34_in_ternaryExpression377); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_34.add(char_literal23);

                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:103:37: (ifExpr= expression )?
                    int alt7=2;
                    int LA7_0 = input.LA(1);

                    if ( ((LA7_0>=ID && LA7_0<=STRING)||(LA7_0>=DECIMAL && LA7_0<=INTEGER)||LA7_0==31||(LA7_0>=52 && LA7_0<=55)||LA7_0==58||(LA7_0>=60 && LA7_0<=63)||LA7_0==65||LA7_0==67) ) {
                        alt7=1;
                    }
                    switch (alt7) {
                        case 1 :
                            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:0:0: ifExpr= expression
                            {
                            pushFollow(FOLLOW_expression_in_ternaryExpression381);
                            ifExpr=expression();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_expression.add(ifExpr.getTree());

                            }
                            break;

                    }

                    char_literal24=(Token)match(input,35,FOLLOW_35_in_ternaryExpression384); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_35.add(char_literal24);

                    pushFollow(FOLLOW_expression_in_ternaryExpression388);
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
                    // 104:2: -> ^( EXPRESSION[\"TernaryExpression\"] $ifClause)
                    {
                        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:104:5: ^( EXPRESSION[\"TernaryExpression\"] $ifClause)
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
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:105:4: ifExpr2= orExpression 'if' ifClause2= expression
                    {
                    pushFollow(FOLLOW_orExpression_in_ternaryExpression411);
                    ifExpr2=orExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_orExpression.add(ifExpr2.getTree());
                    string_literal25=(Token)match(input,36,FOLLOW_36_in_ternaryExpression413); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_36.add(string_literal25);

                    pushFollow(FOLLOW_expression_in_ternaryExpression417);
                    ifClause2=expression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_expression.add(ifClause2.getTree());


                    // AST REWRITE
                    // elements: ifClause2, ifExpr2
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
                    // 106:2: -> ^( EXPRESSION[\"TernaryExpression\"] $ifClause2 $ifExpr2)
                    {
                        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:106:5: ^( EXPRESSION[\"TernaryExpression\"] $ifClause2 $ifExpr2)
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "TernaryExpression"), root_1);

                        adaptor.addChild(root_1, stream_ifClause2.nextTree());
                        adaptor.addChild(root_1, stream_ifExpr2.nextTree());
                        adaptor.addChild(root_1,  EvaluationExpression.VALUE );

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 3 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:107:5: orExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_orExpression_in_ternaryExpression439);
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
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:109:1: orExpression : exprs+= andExpression ( ( 'or' | '||' ) exprs+= andExpression )* -> { $exprs.size() == 1 }? ->;
    public final MeteorParser.orExpression_return orExpression() throws RecognitionException {
        MeteorParser.orExpression_return retval = new MeteorParser.orExpression_return();
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
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:110:3: (exprs+= andExpression ( ( 'or' | '||' ) exprs+= andExpression )* -> { $exprs.size() == 1 }? ->)
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:110:5: exprs+= andExpression ( ( 'or' | '||' ) exprs+= andExpression )*
            {
            pushFollow(FOLLOW_andExpression_in_orExpression452);
            exprs=andExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_andExpression.add(exprs.getTree());
            if (list_exprs==null) list_exprs=new ArrayList();
            list_exprs.add(exprs.getTree());

            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:110:26: ( ( 'or' | '||' ) exprs+= andExpression )*
            loop10:
            do {
                int alt10=2;
                int LA10_0 = input.LA(1);

                if ( ((LA10_0>=37 && LA10_0<=38)) ) {
                    alt10=1;
                }


                switch (alt10) {
            	case 1 :
            	    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:110:27: ( 'or' | '||' ) exprs+= andExpression
            	    {
            	    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:110:27: ( 'or' | '||' )
            	    int alt9=2;
            	    int LA9_0 = input.LA(1);

            	    if ( (LA9_0==37) ) {
            	        alt9=1;
            	    }
            	    else if ( (LA9_0==38) ) {
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
            	            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:110:28: 'or'
            	            {
            	            string_literal27=(Token)match(input,37,FOLLOW_37_in_orExpression456); if (state.failed) return retval; 
            	            if ( state.backtracking==0 ) stream_37.add(string_literal27);


            	            }
            	            break;
            	        case 2 :
            	            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:110:35: '||'
            	            {
            	            string_literal28=(Token)match(input,38,FOLLOW_38_in_orExpression460); if (state.failed) return retval; 
            	            if ( state.backtracking==0 ) stream_38.add(string_literal28);


            	            }
            	            break;

            	    }

            	    pushFollow(FOLLOW_andExpression_in_orExpression465);
            	    exprs=andExpression();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_andExpression.add(exprs.getTree());
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
            // 111:3: -> { $exprs.size() == 1 }?
            if ( list_exprs.size() == 1 ) {
                adaptor.addChild(root_0,  list_exprs.get(0) );

            }
            else // 112:3: ->
            {
                adaptor.addChild(root_0,  OrExpression.valueOf(list_exprs) );

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
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:114:1: andExpression : exprs+= elementExpression ( ( 'and' | '&&' ) exprs+= elementExpression )* -> { $exprs.size() == 1 }? ->;
    public final MeteorParser.andExpression_return andExpression() throws RecognitionException {
        MeteorParser.andExpression_return retval = new MeteorParser.andExpression_return();
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
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:115:3: (exprs+= elementExpression ( ( 'and' | '&&' ) exprs+= elementExpression )* -> { $exprs.size() == 1 }? ->)
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:115:5: exprs+= elementExpression ( ( 'and' | '&&' ) exprs+= elementExpression )*
            {
            pushFollow(FOLLOW_elementExpression_in_andExpression494);
            exprs=elementExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_elementExpression.add(exprs.getTree());
            if (list_exprs==null) list_exprs=new ArrayList();
            list_exprs.add(exprs.getTree());

            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:115:30: ( ( 'and' | '&&' ) exprs+= elementExpression )*
            loop12:
            do {
                int alt12=2;
                int LA12_0 = input.LA(1);

                if ( ((LA12_0>=39 && LA12_0<=40)) ) {
                    alt12=1;
                }


                switch (alt12) {
            	case 1 :
            	    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:115:31: ( 'and' | '&&' ) exprs+= elementExpression
            	    {
            	    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:115:31: ( 'and' | '&&' )
            	    int alt11=2;
            	    int LA11_0 = input.LA(1);

            	    if ( (LA11_0==39) ) {
            	        alt11=1;
            	    }
            	    else if ( (LA11_0==40) ) {
            	        alt11=2;
            	    }
            	    else {
            	        if (state.backtracking>0) {state.failed=true; return retval;}
            	        NoViableAltException nvae =
            	            new NoViableAltException("", 11, 0, input);

            	        throw nvae;
            	    }
            	    switch (alt11) {
            	        case 1 :
            	            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:115:32: 'and'
            	            {
            	            string_literal29=(Token)match(input,39,FOLLOW_39_in_andExpression498); if (state.failed) return retval; 
            	            if ( state.backtracking==0 ) stream_39.add(string_literal29);


            	            }
            	            break;
            	        case 2 :
            	            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:115:40: '&&'
            	            {
            	            string_literal30=(Token)match(input,40,FOLLOW_40_in_andExpression502); if (state.failed) return retval; 
            	            if ( state.backtracking==0 ) stream_40.add(string_literal30);


            	            }
            	            break;

            	    }

            	    pushFollow(FOLLOW_elementExpression_in_andExpression507);
            	    exprs=elementExpression();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_elementExpression.add(exprs.getTree());
            	    if (list_exprs==null) list_exprs=new ArrayList();
            	    list_exprs.add(exprs.getTree());


            	    }
            	    break;

            	default :
            	    break loop12;
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
            // 116:3: -> { $exprs.size() == 1 }?
            if ( list_exprs.size() == 1 ) {
                adaptor.addChild(root_0,  list_exprs.get(0) );

            }
            else // 117:3: ->
            {
                adaptor.addChild(root_0,  AndExpression.valueOf(list_exprs) );

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
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:119:1: elementExpression : elem= comparisonExpression ( (not= 'not' )? 'in' set= comparisonExpression )? -> { set == null }? $elem -> ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set) ;
    public final MeteorParser.elementExpression_return elementExpression() throws RecognitionException {
        MeteorParser.elementExpression_return retval = new MeteorParser.elementExpression_return();
        retval.start = input.LT(1);
        int elementExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token not=null;
        Token string_literal31=null;
        MeteorParser.comparisonExpression_return elem = null;

        MeteorParser.comparisonExpression_return set = null;


        EvaluationExpression not_tree=null;
        EvaluationExpression string_literal31_tree=null;
        RewriteRuleTokenStream stream_42=new RewriteRuleTokenStream(adaptor,"token 42");
        RewriteRuleTokenStream stream_41=new RewriteRuleTokenStream(adaptor,"token 41");
        RewriteRuleSubtreeStream stream_comparisonExpression=new RewriteRuleSubtreeStream(adaptor,"rule comparisonExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 12) ) { return retval; }
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:120:2: (elem= comparisonExpression ( (not= 'not' )? 'in' set= comparisonExpression )? -> { set == null }? $elem -> ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set) )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:120:4: elem= comparisonExpression ( (not= 'not' )? 'in' set= comparisonExpression )?
            {
            pushFollow(FOLLOW_comparisonExpression_in_elementExpression536);
            elem=comparisonExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_comparisonExpression.add(elem.getTree());
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:120:30: ( (not= 'not' )? 'in' set= comparisonExpression )?
            int alt14=2;
            int LA14_0 = input.LA(1);

            if ( ((LA14_0>=41 && LA14_0<=42)) ) {
                alt14=1;
            }
            switch (alt14) {
                case 1 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:120:31: (not= 'not' )? 'in' set= comparisonExpression
                    {
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:120:34: (not= 'not' )?
                    int alt13=2;
                    int LA13_0 = input.LA(1);

                    if ( (LA13_0==41) ) {
                        alt13=1;
                    }
                    switch (alt13) {
                        case 1 :
                            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:0:0: not= 'not'
                            {
                            not=(Token)match(input,41,FOLLOW_41_in_elementExpression541); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_41.add(not);


                            }
                            break;

                    }

                    string_literal31=(Token)match(input,42,FOLLOW_42_in_elementExpression544); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_42.add(string_literal31);

                    pushFollow(FOLLOW_comparisonExpression_in_elementExpression548);
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
            // 121:2: -> { set == null }? $elem
            if ( set == null ) {
                adaptor.addChild(root_0, stream_elem.nextTree());

            }
            else // 122:2: -> ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set)
            {
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:122:5: ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set)
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
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:125:1: comparisonExpression : e1= arithmeticExpression ( (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression )? -> { $s == null }? $e1 -> { $s.getText().equals(\"!=\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) -> { $s.getText().equals(\"==\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) -> ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) ;
    public final MeteorParser.comparisonExpression_return comparisonExpression() throws RecognitionException {
        MeteorParser.comparisonExpression_return retval = new MeteorParser.comparisonExpression_return();
        retval.start = input.LT(1);
        int comparisonExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token s=null;
        MeteorParser.arithmeticExpression_return e1 = null;

        MeteorParser.arithmeticExpression_return e2 = null;


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
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:126:2: (e1= arithmeticExpression ( (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression )? -> { $s == null }? $e1 -> { $s.getText().equals(\"!=\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) -> { $s.getText().equals(\"==\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) -> ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:126:4: e1= arithmeticExpression ( (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression )?
            {
            pushFollow(FOLLOW_arithmeticExpression_in_comparisonExpression589);
            e1=arithmeticExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_arithmeticExpression.add(e1.getTree());
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:126:28: ( (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression )?
            int alt16=2;
            int LA16_0 = input.LA(1);

            if ( ((LA16_0>=43 && LA16_0<=48)) ) {
                alt16=1;
            }
            switch (alt16) {
                case 1 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:126:29: (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression
                    {
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:126:29: (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' )
                    int alt15=6;
                    switch ( input.LA(1) ) {
                    case 43:
                        {
                        alt15=1;
                        }
                        break;
                    case 44:
                        {
                        alt15=2;
                        }
                        break;
                    case 45:
                        {
                        alt15=3;
                        }
                        break;
                    case 46:
                        {
                        alt15=4;
                        }
                        break;
                    case 47:
                        {
                        alt15=5;
                        }
                        break;
                    case 48:
                        {
                        alt15=6;
                        }
                        break;
                    default:
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 15, 0, input);

                        throw nvae;
                    }

                    switch (alt15) {
                        case 1 :
                            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:126:30: s= '<='
                            {
                            s=(Token)match(input,43,FOLLOW_43_in_comparisonExpression595); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_43.add(s);


                            }
                            break;
                        case 2 :
                            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:126:39: s= '>='
                            {
                            s=(Token)match(input,44,FOLLOW_44_in_comparisonExpression601); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_44.add(s);


                            }
                            break;
                        case 3 :
                            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:126:48: s= '<'
                            {
                            s=(Token)match(input,45,FOLLOW_45_in_comparisonExpression607); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_45.add(s);


                            }
                            break;
                        case 4 :
                            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:126:56: s= '>'
                            {
                            s=(Token)match(input,46,FOLLOW_46_in_comparisonExpression613); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_46.add(s);


                            }
                            break;
                        case 5 :
                            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:126:64: s= '=='
                            {
                            s=(Token)match(input,47,FOLLOW_47_in_comparisonExpression619); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_47.add(s);


                            }
                            break;
                        case 6 :
                            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:126:73: s= '!='
                            {
                            s=(Token)match(input,48,FOLLOW_48_in_comparisonExpression625); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_48.add(s);


                            }
                            break;

                    }

                    pushFollow(FOLLOW_arithmeticExpression_in_comparisonExpression630);
                    e2=arithmeticExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_arithmeticExpression.add(e2.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: e1, e1, e1, e2, e1, e2, e2
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
            // 127:2: -> { $s == null }? $e1
            if ( s == null ) {
                adaptor.addChild(root_0, stream_e1.nextTree());

            }
            else // 128:3: -> { $s.getText().equals(\"!=\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
            if ( s.getText().equals("!=") ) {
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:128:38: ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ComparativeExpression"), root_1);

                adaptor.addChild(root_1, stream_e1.nextTree());
                adaptor.addChild(root_1, ComparativeExpression.BinaryOperator.NOT_EQUAL);
                adaptor.addChild(root_1, stream_e2.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }
            else // 129:3: -> { $s.getText().equals(\"==\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
            if ( s.getText().equals("==") ) {
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:129:38: ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ComparativeExpression"), root_1);

                adaptor.addChild(root_1, stream_e1.nextTree());
                adaptor.addChild(root_1, ComparativeExpression.BinaryOperator.EQUAL);
                adaptor.addChild(root_1, stream_e2.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }
            else // 130:2: -> ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
            {
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:130:6: ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
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
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:132:1: arithmeticExpression : e1= multiplicationExpression ( (s= '+' | s= '-' ) e2= multiplicationExpression )? -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2) -> $e1;
    public final MeteorParser.arithmeticExpression_return arithmeticExpression() throws RecognitionException {
        MeteorParser.arithmeticExpression_return retval = new MeteorParser.arithmeticExpression_return();
        retval.start = input.LT(1);
        int arithmeticExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token s=null;
        MeteorParser.multiplicationExpression_return e1 = null;

        MeteorParser.multiplicationExpression_return e2 = null;


        EvaluationExpression s_tree=null;
        RewriteRuleTokenStream stream_49=new RewriteRuleTokenStream(adaptor,"token 49");
        RewriteRuleTokenStream stream_50=new RewriteRuleTokenStream(adaptor,"token 50");
        RewriteRuleSubtreeStream stream_multiplicationExpression=new RewriteRuleSubtreeStream(adaptor,"rule multiplicationExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 14) ) { return retval; }
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:133:2: (e1= multiplicationExpression ( (s= '+' | s= '-' ) e2= multiplicationExpression )? -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2) -> $e1)
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:133:4: e1= multiplicationExpression ( (s= '+' | s= '-' ) e2= multiplicationExpression )?
            {
            pushFollow(FOLLOW_multiplicationExpression_in_arithmeticExpression710);
            e1=multiplicationExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_multiplicationExpression.add(e1.getTree());
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:133:32: ( (s= '+' | s= '-' ) e2= multiplicationExpression )?
            int alt18=2;
            int LA18_0 = input.LA(1);

            if ( ((LA18_0>=49 && LA18_0<=50)) ) {
                alt18=1;
            }
            switch (alt18) {
                case 1 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:133:33: (s= '+' | s= '-' ) e2= multiplicationExpression
                    {
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:133:33: (s= '+' | s= '-' )
                    int alt17=2;
                    int LA17_0 = input.LA(1);

                    if ( (LA17_0==49) ) {
                        alt17=1;
                    }
                    else if ( (LA17_0==50) ) {
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
                            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:133:34: s= '+'
                            {
                            s=(Token)match(input,49,FOLLOW_49_in_arithmeticExpression716); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_49.add(s);


                            }
                            break;
                        case 2 :
                            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:133:42: s= '-'
                            {
                            s=(Token)match(input,50,FOLLOW_50_in_arithmeticExpression722); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_50.add(s);


                            }
                            break;

                    }

                    pushFollow(FOLLOW_multiplicationExpression_in_arithmeticExpression727);
                    e2=multiplicationExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_multiplicationExpression.add(e2.getTree());

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
            // 134:2: -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2)
            if ( s != null ) {
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:134:21: ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ArithmeticExpression"), root_1);

                adaptor.addChild(root_1, stream_e1.nextTree());
                adaptor.addChild(root_1,  s.getText().equals("+") ? ArithmeticExpression.ArithmeticOperator.ADDITION : ArithmeticExpression.ArithmeticOperator.SUBTRACTION);
                adaptor.addChild(root_1, stream_e2.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }
            else // 136:2: -> $e1
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
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:138:1: multiplicationExpression : e1= preincrementExpression ( (s= '*' | s= '/' ) e2= preincrementExpression )? -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2) -> $e1;
    public final MeteorParser.multiplicationExpression_return multiplicationExpression() throws RecognitionException {
        MeteorParser.multiplicationExpression_return retval = new MeteorParser.multiplicationExpression_return();
        retval.start = input.LT(1);
        int multiplicationExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token s=null;
        MeteorParser.preincrementExpression_return e1 = null;

        MeteorParser.preincrementExpression_return e2 = null;


        EvaluationExpression s_tree=null;
        RewriteRuleTokenStream stream_STAR=new RewriteRuleTokenStream(adaptor,"token STAR");
        RewriteRuleTokenStream stream_51=new RewriteRuleTokenStream(adaptor,"token 51");
        RewriteRuleSubtreeStream stream_preincrementExpression=new RewriteRuleSubtreeStream(adaptor,"rule preincrementExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 15) ) { return retval; }
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:139:2: (e1= preincrementExpression ( (s= '*' | s= '/' ) e2= preincrementExpression )? -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2) -> $e1)
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:139:4: e1= preincrementExpression ( (s= '*' | s= '/' ) e2= preincrementExpression )?
            {
            pushFollow(FOLLOW_preincrementExpression_in_multiplicationExpression770);
            e1=preincrementExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_preincrementExpression.add(e1.getTree());
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:139:30: ( (s= '*' | s= '/' ) e2= preincrementExpression )?
            int alt20=2;
            int LA20_0 = input.LA(1);

            if ( (LA20_0==STAR||LA20_0==51) ) {
                alt20=1;
            }
            switch (alt20) {
                case 1 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:139:31: (s= '*' | s= '/' ) e2= preincrementExpression
                    {
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:139:31: (s= '*' | s= '/' )
                    int alt19=2;
                    int LA19_0 = input.LA(1);

                    if ( (LA19_0==STAR) ) {
                        alt19=1;
                    }
                    else if ( (LA19_0==51) ) {
                        alt19=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 19, 0, input);

                        throw nvae;
                    }
                    switch (alt19) {
                        case 1 :
                            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:139:32: s= '*'
                            {
                            s=(Token)match(input,STAR,FOLLOW_STAR_in_multiplicationExpression776); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_STAR.add(s);


                            }
                            break;
                        case 2 :
                            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:139:40: s= '/'
                            {
                            s=(Token)match(input,51,FOLLOW_51_in_multiplicationExpression782); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_51.add(s);


                            }
                            break;

                    }

                    pushFollow(FOLLOW_preincrementExpression_in_multiplicationExpression787);
                    e2=preincrementExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_preincrementExpression.add(e2.getTree());

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
            // 140:2: -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2)
            if ( s != null ) {
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:140:21: ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ArithmeticExpression"), root_1);

                adaptor.addChild(root_1, stream_e1.nextTree());
                adaptor.addChild(root_1,  s.getText().equals("*") ? ArithmeticExpression.ArithmeticOperator.MULTIPLICATION : ArithmeticExpression.ArithmeticOperator.DIVISION);
                adaptor.addChild(root_1, stream_e2.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }
            else // 142:2: -> $e1
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
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:144:1: preincrementExpression : ( '++' preincrementExpression | '--' preincrementExpression | unaryExpression );
    public final MeteorParser.preincrementExpression_return preincrementExpression() throws RecognitionException {
        MeteorParser.preincrementExpression_return retval = new MeteorParser.preincrementExpression_return();
        retval.start = input.LT(1);
        int preincrementExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token string_literal32=null;
        Token string_literal34=null;
        MeteorParser.preincrementExpression_return preincrementExpression33 = null;

        MeteorParser.preincrementExpression_return preincrementExpression35 = null;

        MeteorParser.unaryExpression_return unaryExpression36 = null;


        EvaluationExpression string_literal32_tree=null;
        EvaluationExpression string_literal34_tree=null;

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 16) ) { return retval; }
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:145:2: ( '++' preincrementExpression | '--' preincrementExpression | unaryExpression )
            int alt21=3;
            switch ( input.LA(1) ) {
            case 52:
                {
                alt21=1;
                }
                break;
            case 53:
                {
                alt21=2;
                }
                break;
            case ID:
            case VAR:
            case STRING:
            case DECIMAL:
            case INTEGER:
            case 31:
            case 54:
            case 55:
            case 58:
            case 60:
            case 61:
            case 62:
            case 63:
                {
                alt21=3;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 21, 0, input);

                throw nvae;
            }

            switch (alt21) {
                case 1 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:145:4: '++' preincrementExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    string_literal32=(Token)match(input,52,FOLLOW_52_in_preincrementExpression828); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    string_literal32_tree = (EvaluationExpression)adaptor.create(string_literal32);
                    adaptor.addChild(root_0, string_literal32_tree);
                    }
                    pushFollow(FOLLOW_preincrementExpression_in_preincrementExpression830);
                    preincrementExpression33=preincrementExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, preincrementExpression33.getTree());

                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:146:4: '--' preincrementExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    string_literal34=(Token)match(input,53,FOLLOW_53_in_preincrementExpression835); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    string_literal34_tree = (EvaluationExpression)adaptor.create(string_literal34);
                    adaptor.addChild(root_0, string_literal34_tree);
                    }
                    pushFollow(FOLLOW_preincrementExpression_in_preincrementExpression837);
                    preincrementExpression35=preincrementExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, preincrementExpression35.getTree());

                    }
                    break;
                case 3 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:147:4: unaryExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_unaryExpression_in_preincrementExpression842);
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
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:149:1: unaryExpression : ( '!' | '~' )? castExpression ;
    public final MeteorParser.unaryExpression_return unaryExpression() throws RecognitionException {
        MeteorParser.unaryExpression_return retval = new MeteorParser.unaryExpression_return();
        retval.start = input.LT(1);
        int unaryExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token set37=null;
        MeteorParser.castExpression_return castExpression38 = null;


        EvaluationExpression set37_tree=null;

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 17) ) { return retval; }
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:150:2: ( ( '!' | '~' )? castExpression )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:150:4: ( '!' | '~' )? castExpression
            {
            root_0 = (EvaluationExpression)adaptor.nil();

            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:150:4: ( '!' | '~' )?
            int alt22=2;
            int LA22_0 = input.LA(1);

            if ( ((LA22_0>=54 && LA22_0<=55)) ) {
                alt22=1;
            }
            switch (alt22) {
                case 1 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:
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

            pushFollow(FOLLOW_castExpression_in_unaryExpression861);
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
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:152:1: castExpression : ( '(' type= ID ')' expr= generalPathExpression | expr= generalPathExpression 'as' type= ID | expr= generalPathExpression ) -> { type != null }? -> $expr;
    public final MeteorParser.castExpression_return castExpression() throws RecognitionException {
        MeteorParser.castExpression_return retval = new MeteorParser.castExpression_return();
        retval.start = input.LT(1);
        int castExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token type=null;
        Token char_literal39=null;
        Token char_literal40=null;
        Token string_literal41=null;
        MeteorParser.generalPathExpression_return expr = null;


        EvaluationExpression type_tree=null;
        EvaluationExpression char_literal39_tree=null;
        EvaluationExpression char_literal40_tree=null;
        EvaluationExpression string_literal41_tree=null;
        RewriteRuleTokenStream stream_56=new RewriteRuleTokenStream(adaptor,"token 56");
        RewriteRuleTokenStream stream_32=new RewriteRuleTokenStream(adaptor,"token 32");
        RewriteRuleTokenStream stream_31=new RewriteRuleTokenStream(adaptor,"token 31");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_generalPathExpression=new RewriteRuleSubtreeStream(adaptor,"rule generalPathExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 18) ) { return retval; }
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:153:2: ( ( '(' type= ID ')' expr= generalPathExpression | expr= generalPathExpression 'as' type= ID | expr= generalPathExpression ) -> { type != null }? -> $expr)
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:153:4: ( '(' type= ID ')' expr= generalPathExpression | expr= generalPathExpression 'as' type= ID | expr= generalPathExpression )
            {
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:153:4: ( '(' type= ID ')' expr= generalPathExpression | expr= generalPathExpression 'as' type= ID | expr= generalPathExpression )
            int alt23=3;
            alt23 = dfa23.predict(input);
            switch (alt23) {
                case 1 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:153:5: '(' type= ID ')' expr= generalPathExpression
                    {
                    char_literal39=(Token)match(input,31,FOLLOW_31_in_castExpression871); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_31.add(char_literal39);

                    type=(Token)match(input,ID,FOLLOW_ID_in_castExpression875); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(type);

                    char_literal40=(Token)match(input,32,FOLLOW_32_in_castExpression877); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_32.add(char_literal40);

                    pushFollow(FOLLOW_generalPathExpression_in_castExpression881);
                    expr=generalPathExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_generalPathExpression.add(expr.getTree());

                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:154:4: expr= generalPathExpression 'as' type= ID
                    {
                    pushFollow(FOLLOW_generalPathExpression_in_castExpression888);
                    expr=generalPathExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_generalPathExpression.add(expr.getTree());
                    string_literal41=(Token)match(input,56,FOLLOW_56_in_castExpression890); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_56.add(string_literal41);

                    type=(Token)match(input,ID,FOLLOW_ID_in_castExpression894); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(type);


                    }
                    break;
                case 3 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:155:4: expr= generalPathExpression
                    {
                    pushFollow(FOLLOW_generalPathExpression_in_castExpression901);
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
            // 156:2: -> { type != null }?
            if ( type != null ) {
                adaptor.addChild(root_0,  coerce((type!=null?type.getText():null), (expr!=null?((EvaluationExpression)expr.tree):null)) );

            }
            else // 157:2: -> $expr
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
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:159:1: generalPathExpression : (value= valueExpression path= pathExpression -> | valueExpression );
    public final MeteorParser.generalPathExpression_return generalPathExpression() throws RecognitionException {
        MeteorParser.generalPathExpression_return retval = new MeteorParser.generalPathExpression_return();
        retval.start = input.LT(1);
        int generalPathExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        MeteorParser.valueExpression_return value = null;

        MeteorParser.pathExpression_return path = null;

        MeteorParser.valueExpression_return valueExpression42 = null;


        RewriteRuleSubtreeStream stream_valueExpression=new RewriteRuleSubtreeStream(adaptor,"rule valueExpression");
        RewriteRuleSubtreeStream stream_pathExpression=new RewriteRuleSubtreeStream(adaptor,"rule pathExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 19) ) { return retval; }
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:160:2: (value= valueExpression path= pathExpression -> | valueExpression )
            int alt24=2;
            alt24 = dfa24.predict(input);
            switch (alt24) {
                case 1 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:160:4: value= valueExpression path= pathExpression
                    {
                    pushFollow(FOLLOW_valueExpression_in_generalPathExpression928);
                    value=valueExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_valueExpression.add(value.getTree());
                    pushFollow(FOLLOW_pathExpression_in_generalPathExpression932);
                    path=pathExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_pathExpression.add(path.getTree());


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
                    // 160:46: ->
                    {
                        adaptor.addChild(root_0,  PathExpression.wrapIfNecessary((value!=null?((EvaluationExpression)value.tree):null), (path!=null?((EvaluationExpression)path.tree):null)) );

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:161:4: valueExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_valueExpression_in_generalPathExpression942);
                    valueExpression42=valueExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, valueExpression42.getTree());

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
            if ( state.backtracking>0 ) { memoize(input, 19, generalPathExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "generalPathExpression"

    public static class contextAwarePathExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "contextAwarePathExpression"
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:163:1: contextAwarePathExpression[EvaluationExpression context] : path= pathExpression ->;
    public final MeteorParser.contextAwarePathExpression_return contextAwarePathExpression(EvaluationExpression context) throws RecognitionException {
        MeteorParser.contextAwarePathExpression_return retval = new MeteorParser.contextAwarePathExpression_return();
        retval.start = input.LT(1);
        int contextAwarePathExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        MeteorParser.pathExpression_return path = null;


        RewriteRuleSubtreeStream stream_pathExpression=new RewriteRuleSubtreeStream(adaptor,"rule pathExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 20) ) { return retval; }
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:164:3: (path= pathExpression ->)
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:164:5: path= pathExpression
            {
            pushFollow(FOLLOW_pathExpression_in_contextAwarePathExpression955);
            path=pathExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_pathExpression.add(path.getTree());


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
            // 164:25: ->
            {
                adaptor.addChild(root_0,  PathExpression.wrapIfNecessary(context, (path!=null?((EvaluationExpression)path.tree):null)) );

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
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:166:1: pathExpression : ( ( '.' (field= ID ) ) | arrayAccess )+ ->;
    public final MeteorParser.pathExpression_return pathExpression() throws RecognitionException {
        pathExpression_stack.push(new pathExpression_scope());
        MeteorParser.pathExpression_return retval = new MeteorParser.pathExpression_return();
        retval.start = input.LT(1);
        int pathExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token field=null;
        Token char_literal43=null;
        MeteorParser.arrayAccess_return arrayAccess44 = null;


        EvaluationExpression field_tree=null;
        EvaluationExpression char_literal43_tree=null;
        RewriteRuleTokenStream stream_57=new RewriteRuleTokenStream(adaptor,"token 57");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_arrayAccess=new RewriteRuleSubtreeStream(adaptor,"rule arrayAccess");
         ((pathExpression_scope)pathExpression_stack.peek()).fragments = new ArrayList<EvaluationExpression>(); 
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 21) ) { return retval; }
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:169:3: ( ( ( '.' (field= ID ) ) | arrayAccess )+ ->)
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:170:5: ( ( '.' (field= ID ) ) | arrayAccess )+
            {
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:170:5: ( ( '.' (field= ID ) ) | arrayAccess )+
            int cnt25=0;
            loop25:
            do {
                int alt25=3;
                int LA25_0 = input.LA(1);

                if ( (LA25_0==57) ) {
                    int LA25_2 = input.LA(2);

                    if ( (synpred36_Meteor()) ) {
                        alt25=1;
                    }


                }
                else if ( (LA25_0==63) ) {
                    int LA25_3 = input.LA(2);

                    if ( (synpred37_Meteor()) ) {
                        alt25=2;
                    }


                }


                switch (alt25) {
            	case 1 :
            	    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:170:7: ( '.' (field= ID ) )
            	    {
            	    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:170:7: ( '.' (field= ID ) )
            	    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:170:8: '.' (field= ID )
            	    {
            	    char_literal43=(Token)match(input,57,FOLLOW_57_in_pathExpression988); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_57.add(char_literal43);

            	    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:170:12: (field= ID )
            	    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:170:13: field= ID
            	    {
            	    field=(Token)match(input,ID,FOLLOW_ID_in_pathExpression993); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_ID.add(field);

            	    if ( state.backtracking==0 ) {
            	       ((pathExpression_scope)pathExpression_stack.peek()).fragments.add(new ObjectAccess((field!=null?field.getText():null))); 
            	    }

            	    }


            	    }


            	    }
            	    break;
            	case 2 :
            	    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:171:11: arrayAccess
            	    {
            	    pushFollow(FOLLOW_arrayAccess_in_pathExpression1011);
            	    arrayAccess44=arrayAccess();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_arrayAccess.add(arrayAccess44.getTree());
            	    if ( state.backtracking==0 ) {
            	       ((pathExpression_scope)pathExpression_stack.peek()).fragments.add((arrayAccess44!=null?((EvaluationExpression)arrayAccess44.tree):null)); 
            	    }

            	    }
            	    break;

            	default :
            	    if ( cnt25 >= 1 ) break loop25;
            	    if (state.backtracking>0) {state.failed=true; return retval;}
                        EarlyExitException eee =
                            new EarlyExitException(25, input);
                        throw eee;
                }
                cnt25++;
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
            // 172:3: ->
            {
                adaptor.addChild(root_0,  PathExpression.wrapIfNecessary(((pathExpression_scope)pathExpression_stack.peek()).fragments) );

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
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:174:1: valueExpression : ( methodCall[null] | parenthesesExpression | literal | VAR -> | (packageName= ID ':' )? constant= ID {...}? => -> | streamIndexAccess | arrayCreation | objectCreation );
    public final MeteorParser.valueExpression_return valueExpression() throws RecognitionException {
        MeteorParser.valueExpression_return retval = new MeteorParser.valueExpression_return();
        retval.start = input.LT(1);
        int valueExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token packageName=null;
        Token constant=null;
        Token VAR48=null;
        Token char_literal49=null;
        MeteorParser.methodCall_return methodCall45 = null;

        MeteorParser.parenthesesExpression_return parenthesesExpression46 = null;

        MeteorParser.literal_return literal47 = null;

        MeteorParser.streamIndexAccess_return streamIndexAccess50 = null;

        MeteorParser.arrayCreation_return arrayCreation51 = null;

        MeteorParser.objectCreation_return objectCreation52 = null;


        EvaluationExpression packageName_tree=null;
        EvaluationExpression constant_tree=null;
        EvaluationExpression VAR48_tree=null;
        EvaluationExpression char_literal49_tree=null;
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_35=new RewriteRuleTokenStream(adaptor,"token 35");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 22) ) { return retval; }
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:175:2: ( methodCall[null] | parenthesesExpression | literal | VAR -> | (packageName= ID ':' )? constant= ID {...}? => -> | streamIndexAccess | arrayCreation | objectCreation )
            int alt27=8;
            alt27 = dfa27.predict(input);
            switch (alt27) {
                case 1 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:175:4: methodCall[null]
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_methodCall_in_valueExpression1032);
                    methodCall45=methodCall(null);

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, methodCall45.getTree());

                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:176:4: parenthesesExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_parenthesesExpression_in_valueExpression1038);
                    parenthesesExpression46=parenthesesExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, parenthesesExpression46.getTree());

                    }
                    break;
                case 3 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:177:4: literal
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_literal_in_valueExpression1044);
                    literal47=literal();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, literal47.getTree());

                    }
                    break;
                case 4 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:178:4: VAR
                    {
                    VAR48=(Token)match(input,VAR,FOLLOW_VAR_in_valueExpression1050); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_VAR.add(VAR48);



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
                    // 178:8: ->
                    {
                        adaptor.addChild(root_0,  makePath(VAR48) );

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 5 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:179:5: (packageName= ID ':' )? constant= ID {...}? =>
                    {
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:179:5: (packageName= ID ':' )?
                    int alt26=2;
                    int LA26_0 = input.LA(1);

                    if ( (LA26_0==ID) ) {
                        int LA26_1 = input.LA(2);

                        if ( (LA26_1==35) ) {
                            int LA26_2 = input.LA(3);

                            if ( (synpred42_Meteor()) ) {
                                alt26=1;
                            }
                        }
                    }
                    switch (alt26) {
                        case 1 :
                            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:179:6: packageName= ID ':'
                            {
                            packageName=(Token)match(input,ID,FOLLOW_ID_in_valueExpression1063); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_ID.add(packageName);

                            char_literal49=(Token)match(input,35,FOLLOW_35_in_valueExpression1065); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_35.add(char_literal49);


                            }
                            break;

                    }

                    constant=(Token)match(input,ID,FOLLOW_ID_in_valueExpression1071); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(constant);

                    if ( !(( getScope((packageName!=null?packageName.getText():null)).getConstantRegistry().get((constant!=null?constant.getText():null)) != null )) ) {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        throw new FailedPredicateException(input, "valueExpression", " getScope($packageName.text).getConstantRegistry().get($constant.text) != null ");
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
                    // 180:5: ->
                    {
                        adaptor.addChild(root_0,  getScope((packageName!=null?packageName.getText():null)).getConstantRegistry().get((constant!=null?constant.getText():null)) );

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 6 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:181:5: streamIndexAccess
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_streamIndexAccess_in_valueExpression1089);
                    streamIndexAccess50=streamIndexAccess();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, streamIndexAccess50.getTree());

                    }
                    break;
                case 7 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:182:4: arrayCreation
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_arrayCreation_in_valueExpression1094);
                    arrayCreation51=arrayCreation();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, arrayCreation51.getTree());

                    }
                    break;
                case 8 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:183:4: objectCreation
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_objectCreation_in_valueExpression1100);
                    objectCreation52=objectCreation();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, objectCreation52.getTree());

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
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:185:1: operatorExpression : op= operator -> ^( EXPRESSION[\"NestedOperatorExpression\"] ) ;
    public final MeteorParser.operatorExpression_return operatorExpression() throws RecognitionException {
        MeteorParser.operatorExpression_return retval = new MeteorParser.operatorExpression_return();
        retval.start = input.LT(1);
        int operatorExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        MeteorParser.operator_return op = null;


        RewriteRuleSubtreeStream stream_operator=new RewriteRuleSubtreeStream(adaptor,"rule operator");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 23) ) { return retval; }
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:186:2: (op= operator -> ^( EXPRESSION[\"NestedOperatorExpression\"] ) )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:186:4: op= operator
            {
            pushFollow(FOLLOW_operator_in_operatorExpression1113);
            op=operator();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_operator.add(op.getTree());


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
            // 186:16: -> ^( EXPRESSION[\"NestedOperatorExpression\"] )
            {
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:186:19: ^( EXPRESSION[\"NestedOperatorExpression\"] )
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "NestedOperatorExpression"), root_1);

                adaptor.addChild(root_1,  (op!=null?op.op:null) );

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
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:188:1: parenthesesExpression : ( '(' expression ')' ) -> expression ;
    public final MeteorParser.parenthesesExpression_return parenthesesExpression() throws RecognitionException {
        MeteorParser.parenthesesExpression_return retval = new MeteorParser.parenthesesExpression_return();
        retval.start = input.LT(1);
        int parenthesesExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token char_literal53=null;
        Token char_literal55=null;
        MeteorParser.expression_return expression54 = null;


        EvaluationExpression char_literal53_tree=null;
        EvaluationExpression char_literal55_tree=null;
        RewriteRuleTokenStream stream_32=new RewriteRuleTokenStream(adaptor,"token 32");
        RewriteRuleTokenStream stream_31=new RewriteRuleTokenStream(adaptor,"token 31");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 24) ) { return retval; }
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:189:2: ( ( '(' expression ')' ) -> expression )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:189:4: ( '(' expression ')' )
            {
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:189:4: ( '(' expression ')' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:189:5: '(' expression ')'
            {
            char_literal53=(Token)match(input,31,FOLLOW_31_in_parenthesesExpression1134); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_31.add(char_literal53);

            pushFollow(FOLLOW_expression_in_parenthesesExpression1136);
            expression54=expression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_expression.add(expression54.getTree());
            char_literal55=(Token)match(input,32,FOLLOW_32_in_parenthesesExpression1138); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_32.add(char_literal55);


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
            // 189:25: -> expression
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
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:191:1: methodCall[EvaluationExpression targetExpr] : (packageName= ID ':' )? name= ID '(' (param= expression ( ',' param= expression )* )? ')' ->;
    public final MeteorParser.methodCall_return methodCall(EvaluationExpression targetExpr) throws RecognitionException {
        MeteorParser.methodCall_return retval = new MeteorParser.methodCall_return();
        retval.start = input.LT(1);
        int methodCall_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token packageName=null;
        Token name=null;
        Token char_literal56=null;
        Token char_literal57=null;
        Token char_literal58=null;
        Token char_literal59=null;
        MeteorParser.expression_return param = null;


        EvaluationExpression packageName_tree=null;
        EvaluationExpression name_tree=null;
        EvaluationExpression char_literal56_tree=null;
        EvaluationExpression char_literal57_tree=null;
        EvaluationExpression char_literal58_tree=null;
        EvaluationExpression char_literal59_tree=null;
        RewriteRuleTokenStream stream_32=new RewriteRuleTokenStream(adaptor,"token 32");
        RewriteRuleTokenStream stream_31=new RewriteRuleTokenStream(adaptor,"token 31");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_35=new RewriteRuleTokenStream(adaptor,"token 35");
        RewriteRuleTokenStream stream_28=new RewriteRuleTokenStream(adaptor,"token 28");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
         List<EvaluationExpression> params = new ArrayList(); 
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 25) ) { return retval; }
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:193:2: ( (packageName= ID ':' )? name= ID '(' (param= expression ( ',' param= expression )* )? ')' ->)
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:193:4: (packageName= ID ':' )? name= ID '(' (param= expression ( ',' param= expression )* )? ')'
            {
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:193:4: (packageName= ID ':' )?
            int alt28=2;
            int LA28_0 = input.LA(1);

            if ( (LA28_0==ID) ) {
                int LA28_1 = input.LA(2);

                if ( (LA28_1==35) ) {
                    alt28=1;
                }
            }
            switch (alt28) {
                case 1 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:193:5: packageName= ID ':'
                    {
                    packageName=(Token)match(input,ID,FOLLOW_ID_in_methodCall1162); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(packageName);

                    char_literal56=(Token)match(input,35,FOLLOW_35_in_methodCall1164); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_35.add(char_literal56);


                    }
                    break;

            }

            name=(Token)match(input,ID,FOLLOW_ID_in_methodCall1170); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);

            char_literal57=(Token)match(input,31,FOLLOW_31_in_methodCall1172); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_31.add(char_literal57);

            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:194:2: (param= expression ( ',' param= expression )* )?
            int alt30=2;
            int LA30_0 = input.LA(1);

            if ( ((LA30_0>=ID && LA30_0<=STRING)||(LA30_0>=DECIMAL && LA30_0<=INTEGER)||LA30_0==31||(LA30_0>=52 && LA30_0<=55)||LA30_0==58||(LA30_0>=60 && LA30_0<=63)||LA30_0==65||LA30_0==67) ) {
                alt30=1;
            }
            switch (alt30) {
                case 1 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:194:3: param= expression ( ',' param= expression )*
                    {
                    pushFollow(FOLLOW_expression_in_methodCall1179);
                    param=expression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_expression.add(param.getTree());
                    if ( state.backtracking==0 ) {
                       params.add((param!=null?((EvaluationExpression)param.tree):null)); 
                    }
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:195:2: ( ',' param= expression )*
                    loop29:
                    do {
                        int alt29=2;
                        int LA29_0 = input.LA(1);

                        if ( (LA29_0==28) ) {
                            alt29=1;
                        }


                        switch (alt29) {
                    	case 1 :
                    	    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:195:3: ',' param= expression
                    	    {
                    	    char_literal58=(Token)match(input,28,FOLLOW_28_in_methodCall1185); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_28.add(char_literal58);

                    	    pushFollow(FOLLOW_expression_in_methodCall1189);
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
                    	    break loop29;
                        }
                    } while (true);


                    }
                    break;

            }

            char_literal59=(Token)match(input,32,FOLLOW_32_in_methodCall1199); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_32.add(char_literal59);



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
            // 196:6: ->
            {
                adaptor.addChild(root_0,  createCheckedMethodCall((packageName!=null?packageName.getText():null), name, targetExpr, params.toArray(new EvaluationExpression[params.size()])) );

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
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "fieldAssignment"
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:198:1: fieldAssignment : ( ID ':' expression -> | VAR ( '.' STAR -> | '=' op= operator {...}? => | p= contextAwarePathExpression[getVariable($VAR).toInputSelection($operator::result)] ( ':' e2= expression -> | ->) ) );
    public final MeteorParser.fieldAssignment_return fieldAssignment() throws RecognitionException {
        MeteorParser.fieldAssignment_return retval = new MeteorParser.fieldAssignment_return();
        retval.start = input.LT(1);
        int fieldAssignment_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token ID60=null;
        Token char_literal61=null;
        Token VAR63=null;
        Token char_literal64=null;
        Token STAR65=null;
        Token char_literal66=null;
        Token char_literal67=null;
        MeteorParser.operator_return op = null;

        MeteorParser.contextAwarePathExpression_return p = null;

        MeteorParser.expression_return e2 = null;

        MeteorParser.expression_return expression62 = null;


        EvaluationExpression ID60_tree=null;
        EvaluationExpression char_literal61_tree=null;
        EvaluationExpression VAR63_tree=null;
        EvaluationExpression char_literal64_tree=null;
        EvaluationExpression STAR65_tree=null;
        EvaluationExpression char_literal66_tree=null;
        EvaluationExpression char_literal67_tree=null;
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_STAR=new RewriteRuleTokenStream(adaptor,"token STAR");
        RewriteRuleTokenStream stream_57=new RewriteRuleTokenStream(adaptor,"token 57");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_35=new RewriteRuleTokenStream(adaptor,"token 35");
        RewriteRuleTokenStream stream_29=new RewriteRuleTokenStream(adaptor,"token 29");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
        RewriteRuleSubtreeStream stream_contextAwarePathExpression=new RewriteRuleSubtreeStream(adaptor,"rule contextAwarePathExpression");
        RewriteRuleSubtreeStream stream_operator=new RewriteRuleSubtreeStream(adaptor,"rule operator");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 26) ) { return retval; }
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:199:2: ( ID ':' expression -> | VAR ( '.' STAR -> | '=' op= operator {...}? => | p= contextAwarePathExpression[getVariable($VAR).toInputSelection($operator::result)] ( ':' e2= expression -> | ->) ) )
            int alt33=2;
            int LA33_0 = input.LA(1);

            if ( (LA33_0==ID) ) {
                alt33=1;
            }
            else if ( (LA33_0==VAR) ) {
                alt33=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 33, 0, input);

                throw nvae;
            }
            switch (alt33) {
                case 1 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:199:4: ID ':' expression
                    {
                    ID60=(Token)match(input,ID,FOLLOW_ID_in_fieldAssignment1213); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(ID60);

                    char_literal61=(Token)match(input,35,FOLLOW_35_in_fieldAssignment1215); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_35.add(char_literal61);

                    pushFollow(FOLLOW_expression_in_fieldAssignment1217);
                    expression62=expression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_expression.add(expression62.getTree());
                    if ( state.backtracking==0 ) {
                       ((objectCreation_scope)objectCreation_stack.peek()).mappings.add(new ObjectCreation.FieldAssignment((ID60!=null?ID60.getText():null), (expression62!=null?((EvaluationExpression)expression62.tree):null))); 
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
                    // 200:104: ->
                    {
                        root_0 = null;
                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:201:5: VAR ( '.' STAR -> | '=' op= operator {...}? => | p= contextAwarePathExpression[getVariable($VAR).toInputSelection($operator::result)] ( ':' e2= expression -> | ->) )
                    {
                    VAR63=(Token)match(input,VAR,FOLLOW_VAR_in_fieldAssignment1232); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_VAR.add(VAR63);

                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:202:5: ( '.' STAR -> | '=' op= operator {...}? => | p= contextAwarePathExpression[getVariable($VAR).toInputSelection($operator::result)] ( ':' e2= expression -> | ->) )
                    int alt32=3;
                    switch ( input.LA(1) ) {
                    case 57:
                        {
                        int LA32_1 = input.LA(2);

                        if ( (LA32_1==STAR) ) {
                            alt32=1;
                        }
                        else if ( (LA32_1==ID) ) {
                            alt32=3;
                        }
                        else {
                            if (state.backtracking>0) {state.failed=true; return retval;}
                            NoViableAltException nvae =
                                new NoViableAltException("", 32, 1, input);

                            throw nvae;
                        }
                        }
                        break;
                    case 29:
                        {
                        alt32=2;
                        }
                        break;
                    case 63:
                        {
                        alt32=3;
                        }
                        break;
                    default:
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 32, 0, input);

                        throw nvae;
                    }

                    switch (alt32) {
                        case 1 :
                            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:202:7: '.' STAR
                            {
                            char_literal64=(Token)match(input,57,FOLLOW_57_in_fieldAssignment1241); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_57.add(char_literal64);

                            STAR65=(Token)match(input,STAR,FOLLOW_STAR_in_fieldAssignment1243); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_STAR.add(STAR65);

                            if ( state.backtracking==0 ) {
                               ((objectCreation_scope)objectCreation_stack.peek()).mappings.add(new ObjectCreation.CopyFields(makePath(VAR63))); 
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
                            // 202:98: ->
                            {
                                root_0 = null;
                            }

                            retval.tree = root_0;}
                            }
                            break;
                        case 2 :
                            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:203:9: '=' op= operator {...}? =>
                            {
                            char_literal66=(Token)match(input,29,FOLLOW_29_in_fieldAssignment1257); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_29.add(char_literal66);

                            pushFollow(FOLLOW_operator_in_fieldAssignment1261);
                            op=operator();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_operator.add(op.getTree());
                            if ( !(( setInnerOutput(VAR63, (op!=null?op.op:null)) )) ) {
                                if (state.backtracking>0) {state.failed=true; return retval;}
                                throw new FailedPredicateException(input, "fieldAssignment", " setInnerOutput($VAR, $op.op) ");
                            }

                            }
                            break;
                        case 3 :
                            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:204:9: p= contextAwarePathExpression[getVariable($VAR).toInputSelection($operator::result)] ( ':' e2= expression -> | ->)
                            {
                            pushFollow(FOLLOW_contextAwarePathExpression_in_fieldAssignment1276);
                            p=contextAwarePathExpression(getVariable(VAR63).toInputSelection(((operator_scope)operator_stack.peek()).result));

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_contextAwarePathExpression.add(p.getTree());
                            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:205:7: ( ':' e2= expression -> | ->)
                            int alt31=2;
                            int LA31_0 = input.LA(1);

                            if ( (LA31_0==35) ) {
                                alt31=1;
                            }
                            else if ( (LA31_0==EOF||LA31_0==28||LA31_0==59) ) {
                                alt31=2;
                            }
                            else {
                                if (state.backtracking>0) {state.failed=true; return retval;}
                                NoViableAltException nvae =
                                    new NoViableAltException("", 31, 0, input);

                                throw nvae;
                            }
                            switch (alt31) {
                                case 1 :
                                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:205:9: ':' e2= expression
                                    {
                                    char_literal67=(Token)match(input,35,FOLLOW_35_in_fieldAssignment1287); if (state.failed) return retval; 
                                    if ( state.backtracking==0 ) stream_35.add(char_literal67);

                                    pushFollow(FOLLOW_expression_in_fieldAssignment1291);
                                    e2=expression();

                                    state._fsp--;
                                    if (state.failed) return retval;
                                    if ( state.backtracking==0 ) stream_expression.add(e2.getTree());
                                    if ( state.backtracking==0 ) {
                                       ((objectCreation_scope)objectCreation_stack.peek()).mappings.add(new ObjectCreation.TagMapping((p!=null?((EvaluationExpression)p.tree):null), (e2!=null?((EvaluationExpression)e2.tree):null))); 
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
                                    // 205:112: ->
                                    {
                                        root_0 = null;
                                    }

                                    retval.tree = root_0;}
                                    }
                                    break;
                                case 2 :
                                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:206:23: 
                                    {
                                    if ( state.backtracking==0 ) {
                                       ((objectCreation_scope)objectCreation_stack.peek()).mappings.add(new ObjectCreation.FieldAssignment(getAssignmentName((p!=null?((EvaluationExpression)p.tree):null)), (p!=null?((EvaluationExpression)p.tree):null))); 
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
                                    // 206:131: ->
                                    {
                                        root_0 = null;
                                    }

                                    retval.tree = root_0;}
                                    }
                                    break;

                            }


                            }
                            break;

                    }


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
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:210:1: objectCreation : '{' ( fieldAssignment ( ',' fieldAssignment )* ( ',' )? )? '}' -> ^( EXPRESSION[\"ObjectCreation\"] ) ;
    public final MeteorParser.objectCreation_return objectCreation() throws RecognitionException {
        objectCreation_stack.push(new objectCreation_scope());
        MeteorParser.objectCreation_return retval = new MeteorParser.objectCreation_return();
        retval.start = input.LT(1);
        int objectCreation_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token char_literal68=null;
        Token char_literal70=null;
        Token char_literal72=null;
        Token char_literal73=null;
        MeteorParser.fieldAssignment_return fieldAssignment69 = null;

        MeteorParser.fieldAssignment_return fieldAssignment71 = null;


        EvaluationExpression char_literal68_tree=null;
        EvaluationExpression char_literal70_tree=null;
        EvaluationExpression char_literal72_tree=null;
        EvaluationExpression char_literal73_tree=null;
        RewriteRuleTokenStream stream_59=new RewriteRuleTokenStream(adaptor,"token 59");
        RewriteRuleTokenStream stream_58=new RewriteRuleTokenStream(adaptor,"token 58");
        RewriteRuleTokenStream stream_28=new RewriteRuleTokenStream(adaptor,"token 28");
        RewriteRuleSubtreeStream stream_fieldAssignment=new RewriteRuleSubtreeStream(adaptor,"rule fieldAssignment");
         ((objectCreation_scope)objectCreation_stack.peek()).mappings = new ArrayList<ObjectCreation.Mapping>(); 
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 27) ) { return retval; }
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:213:2: ( '{' ( fieldAssignment ( ',' fieldAssignment )* ( ',' )? )? '}' -> ^( EXPRESSION[\"ObjectCreation\"] ) )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:213:4: '{' ( fieldAssignment ( ',' fieldAssignment )* ( ',' )? )? '}'
            {
            char_literal68=(Token)match(input,58,FOLLOW_58_in_objectCreation1343); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_58.add(char_literal68);

            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:213:8: ( fieldAssignment ( ',' fieldAssignment )* ( ',' )? )?
            int alt36=2;
            int LA36_0 = input.LA(1);

            if ( ((LA36_0>=ID && LA36_0<=VAR)) ) {
                alt36=1;
            }
            switch (alt36) {
                case 1 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:213:9: fieldAssignment ( ',' fieldAssignment )* ( ',' )?
                    {
                    pushFollow(FOLLOW_fieldAssignment_in_objectCreation1346);
                    fieldAssignment69=fieldAssignment();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_fieldAssignment.add(fieldAssignment69.getTree());
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:213:25: ( ',' fieldAssignment )*
                    loop34:
                    do {
                        int alt34=2;
                        int LA34_0 = input.LA(1);

                        if ( (LA34_0==28) ) {
                            int LA34_1 = input.LA(2);

                            if ( ((LA34_1>=ID && LA34_1<=VAR)) ) {
                                alt34=1;
                            }


                        }


                        switch (alt34) {
                    	case 1 :
                    	    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:213:26: ',' fieldAssignment
                    	    {
                    	    char_literal70=(Token)match(input,28,FOLLOW_28_in_objectCreation1349); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_28.add(char_literal70);

                    	    pushFollow(FOLLOW_fieldAssignment_in_objectCreation1351);
                    	    fieldAssignment71=fieldAssignment();

                    	    state._fsp--;
                    	    if (state.failed) return retval;
                    	    if ( state.backtracking==0 ) stream_fieldAssignment.add(fieldAssignment71.getTree());

                    	    }
                    	    break;

                    	default :
                    	    break loop34;
                        }
                    } while (true);

                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:213:48: ( ',' )?
                    int alt35=2;
                    int LA35_0 = input.LA(1);

                    if ( (LA35_0==28) ) {
                        alt35=1;
                    }
                    switch (alt35) {
                        case 1 :
                            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:0:0: ','
                            {
                            char_literal72=(Token)match(input,28,FOLLOW_28_in_objectCreation1355); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_28.add(char_literal72);


                            }
                            break;

                    }


                    }
                    break;

            }

            char_literal73=(Token)match(input,59,FOLLOW_59_in_objectCreation1360); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_59.add(char_literal73);



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
            // 213:59: -> ^( EXPRESSION[\"ObjectCreation\"] )
            {
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:213:62: ^( EXPRESSION[\"ObjectCreation\"] )
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
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:215:1: literal : (val= 'true' -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= 'false' -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= DECIMAL -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= STRING -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= INTEGER -> ^( EXPRESSION[\"ConstantExpression\"] ) | 'null' ->);
    public final MeteorParser.literal_return literal() throws RecognitionException {
        MeteorParser.literal_return retval = new MeteorParser.literal_return();
        retval.start = input.LT(1);
        int literal_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token val=null;
        Token string_literal74=null;

        EvaluationExpression val_tree=null;
        EvaluationExpression string_literal74_tree=null;
        RewriteRuleTokenStream stream_INTEGER=new RewriteRuleTokenStream(adaptor,"token INTEGER");
        RewriteRuleTokenStream stream_DECIMAL=new RewriteRuleTokenStream(adaptor,"token DECIMAL");
        RewriteRuleTokenStream stream_62=new RewriteRuleTokenStream(adaptor,"token 62");
        RewriteRuleTokenStream stream_60=new RewriteRuleTokenStream(adaptor,"token 60");
        RewriteRuleTokenStream stream_61=new RewriteRuleTokenStream(adaptor,"token 61");
        RewriteRuleTokenStream stream_STRING=new RewriteRuleTokenStream(adaptor,"token STRING");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 28) ) { return retval; }
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:216:2: (val= 'true' -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= 'false' -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= DECIMAL -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= STRING -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= INTEGER -> ^( EXPRESSION[\"ConstantExpression\"] ) | 'null' ->)
            int alt37=6;
            switch ( input.LA(1) ) {
            case 60:
                {
                alt37=1;
                }
                break;
            case 61:
                {
                alt37=2;
                }
                break;
            case DECIMAL:
                {
                alt37=3;
                }
                break;
            case STRING:
                {
                alt37=4;
                }
                break;
            case INTEGER:
                {
                alt37=5;
                }
                break;
            case 62:
                {
                alt37=6;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 37, 0, input);

                throw nvae;
            }

            switch (alt37) {
                case 1 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:216:4: val= 'true'
                    {
                    val=(Token)match(input,60,FOLLOW_60_in_literal1380); if (state.failed) return retval; 
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
                    // 216:15: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:216:18: ^( EXPRESSION[\"ConstantExpression\"] )
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
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:217:4: val= 'false'
                    {
                    val=(Token)match(input,61,FOLLOW_61_in_literal1396); if (state.failed) return retval; 
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
                    // 217:16: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:217:19: ^( EXPRESSION[\"ConstantExpression\"] )
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
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:218:4: val= DECIMAL
                    {
                    val=(Token)match(input,DECIMAL,FOLLOW_DECIMAL_in_literal1412); if (state.failed) return retval; 
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
                    // 218:16: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:218:19: ^( EXPRESSION[\"ConstantExpression\"] )
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
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:219:4: val= STRING
                    {
                    val=(Token)match(input,STRING,FOLLOW_STRING_in_literal1428); if (state.failed) return retval; 
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
                    // 219:15: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:219:18: ^( EXPRESSION[\"ConstantExpression\"] )
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
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:220:5: val= INTEGER
                    {
                    val=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_literal1445); if (state.failed) return retval; 
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
                    // 220:17: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:220:20: ^( EXPRESSION[\"ConstantExpression\"] )
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
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:221:5: 'null'
                    {
                    string_literal74=(Token)match(input,62,FOLLOW_62_in_literal1460); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_62.add(string_literal74);



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
                    // 221:12: ->
                    {
                        adaptor.addChild(root_0,  ConstantExpression.NULL );

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
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:223:1: arrayAccess : ( '[' STAR ']' path= pathExpression -> ^( EXPRESSION[\"ArrayProjection\"] $path) | '[' (pos= INTEGER | pos= UINT ) ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) | '[' (start= INTEGER | start= UINT ) ':' (end= INTEGER | end= UINT ) ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) );
    public final MeteorParser.arrayAccess_return arrayAccess() throws RecognitionException {
        MeteorParser.arrayAccess_return retval = new MeteorParser.arrayAccess_return();
        retval.start = input.LT(1);
        int arrayAccess_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token pos=null;
        Token start=null;
        Token end=null;
        Token char_literal75=null;
        Token STAR76=null;
        Token char_literal77=null;
        Token char_literal78=null;
        Token char_literal79=null;
        Token char_literal80=null;
        Token char_literal81=null;
        Token char_literal82=null;
        MeteorParser.pathExpression_return path = null;


        EvaluationExpression pos_tree=null;
        EvaluationExpression start_tree=null;
        EvaluationExpression end_tree=null;
        EvaluationExpression char_literal75_tree=null;
        EvaluationExpression STAR76_tree=null;
        EvaluationExpression char_literal77_tree=null;
        EvaluationExpression char_literal78_tree=null;
        EvaluationExpression char_literal79_tree=null;
        EvaluationExpression char_literal80_tree=null;
        EvaluationExpression char_literal81_tree=null;
        EvaluationExpression char_literal82_tree=null;
        RewriteRuleTokenStream stream_INTEGER=new RewriteRuleTokenStream(adaptor,"token INTEGER");
        RewriteRuleTokenStream stream_STAR=new RewriteRuleTokenStream(adaptor,"token STAR");
        RewriteRuleTokenStream stream_35=new RewriteRuleTokenStream(adaptor,"token 35");
        RewriteRuleTokenStream stream_64=new RewriteRuleTokenStream(adaptor,"token 64");
        RewriteRuleTokenStream stream_UINT=new RewriteRuleTokenStream(adaptor,"token UINT");
        RewriteRuleTokenStream stream_63=new RewriteRuleTokenStream(adaptor,"token 63");
        RewriteRuleSubtreeStream stream_pathExpression=new RewriteRuleSubtreeStream(adaptor,"rule pathExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 29) ) { return retval; }
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:224:3: ( '[' STAR ']' path= pathExpression -> ^( EXPRESSION[\"ArrayProjection\"] $path) | '[' (pos= INTEGER | pos= UINT ) ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) | '[' (start= INTEGER | start= UINT ) ':' (end= INTEGER | end= UINT ) ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) )
            int alt41=3;
            int LA41_0 = input.LA(1);

            if ( (LA41_0==63) ) {
                switch ( input.LA(2) ) {
                case STAR:
                    {
                    alt41=1;
                    }
                    break;
                case INTEGER:
                    {
                    int LA41_3 = input.LA(3);

                    if ( (LA41_3==64) ) {
                        alt41=2;
                    }
                    else if ( (LA41_3==35) ) {
                        alt41=3;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 41, 3, input);

                        throw nvae;
                    }
                    }
                    break;
                case UINT:
                    {
                    int LA41_4 = input.LA(3);

                    if ( (LA41_4==64) ) {
                        alt41=2;
                    }
                    else if ( (LA41_4==35) ) {
                        alt41=3;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 41, 4, input);

                        throw nvae;
                    }
                    }
                    break;
                default:
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 41, 1, input);

                    throw nvae;
                }

            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 41, 0, input);

                throw nvae;
            }
            switch (alt41) {
                case 1 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:224:5: '[' STAR ']' path= pathExpression
                    {
                    char_literal75=(Token)match(input,63,FOLLOW_63_in_arrayAccess1474); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_63.add(char_literal75);

                    STAR76=(Token)match(input,STAR,FOLLOW_STAR_in_arrayAccess1476); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STAR.add(STAR76);

                    char_literal77=(Token)match(input,64,FOLLOW_64_in_arrayAccess1478); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_64.add(char_literal77);

                    pushFollow(FOLLOW_pathExpression_in_arrayAccess1482);
                    path=pathExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_pathExpression.add(path.getTree());


                    // AST REWRITE
                    // elements: path
                    // token labels: 
                    // rule labels: retval, path
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
                    RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path",path!=null?path.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 225:3: -> ^( EXPRESSION[\"ArrayProjection\"] $path)
                    {
                        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:225:6: ^( EXPRESSION[\"ArrayProjection\"] $path)
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ArrayProjection"), root_1);

                        adaptor.addChild(root_1, stream_path.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:226:5: '[' (pos= INTEGER | pos= UINT ) ']'
                    {
                    char_literal78=(Token)match(input,63,FOLLOW_63_in_arrayAccess1502); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_63.add(char_literal78);

                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:226:9: (pos= INTEGER | pos= UINT )
                    int alt38=2;
                    int LA38_0 = input.LA(1);

                    if ( (LA38_0==INTEGER) ) {
                        alt38=1;
                    }
                    else if ( (LA38_0==UINT) ) {
                        alt38=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 38, 0, input);

                        throw nvae;
                    }
                    switch (alt38) {
                        case 1 :
                            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:226:10: pos= INTEGER
                            {
                            pos=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_arrayAccess1507); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_INTEGER.add(pos);


                            }
                            break;
                        case 2 :
                            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:226:24: pos= UINT
                            {
                            pos=(Token)match(input,UINT,FOLLOW_UINT_in_arrayAccess1513); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_UINT.add(pos);


                            }
                            break;

                    }

                    char_literal79=(Token)match(input,64,FOLLOW_64_in_arrayAccess1516); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_64.add(char_literal79);



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
                    // 227:3: -> ^( EXPRESSION[\"ArrayAccess\"] )
                    {
                        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:227:6: ^( EXPRESSION[\"ArrayAccess\"] )
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
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:228:5: '[' (start= INTEGER | start= UINT ) ':' (end= INTEGER | end= UINT ) ']'
                    {
                    char_literal80=(Token)match(input,63,FOLLOW_63_in_arrayAccess1534); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_63.add(char_literal80);

                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:228:9: (start= INTEGER | start= UINT )
                    int alt39=2;
                    int LA39_0 = input.LA(1);

                    if ( (LA39_0==INTEGER) ) {
                        alt39=1;
                    }
                    else if ( (LA39_0==UINT) ) {
                        alt39=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 39, 0, input);

                        throw nvae;
                    }
                    switch (alt39) {
                        case 1 :
                            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:228:10: start= INTEGER
                            {
                            start=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_arrayAccess1539); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_INTEGER.add(start);


                            }
                            break;
                        case 2 :
                            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:228:26: start= UINT
                            {
                            start=(Token)match(input,UINT,FOLLOW_UINT_in_arrayAccess1545); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_UINT.add(start);


                            }
                            break;

                    }

                    char_literal81=(Token)match(input,35,FOLLOW_35_in_arrayAccess1548); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_35.add(char_literal81);

                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:228:42: (end= INTEGER | end= UINT )
                    int alt40=2;
                    int LA40_0 = input.LA(1);

                    if ( (LA40_0==INTEGER) ) {
                        alt40=1;
                    }
                    else if ( (LA40_0==UINT) ) {
                        alt40=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 40, 0, input);

                        throw nvae;
                    }
                    switch (alt40) {
                        case 1 :
                            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:228:43: end= INTEGER
                            {
                            end=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_arrayAccess1553); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_INTEGER.add(end);


                            }
                            break;
                        case 2 :
                            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:228:57: end= UINT
                            {
                            end=(Token)match(input,UINT,FOLLOW_UINT_in_arrayAccess1559); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_UINT.add(end);


                            }
                            break;

                    }

                    char_literal82=(Token)match(input,64,FOLLOW_64_in_arrayAccess1562); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_64.add(char_literal82);



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
                    // 229:3: -> ^( EXPRESSION[\"ArrayAccess\"] )
                    {
                        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:229:6: ^( EXPRESSION[\"ArrayAccess\"] )
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

    public static class streamIndexAccess_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "streamIndexAccess"
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:231:1: streamIndexAccess : op= VAR {...}? => '[' path= generalPathExpression ']' {...}? ->;
    public final MeteorParser.streamIndexAccess_return streamIndexAccess() throws RecognitionException {
        MeteorParser.streamIndexAccess_return retval = new MeteorParser.streamIndexAccess_return();
        retval.start = input.LT(1);
        int streamIndexAccess_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token op=null;
        Token char_literal83=null;
        Token char_literal84=null;
        MeteorParser.generalPathExpression_return path = null;


        EvaluationExpression op_tree=null;
        EvaluationExpression char_literal83_tree=null;
        EvaluationExpression char_literal84_tree=null;
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_64=new RewriteRuleTokenStream(adaptor,"token 64");
        RewriteRuleTokenStream stream_63=new RewriteRuleTokenStream(adaptor,"token 63");
        RewriteRuleSubtreeStream stream_generalPathExpression=new RewriteRuleSubtreeStream(adaptor,"rule generalPathExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 30) ) { return retval; }
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:232:3: (op= VAR {...}? => '[' path= generalPathExpression ']' {...}? ->)
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:232:5: op= VAR {...}? => '[' path= generalPathExpression ']' {...}?
            {
            op=(Token)match(input,VAR,FOLLOW_VAR_in_streamIndexAccess1590); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_VAR.add(op);

            if ( !(( getVariable(op) != null )) ) {
                if (state.backtracking>0) {state.failed=true; return retval;}
                throw new FailedPredicateException(input, "streamIndexAccess", " getVariable($op) != null ");
            }
            char_literal83=(Token)match(input,63,FOLLOW_63_in_streamIndexAccess1599); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_63.add(char_literal83);

            pushFollow(FOLLOW_generalPathExpression_in_streamIndexAccess1603);
            path=generalPathExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_generalPathExpression.add(path.getTree());
            char_literal84=(Token)match(input,64,FOLLOW_64_in_streamIndexAccess1605); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_64.add(char_literal84);

            if ( !(( !((path!=null?((EvaluationExpression)path.tree):null) instanceof ConstantExpression) )) ) {
                if (state.backtracking>0) {state.failed=true; return retval;}
                throw new FailedPredicateException(input, "streamIndexAccess", " !($path.tree instanceof ConstantExpression) ");
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
            // 234:3: ->
            {
                adaptor.addChild(root_0,  new StreamIndexExpression(getVariable(op).getStream(), (path!=null?((EvaluationExpression)path.tree):null)) );

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
            if ( state.backtracking>0 ) { memoize(input, 30, streamIndexAccess_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "streamIndexAccess"

    public static class arrayCreation_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "arrayCreation"
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:236:1: arrayCreation : '[' elems+= expression ( ',' elems+= expression )* ( ',' )? ']' -> ^( EXPRESSION[\"ArrayCreation\"] ) ;
    public final MeteorParser.arrayCreation_return arrayCreation() throws RecognitionException {
        MeteorParser.arrayCreation_return retval = new MeteorParser.arrayCreation_return();
        retval.start = input.LT(1);
        int arrayCreation_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token char_literal85=null;
        Token char_literal86=null;
        Token char_literal87=null;
        Token char_literal88=null;
        List list_elems=null;
        RuleReturnScope elems = null;
        EvaluationExpression char_literal85_tree=null;
        EvaluationExpression char_literal86_tree=null;
        EvaluationExpression char_literal87_tree=null;
        EvaluationExpression char_literal88_tree=null;
        RewriteRuleTokenStream stream_64=new RewriteRuleTokenStream(adaptor,"token 64");
        RewriteRuleTokenStream stream_63=new RewriteRuleTokenStream(adaptor,"token 63");
        RewriteRuleTokenStream stream_28=new RewriteRuleTokenStream(adaptor,"token 28");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 31) ) { return retval; }
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:237:2: ( '[' elems+= expression ( ',' elems+= expression )* ( ',' )? ']' -> ^( EXPRESSION[\"ArrayCreation\"] ) )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:237:5: '[' elems+= expression ( ',' elems+= expression )* ( ',' )? ']'
            {
            char_literal85=(Token)match(input,63,FOLLOW_63_in_arrayCreation1624); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_63.add(char_literal85);

            pushFollow(FOLLOW_expression_in_arrayCreation1628);
            elems=expression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_expression.add(elems.getTree());
            if (list_elems==null) list_elems=new ArrayList();
            list_elems.add(elems.getTree());

            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:237:27: ( ',' elems+= expression )*
            loop42:
            do {
                int alt42=2;
                int LA42_0 = input.LA(1);

                if ( (LA42_0==28) ) {
                    int LA42_1 = input.LA(2);

                    if ( ((LA42_1>=ID && LA42_1<=STRING)||(LA42_1>=DECIMAL && LA42_1<=INTEGER)||LA42_1==31||(LA42_1>=52 && LA42_1<=55)||LA42_1==58||(LA42_1>=60 && LA42_1<=63)||LA42_1==65||LA42_1==67) ) {
                        alt42=1;
                    }


                }


                switch (alt42) {
            	case 1 :
            	    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:237:28: ',' elems+= expression
            	    {
            	    char_literal86=(Token)match(input,28,FOLLOW_28_in_arrayCreation1631); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_28.add(char_literal86);

            	    pushFollow(FOLLOW_expression_in_arrayCreation1635);
            	    elems=expression();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_expression.add(elems.getTree());
            	    if (list_elems==null) list_elems=new ArrayList();
            	    list_elems.add(elems.getTree());


            	    }
            	    break;

            	default :
            	    break loop42;
                }
            } while (true);

            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:237:52: ( ',' )?
            int alt43=2;
            int LA43_0 = input.LA(1);

            if ( (LA43_0==28) ) {
                alt43=1;
            }
            switch (alt43) {
                case 1 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:0:0: ','
                    {
                    char_literal87=(Token)match(input,28,FOLLOW_28_in_arrayCreation1639); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_28.add(char_literal87);


                    }
                    break;

            }

            char_literal88=(Token)match(input,64,FOLLOW_64_in_arrayCreation1642); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_64.add(char_literal88);



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
            // 237:61: -> ^( EXPRESSION[\"ArrayCreation\"] )
            {
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:237:64: ^( EXPRESSION[\"ArrayCreation\"] )
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
            if ( state.backtracking>0 ) { memoize(input, 31, arrayCreation_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "arrayCreation"

    protected static class operator_scope {
        Operator<?> result;
        int numInputs;
        Map<JsonStream, List<ExpressionTag>> inputTags;
    }
    protected Stack operator_stack = new Stack();

    public static class operator_return extends ParserRuleReturnScope {
        public Operator<?> op=null;
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "operator"
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:239:1: operator returns [Operator<?> op=null] : opRule= ( readOperator | writeOperator | genericOperator ) ;
    public final MeteorParser.operator_return operator() throws RecognitionException {
        operator_stack.push(new operator_scope());
        MeteorParser.operator_return retval = new MeteorParser.operator_return();
        retval.start = input.LT(1);
        int operator_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token opRule=null;
        MeteorParser.readOperator_return readOperator89 = null;

        MeteorParser.writeOperator_return writeOperator90 = null;

        MeteorParser.genericOperator_return genericOperator91 = null;


        EvaluationExpression opRule_tree=null;


          if(state.backtracking == 0) 
        	  addScope();
        	((operator_scope)operator_stack.peek()).inputTags = new IdentityHashMap<JsonStream, List<ExpressionTag>>();

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 32) ) { return retval; }
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:252:2: (opRule= ( readOperator | writeOperator | genericOperator ) )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:252:4: opRule= ( readOperator | writeOperator | genericOperator )
            {
            root_0 = (EvaluationExpression)adaptor.nil();

            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:252:11: ( readOperator | writeOperator | genericOperator )
            int alt44=3;
            switch ( input.LA(1) ) {
            case 65:
                {
                alt44=1;
                }
                break;
            case 67:
                {
                alt44=2;
                }
                break;
            case ID:
                {
                alt44=3;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 44, 0, input);

                throw nvae;
            }

            switch (alt44) {
                case 1 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:252:12: readOperator
                    {
                    pushFollow(FOLLOW_readOperator_in_operator1679);
                    readOperator89=readOperator();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, readOperator89.getTree());

                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:252:27: writeOperator
                    {
                    pushFollow(FOLLOW_writeOperator_in_operator1683);
                    writeOperator90=writeOperator();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, writeOperator90.getTree());

                    }
                    break;
                case 3 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:252:43: genericOperator
                    {
                    pushFollow(FOLLOW_genericOperator_in_operator1687);
                    genericOperator91=genericOperator();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, genericOperator91.getTree());

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
            if ( state.backtracking==0 ) {

                removeScope();

            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 32, operator_StartIndex); }
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
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:257:1: readOperator : 'read' 'from' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' ) ->;
    public final MeteorParser.readOperator_return readOperator() throws RecognitionException {
        MeteorParser.readOperator_return retval = new MeteorParser.readOperator_return();
        retval.start = input.LT(1);
        int readOperator_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token loc=null;
        Token file=null;
        Token string_literal92=null;
        Token string_literal93=null;
        Token char_literal94=null;
        Token char_literal95=null;

        EvaluationExpression loc_tree=null;
        EvaluationExpression file_tree=null;
        EvaluationExpression string_literal92_tree=null;
        EvaluationExpression string_literal93_tree=null;
        EvaluationExpression char_literal94_tree=null;
        EvaluationExpression char_literal95_tree=null;
        RewriteRuleTokenStream stream_66=new RewriteRuleTokenStream(adaptor,"token 66");
        RewriteRuleTokenStream stream_32=new RewriteRuleTokenStream(adaptor,"token 32");
        RewriteRuleTokenStream stream_31=new RewriteRuleTokenStream(adaptor,"token 31");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_65=new RewriteRuleTokenStream(adaptor,"token 65");
        RewriteRuleTokenStream stream_STRING=new RewriteRuleTokenStream(adaptor,"token STRING");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 33) ) { return retval; }
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:258:2: ( 'read' 'from' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' ) ->)
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:258:4: 'read' 'from' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' )
            {
            string_literal92=(Token)match(input,65,FOLLOW_65_in_readOperator1701); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_65.add(string_literal92);

            string_literal93=(Token)match(input,66,FOLLOW_66_in_readOperator1703); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_66.add(string_literal93);

            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:258:18: ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' )
            int alt46=2;
            int LA46_0 = input.LA(1);

            if ( (LA46_0==ID) ) {
                int LA46_1 = input.LA(2);

                if ( (LA46_1==31) ) {
                    alt46=2;
                }
                else if ( (LA46_1==STRING) ) {
                    alt46=1;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 46, 1, input);

                    throw nvae;
                }
            }
            else if ( (LA46_0==STRING) ) {
                alt46=1;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 46, 0, input);

                throw nvae;
            }
            switch (alt46) {
                case 1 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:258:19: (loc= ID )? file= STRING
                    {
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:258:22: (loc= ID )?
                    int alt45=2;
                    int LA45_0 = input.LA(1);

                    if ( (LA45_0==ID) ) {
                        alt45=1;
                    }
                    switch (alt45) {
                        case 1 :
                            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:0:0: loc= ID
                            {
                            loc=(Token)match(input,ID,FOLLOW_ID_in_readOperator1708); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_ID.add(loc);


                            }
                            break;

                    }

                    file=(Token)match(input,STRING,FOLLOW_STRING_in_readOperator1713); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STRING.add(file);


                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:258:41: loc= ID '(' file= STRING ')'
                    {
                    loc=(Token)match(input,ID,FOLLOW_ID_in_readOperator1719); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(loc);

                    char_literal94=(Token)match(input,31,FOLLOW_31_in_readOperator1721); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_31.add(char_literal94);

                    file=(Token)match(input,STRING,FOLLOW_STRING_in_readOperator1725); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STRING.add(file);

                    char_literal95=(Token)match(input,32,FOLLOW_32_in_readOperator1727); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_32.add(char_literal95);


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
            // 258:140: ->
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
            if ( state.backtracking>0 ) { memoize(input, 33, readOperator_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "readOperator"

    public static class writeOperator_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "writeOperator"
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:260:1: writeOperator : 'write' from= VAR 'to' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' ) ->;
    public final MeteorParser.writeOperator_return writeOperator() throws RecognitionException {
        MeteorParser.writeOperator_return retval = new MeteorParser.writeOperator_return();
        retval.start = input.LT(1);
        int writeOperator_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token from=null;
        Token loc=null;
        Token file=null;
        Token string_literal96=null;
        Token string_literal97=null;
        Token char_literal98=null;
        Token char_literal99=null;

        EvaluationExpression from_tree=null;
        EvaluationExpression loc_tree=null;
        EvaluationExpression file_tree=null;
        EvaluationExpression string_literal96_tree=null;
        EvaluationExpression string_literal97_tree=null;
        EvaluationExpression char_literal98_tree=null;
        EvaluationExpression char_literal99_tree=null;
        RewriteRuleTokenStream stream_67=new RewriteRuleTokenStream(adaptor,"token 67");
        RewriteRuleTokenStream stream_68=new RewriteRuleTokenStream(adaptor,"token 68");
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_32=new RewriteRuleTokenStream(adaptor,"token 32");
        RewriteRuleTokenStream stream_31=new RewriteRuleTokenStream(adaptor,"token 31");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_STRING=new RewriteRuleTokenStream(adaptor,"token STRING");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 34) ) { return retval; }
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:261:2: ( 'write' from= VAR 'to' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' ) ->)
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:261:4: 'write' from= VAR 'to' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' )
            {
            string_literal96=(Token)match(input,67,FOLLOW_67_in_writeOperator1741); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_67.add(string_literal96);

            from=(Token)match(input,VAR,FOLLOW_VAR_in_writeOperator1745); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_VAR.add(from);

            string_literal97=(Token)match(input,68,FOLLOW_68_in_writeOperator1747); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_68.add(string_literal97);

            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:261:26: ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' )
            int alt48=2;
            int LA48_0 = input.LA(1);

            if ( (LA48_0==ID) ) {
                int LA48_1 = input.LA(2);

                if ( (LA48_1==31) ) {
                    alt48=2;
                }
                else if ( (LA48_1==STRING) ) {
                    alt48=1;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 48, 1, input);

                    throw nvae;
                }
            }
            else if ( (LA48_0==STRING) ) {
                alt48=1;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 48, 0, input);

                throw nvae;
            }
            switch (alt48) {
                case 1 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:261:27: (loc= ID )? file= STRING
                    {
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:261:30: (loc= ID )?
                    int alt47=2;
                    int LA47_0 = input.LA(1);

                    if ( (LA47_0==ID) ) {
                        alt47=1;
                    }
                    switch (alt47) {
                        case 1 :
                            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:0:0: loc= ID
                            {
                            loc=(Token)match(input,ID,FOLLOW_ID_in_writeOperator1752); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_ID.add(loc);


                            }
                            break;

                    }

                    file=(Token)match(input,STRING,FOLLOW_STRING_in_writeOperator1757); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STRING.add(file);


                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:261:49: loc= ID '(' file= STRING ')'
                    {
                    loc=(Token)match(input,ID,FOLLOW_ID_in_writeOperator1763); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(loc);

                    char_literal98=(Token)match(input,31,FOLLOW_31_in_writeOperator1765); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_31.add(char_literal98);

                    file=(Token)match(input,STRING,FOLLOW_STRING_in_writeOperator1769); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STRING.add(file);

                    char_literal99=(Token)match(input,32,FOLLOW_32_in_writeOperator1771); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_32.add(char_literal99);


                    }
                    break;

            }

            if ( state.backtracking==0 ) {
               
              	Sink sink = new Sink(JsonOutputFormat.class, (file!=null?file.getText():null));
                ((operator_scope)operator_stack.peek()).result = sink;
                sink.setInputs(getVariable(from).getStream());
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
            // 267:3: ->
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
            if ( state.backtracking>0 ) { memoize(input, 34, writeOperator_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "writeOperator"

    protected static class genericOperator_scope {
        OperatorInfo<?> operatorInfo;
    }
    protected Stack genericOperator_stack = new Stack();

    public static class genericOperator_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "genericOperator"
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:269:1: genericOperator : (packageName= ID ':' )? name= ID {...}? => ( operatorFlag )* ( arrayInput | input ( ',' input )* ) ( operatorOption )* ->;
    public final MeteorParser.genericOperator_return genericOperator() throws RecognitionException {
        genericOperator_stack.push(new genericOperator_scope());
        MeteorParser.genericOperator_return retval = new MeteorParser.genericOperator_return();
        retval.start = input.LT(1);
        int genericOperator_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token packageName=null;
        Token name=null;
        Token char_literal100=null;
        Token char_literal104=null;
        MeteorParser.operatorFlag_return operatorFlag101 = null;

        MeteorParser.arrayInput_return arrayInput102 = null;

        MeteorParser.input_return input103 = null;

        MeteorParser.input_return input105 = null;

        MeteorParser.operatorOption_return operatorOption106 = null;


        EvaluationExpression packageName_tree=null;
        EvaluationExpression name_tree=null;
        EvaluationExpression char_literal100_tree=null;
        EvaluationExpression char_literal104_tree=null;
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_35=new RewriteRuleTokenStream(adaptor,"token 35");
        RewriteRuleTokenStream stream_28=new RewriteRuleTokenStream(adaptor,"token 28");
        RewriteRuleSubtreeStream stream_operatorOption=new RewriteRuleSubtreeStream(adaptor,"rule operatorOption");
        RewriteRuleSubtreeStream stream_input=new RewriteRuleSubtreeStream(adaptor,"rule input");
        RewriteRuleSubtreeStream stream_operatorFlag=new RewriteRuleSubtreeStream(adaptor,"rule operatorFlag");
        RewriteRuleSubtreeStream stream_arrayInput=new RewriteRuleSubtreeStream(adaptor,"rule arrayInput");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 35) ) { return retval; }
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:272:3: ( (packageName= ID ':' )? name= ID {...}? => ( operatorFlag )* ( arrayInput | input ( ',' input )* ) ( operatorOption )* ->)
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:272:5: (packageName= ID ':' )? name= ID {...}? => ( operatorFlag )* ( arrayInput | input ( ',' input )* ) ( operatorOption )*
            {
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:272:5: (packageName= ID ':' )?
            int alt49=2;
            int LA49_0 = input.LA(1);

            if ( (LA49_0==ID) ) {
                int LA49_1 = input.LA(2);

                if ( (LA49_1==35) ) {
                    alt49=1;
                }
            }
            switch (alt49) {
                case 1 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:272:6: packageName= ID ':'
                    {
                    packageName=(Token)match(input,ID,FOLLOW_ID_in_genericOperator1792); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(packageName);

                    char_literal100=(Token)match(input,35,FOLLOW_35_in_genericOperator1794); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_35.add(char_literal100);


                    }
                    break;

            }

            name=(Token)match(input,ID,FOLLOW_ID_in_genericOperator1800); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);

            if ( !(( (((genericOperator_scope)genericOperator_stack.peek()).operatorInfo = findOperatorGreedily((packageName!=null?packageName.getText():null), name)) != null )) ) {
                if (state.backtracking>0) {state.failed=true; return retval;}
                throw new FailedPredicateException(input, "genericOperator", " ($genericOperator::operatorInfo = findOperatorGreedily($packageName.text, $name)) != null ");
            }
            if ( state.backtracking==0 ) {
               ((operator_scope)operator_stack.peek()).result = ((genericOperator_scope)genericOperator_stack.peek()).operatorInfo.newInstance(); 
            }
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:274:1: ( operatorFlag )*
            loop50:
            do {
                int alt50=2;
                int LA50_0 = input.LA(1);

                if ( (LA50_0==ID) ) {
                    alt50=1;
                }


                switch (alt50) {
            	case 1 :
            	    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:0:0: operatorFlag
            	    {
            	    pushFollow(FOLLOW_operatorFlag_in_genericOperator1808);
            	    operatorFlag101=operatorFlag();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_operatorFlag.add(operatorFlag101.getTree());

            	    }
            	    break;

            	default :
            	    break loop50;
                }
            } while (true);

            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:275:1: ( arrayInput | input ( ',' input )* )
            int alt52=2;
            int LA52_0 = input.LA(1);

            if ( (LA52_0==63) ) {
                alt52=1;
            }
            else if ( (LA52_0==VAR||LA52_0==69) ) {
                alt52=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 52, 0, input);

                throw nvae;
            }
            switch (alt52) {
                case 1 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:275:2: arrayInput
                    {
                    pushFollow(FOLLOW_arrayInput_in_genericOperator1812);
                    arrayInput102=arrayInput();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_arrayInput.add(arrayInput102.getTree());

                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:275:15: input ( ',' input )*
                    {
                    pushFollow(FOLLOW_input_in_genericOperator1816);
                    input103=input();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_input.add(input103.getTree());
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:275:21: ( ',' input )*
                    loop51:
                    do {
                        int alt51=2;
                        int LA51_0 = input.LA(1);

                        if ( (LA51_0==28) ) {
                            int LA51_2 = input.LA(2);

                            if ( (synpred77_Meteor()) ) {
                                alt51=1;
                            }


                        }


                        switch (alt51) {
                    	case 1 :
                    	    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:275:22: ',' input
                    	    {
                    	    char_literal104=(Token)match(input,28,FOLLOW_28_in_genericOperator1819); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_28.add(char_literal104);

                    	    pushFollow(FOLLOW_input_in_genericOperator1821);
                    	    input105=input();

                    	    state._fsp--;
                    	    if (state.failed) return retval;
                    	    if ( state.backtracking==0 ) stream_input.add(input105.getTree());

                    	    }
                    	    break;

                    	default :
                    	    break loop51;
                        }
                    } while (true);


                    }
                    break;

            }

            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:276:1: ( operatorOption )*
            loop53:
            do {
                int alt53=2;
                int LA53_0 = input.LA(1);

                if ( (LA53_0==ID) ) {
                    int LA53_2 = input.LA(2);

                    if ( (synpred78_Meteor()) ) {
                        alt53=1;
                    }


                }


                switch (alt53) {
            	case 1 :
            	    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:0:0: operatorOption
            	    {
            	    pushFollow(FOLLOW_operatorOption_in_genericOperator1827);
            	    operatorOption106=operatorOption();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_operatorOption.add(operatorOption106.getTree());

            	    }
            	    break;

            	default :
            	    break loop53;
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
            // 276:17: ->
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
            if ( state.backtracking>0 ) { memoize(input, 35, genericOperator_StartIndex); }
            genericOperator_stack.pop();
        }
        return retval;
    }
    // $ANTLR end "genericOperator"

    protected static class operatorOption_scope {
        OperatorInfo.OperatorPropertyInfo property;
    }
    protected Stack operatorOption_stack = new Stack();

    public static class operatorOption_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "operatorOption"
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:278:1: operatorOption : name= ID {...}?expr= contextAwareExpression[null] ->;
    public final MeteorParser.operatorOption_return operatorOption() throws RecognitionException {
        operatorOption_stack.push(new operatorOption_scope());
        MeteorParser.operatorOption_return retval = new MeteorParser.operatorOption_return();
        retval.start = input.LT(1);
        int operatorOption_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token name=null;
        MeteorParser.contextAwareExpression_return expr = null;


        EvaluationExpression name_tree=null;
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_contextAwareExpression=new RewriteRuleSubtreeStream(adaptor,"rule contextAwareExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 36) ) { return retval; }
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:282:2: (name= ID {...}?expr= contextAwareExpression[null] ->)
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:282:4: name= ID {...}?expr= contextAwareExpression[null]
            {
            name=(Token)match(input,ID,FOLLOW_ID_in_operatorOption1847); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);

            if ( !(( (((operatorOption_scope)operatorOption_stack.peek()).property = findOperatorPropertyRelunctantly(((genericOperator_scope)genericOperator_stack.peek()).operatorInfo, name)) != null )) ) {
                if (state.backtracking>0) {state.failed=true; return retval;}
                throw new FailedPredicateException(input, "operatorOption", " ($operatorOption::property = findOperatorPropertyRelunctantly($genericOperator::operatorInfo, $name)) != null ");
            }
            pushFollow(FOLLOW_contextAwareExpression_in_operatorOption1853);
            expr=contextAwareExpression(null);

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_contextAwareExpression.add(expr.getTree());
            if ( state.backtracking==0 ) {
               ((operatorOption_scope)operatorOption_stack.peek()).property.setValue(((operator_scope)operator_stack.peek()).result, (expr!=null?((EvaluationExpression)expr.tree):null)); 
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
            // 283:106: ->
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
            if ( state.backtracking>0 ) { memoize(input, 36, operatorOption_StartIndex); }
            operatorOption_stack.pop();
        }
        return retval;
    }
    // $ANTLR end "operatorOption"

    protected static class operatorFlag_scope {
        OperatorInfo.OperatorPropertyInfo property;
    }
    protected Stack operatorFlag_stack = new Stack();

    public static class operatorFlag_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "operatorFlag"
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:285:1: operatorFlag : name= ID {...}? ->;
    public final MeteorParser.operatorFlag_return operatorFlag() throws RecognitionException {
        operatorFlag_stack.push(new operatorFlag_scope());
        MeteorParser.operatorFlag_return retval = new MeteorParser.operatorFlag_return();
        retval.start = input.LT(1);
        int operatorFlag_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token name=null;

        EvaluationExpression name_tree=null;
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 37) ) { return retval; }
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:289:3: (name= ID {...}? ->)
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:289:5: name= ID {...}?
            {
            name=(Token)match(input,ID,FOLLOW_ID_in_operatorFlag1874); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);

            if ( !(( (((operatorFlag_scope)operatorFlag_stack.peek()).property = findOperatorPropertyRelunctantly(((genericOperator_scope)genericOperator_stack.peek()).operatorInfo, name)) != null )) ) {
                if (state.backtracking>0) {state.failed=true; return retval;}
                throw new FailedPredicateException(input, "operatorFlag", " ($operatorFlag::property = findOperatorPropertyRelunctantly($genericOperator::operatorInfo, $name)) != null ");
            }
            if ( state.backtracking==0 ) {
               if(!((operatorFlag_scope)operatorFlag_stack.peek()).property.isFlag())
                  throw new QueryParserException(String.format("Property %s is not a flag", (name!=null?name.getText():null)), name);
                ((operatorFlag_scope)operatorFlag_stack.peek()).property.setValue(((operator_scope)operator_stack.peek()).result, true); 
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
            // 292:64: ->
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
            if ( state.backtracking>0 ) { memoize(input, 37, operatorFlag_StartIndex); }
            operatorFlag_stack.pop();
        }
        return retval;
    }
    // $ANTLR end "operatorFlag"

    protected static class input_scope {
        OperatorInfo.InputPropertyInfo inputProperty;
    }
    protected Stack input_stack = new Stack();

    public static class input_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "input"
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:294:1: input : (preserveFlag= 'preserve' )? (name= VAR 'in' )? from= VAR (inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::numInputs - 1)] )? ->;
    public final MeteorParser.input_return input() throws RecognitionException {
        input_stack.push(new input_scope());
        MeteorParser.input_return retval = new MeteorParser.input_return();
        retval.start = input.LT(1);
        int input_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token preserveFlag=null;
        Token name=null;
        Token from=null;
        Token inputOption=null;
        Token string_literal107=null;
        MeteorParser.contextAwareExpression_return expr = null;


        EvaluationExpression preserveFlag_tree=null;
        EvaluationExpression name_tree=null;
        EvaluationExpression from_tree=null;
        EvaluationExpression inputOption_tree=null;
        EvaluationExpression string_literal107_tree=null;
        RewriteRuleTokenStream stream_69=new RewriteRuleTokenStream(adaptor,"token 69");
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_42=new RewriteRuleTokenStream(adaptor,"token 42");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_contextAwareExpression=new RewriteRuleSubtreeStream(adaptor,"rule contextAwareExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 38) ) { return retval; }
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:297:3: ( (preserveFlag= 'preserve' )? (name= VAR 'in' )? from= VAR (inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::numInputs - 1)] )? ->)
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:297:5: (preserveFlag= 'preserve' )? (name= VAR 'in' )? from= VAR (inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::numInputs - 1)] )?
            {
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:297:17: (preserveFlag= 'preserve' )?
            int alt54=2;
            int LA54_0 = input.LA(1);

            if ( (LA54_0==69) ) {
                alt54=1;
            }
            switch (alt54) {
                case 1 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:0:0: preserveFlag= 'preserve'
                    {
                    preserveFlag=(Token)match(input,69,FOLLOW_69_in_input1896); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_69.add(preserveFlag);


                    }
                    break;

            }

            if ( state.backtracking==0 ) {
            }
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:297:33: (name= VAR 'in' )?
            int alt55=2;
            int LA55_0 = input.LA(1);

            if ( (LA55_0==VAR) ) {
                int LA55_1 = input.LA(2);

                if ( (LA55_1==42) ) {
                    alt55=1;
                }
            }
            switch (alt55) {
                case 1 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:297:34: name= VAR 'in'
                    {
                    name=(Token)match(input,VAR,FOLLOW_VAR_in_input1904); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_VAR.add(name);

                    string_literal107=(Token)match(input,42,FOLLOW_42_in_input1906); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_42.add(string_literal107);


                    }
                    break;

            }

            from=(Token)match(input,VAR,FOLLOW_VAR_in_input1912); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_VAR.add(from);

            if ( state.backtracking==0 ) {
               
                int inputIndex = ((operator_scope)operator_stack.peek()).numInputs++;
                JsonStreamExpression input = getVariable(from);
                ((operator_scope)operator_stack.peek()).result.setInput(inputIndex, input.getStream());
                
                JsonStreamExpression inputExpression = new JsonStreamExpression(input.getStream(), inputIndex);
                if(preserveFlag != null)
                  inputExpression.addTag(ExpressionTag.RETAIN);
                putVariable(name != null ? name : from, inputExpression);

            }
            if ( state.backtracking==0 ) {
               if(state.backtracking == 0) {
                  addScope();
                }

            }
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:312:1: (inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::numInputs - 1)] )?
            int alt56=2;
            alt56 = dfa56.predict(input);
            switch (alt56) {
                case 1 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:312:2: inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::numInputs - 1)]
                    {
                    inputOption=(Token)match(input,ID,FOLLOW_ID_in_input1922); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(inputOption);

                    if ( !(( (((input_scope)input_stack.peek()).inputProperty = findInputPropertyRelunctantly(((genericOperator_scope)genericOperator_stack.peek()).operatorInfo, inputOption)) != null )) ) {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        throw new FailedPredicateException(input, "input", " ($input::inputProperty = findInputPropertyRelunctantly($genericOperator::operatorInfo, $inputOption)) != null ");
                    }
                    pushFollow(FOLLOW_contextAwareExpression_in_input1930);
                    expr=contextAwareExpression(new InputSelection(((operator_scope)operator_stack.peek()).numInputs - 1));

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_contextAwareExpression.add(expr.getTree());
                    if ( state.backtracking==0 ) {
                       ((input_scope)input_stack.peek()).inputProperty.setValue(((operator_scope)operator_stack.peek()).result, ((operator_scope)operator_stack.peek()).numInputs-1, (expr!=null?((EvaluationExpression)expr.tree):null)); 
                    }

                    }
                    break;

            }

            if ( state.backtracking==0 ) {
               if(state.backtracking == 0) 
                  removeScope();

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
            // 317:1: ->
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
            if ( state.backtracking>0 ) { memoize(input, 38, input_StartIndex); }
            input_stack.pop();
        }
        return retval;
    }
    // $ANTLR end "input"

    public static class arrayInput_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "arrayInput"
    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:319:1: arrayInput : '[' names+= VAR ( ',' names+= VAR )? ']' 'in' from= VAR ->;
    public final MeteorParser.arrayInput_return arrayInput() throws RecognitionException {
        MeteorParser.arrayInput_return retval = new MeteorParser.arrayInput_return();
        retval.start = input.LT(1);
        int arrayInput_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token from=null;
        Token char_literal108=null;
        Token char_literal109=null;
        Token char_literal110=null;
        Token string_literal111=null;
        Token names=null;
        List list_names=null;

        EvaluationExpression from_tree=null;
        EvaluationExpression char_literal108_tree=null;
        EvaluationExpression char_literal109_tree=null;
        EvaluationExpression char_literal110_tree=null;
        EvaluationExpression string_literal111_tree=null;
        EvaluationExpression names_tree=null;
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_42=new RewriteRuleTokenStream(adaptor,"token 42");
        RewriteRuleTokenStream stream_64=new RewriteRuleTokenStream(adaptor,"token 64");
        RewriteRuleTokenStream stream_63=new RewriteRuleTokenStream(adaptor,"token 63");
        RewriteRuleTokenStream stream_28=new RewriteRuleTokenStream(adaptor,"token 28");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 39) ) { return retval; }
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:320:3: ( '[' names+= VAR ( ',' names+= VAR )? ']' 'in' from= VAR ->)
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:320:5: '[' names+= VAR ( ',' names+= VAR )? ']' 'in' from= VAR
            {
            char_literal108=(Token)match(input,63,FOLLOW_63_in_arrayInput1952); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_63.add(char_literal108);

            names=(Token)match(input,VAR,FOLLOW_VAR_in_arrayInput1956); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_VAR.add(names);

            if (list_names==null) list_names=new ArrayList();
            list_names.add(names);

            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:320:20: ( ',' names+= VAR )?
            int alt57=2;
            int LA57_0 = input.LA(1);

            if ( (LA57_0==28) ) {
                alt57=1;
            }
            switch (alt57) {
                case 1 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:320:21: ',' names+= VAR
                    {
                    char_literal109=(Token)match(input,28,FOLLOW_28_in_arrayInput1959); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_28.add(char_literal109);

                    names=(Token)match(input,VAR,FOLLOW_VAR_in_arrayInput1963); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_VAR.add(names);

                    if (list_names==null) list_names=new ArrayList();
                    list_names.add(names);


                    }
                    break;

            }

            char_literal110=(Token)match(input,64,FOLLOW_64_in_arrayInput1967); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_64.add(char_literal110);

            string_literal111=(Token)match(input,42,FOLLOW_42_in_arrayInput1969); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_42.add(string_literal111);

            from=(Token)match(input,VAR,FOLLOW_VAR_in_arrayInput1973); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_VAR.add(from);

            if ( state.backtracking==0 ) {
               
                ((operator_scope)operator_stack.peek()).result.setInput(0, getVariable(from).getStream());
                for(int index = 0; index < list_names.size(); index++) {
              	  putVariable((Token) list_names.get(index), new JsonStreamExpression(null, index)); 
                }

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
            // 326:3: ->
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
            if ( state.backtracking>0 ) { memoize(input, 39, arrayInput_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "arrayInput"

    // $ANTLR start synpred9_Meteor
    public final void synpred9_Meteor_fragment() throws RecognitionException {   
        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:99:5: ( ternaryExpression )
        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:99:5: ternaryExpression
        {
        pushFollow(FOLLOW_ternaryExpression_in_synpred9_Meteor357);
        ternaryExpression();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred9_Meteor

    // $ANTLR start synpred11_Meteor
    public final void synpred11_Meteor_fragment() throws RecognitionException {   
        MeteorParser.orExpression_return ifClause = null;

        MeteorParser.expression_return ifExpr = null;

        MeteorParser.expression_return elseExpr = null;


        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:103:4: (ifClause= orExpression ( '?' (ifExpr= expression )? ':' elseExpr= expression ) )
        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:103:4: ifClause= orExpression ( '?' (ifExpr= expression )? ':' elseExpr= expression )
        {
        pushFollow(FOLLOW_orExpression_in_synpred11_Meteor374);
        ifClause=orExpression();

        state._fsp--;
        if (state.failed) return ;
        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:103:26: ( '?' (ifExpr= expression )? ':' elseExpr= expression )
        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:103:27: '?' (ifExpr= expression )? ':' elseExpr= expression
        {
        match(input,34,FOLLOW_34_in_synpred11_Meteor377); if (state.failed) return ;
        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:103:37: (ifExpr= expression )?
        int alt59=2;
        int LA59_0 = input.LA(1);

        if ( ((LA59_0>=ID && LA59_0<=STRING)||(LA59_0>=DECIMAL && LA59_0<=INTEGER)||LA59_0==31||(LA59_0>=52 && LA59_0<=55)||LA59_0==58||(LA59_0>=60 && LA59_0<=63)||LA59_0==65||LA59_0==67) ) {
            alt59=1;
        }
        switch (alt59) {
            case 1 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:0:0: ifExpr= expression
                {
                pushFollow(FOLLOW_expression_in_synpred11_Meteor381);
                ifExpr=expression();

                state._fsp--;
                if (state.failed) return ;

                }
                break;

        }

        match(input,35,FOLLOW_35_in_synpred11_Meteor384); if (state.failed) return ;
        pushFollow(FOLLOW_expression_in_synpred11_Meteor388);
        elseExpr=expression();

        state._fsp--;
        if (state.failed) return ;

        }


        }
    }
    // $ANTLR end synpred11_Meteor

    // $ANTLR start synpred12_Meteor
    public final void synpred12_Meteor_fragment() throws RecognitionException {   
        MeteorParser.orExpression_return ifExpr2 = null;

        MeteorParser.expression_return ifClause2 = null;


        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:105:4: (ifExpr2= orExpression 'if' ifClause2= expression )
        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:105:4: ifExpr2= orExpression 'if' ifClause2= expression
        {
        pushFollow(FOLLOW_orExpression_in_synpred12_Meteor411);
        ifExpr2=orExpression();

        state._fsp--;
        if (state.failed) return ;
        match(input,36,FOLLOW_36_in_synpred12_Meteor413); if (state.failed) return ;
        pushFollow(FOLLOW_expression_in_synpred12_Meteor417);
        ifClause2=expression();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred12_Meteor

    // $ANTLR start synpred33_Meteor
    public final void synpred33_Meteor_fragment() throws RecognitionException {   
        Token type=null;
        MeteorParser.generalPathExpression_return expr = null;


        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:153:5: ( '(' type= ID ')' expr= generalPathExpression )
        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:153:5: '(' type= ID ')' expr= generalPathExpression
        {
        match(input,31,FOLLOW_31_in_synpred33_Meteor871); if (state.failed) return ;
        type=(Token)match(input,ID,FOLLOW_ID_in_synpred33_Meteor875); if (state.failed) return ;
        match(input,32,FOLLOW_32_in_synpred33_Meteor877); if (state.failed) return ;
        pushFollow(FOLLOW_generalPathExpression_in_synpred33_Meteor881);
        expr=generalPathExpression();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred33_Meteor

    // $ANTLR start synpred34_Meteor
    public final void synpred34_Meteor_fragment() throws RecognitionException {   
        Token type=null;
        MeteorParser.generalPathExpression_return expr = null;


        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:154:4: (expr= generalPathExpression 'as' type= ID )
        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:154:4: expr= generalPathExpression 'as' type= ID
        {
        pushFollow(FOLLOW_generalPathExpression_in_synpred34_Meteor888);
        expr=generalPathExpression();

        state._fsp--;
        if (state.failed) return ;
        match(input,56,FOLLOW_56_in_synpred34_Meteor890); if (state.failed) return ;
        type=(Token)match(input,ID,FOLLOW_ID_in_synpred34_Meteor894); if (state.failed) return ;

        }
    }
    // $ANTLR end synpred34_Meteor

    // $ANTLR start synpred35_Meteor
    public final void synpred35_Meteor_fragment() throws RecognitionException {   
        MeteorParser.valueExpression_return value = null;

        MeteorParser.pathExpression_return path = null;


        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:160:4: (value= valueExpression path= pathExpression )
        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:160:4: value= valueExpression path= pathExpression
        {
        pushFollow(FOLLOW_valueExpression_in_synpred35_Meteor928);
        value=valueExpression();

        state._fsp--;
        if (state.failed) return ;
        pushFollow(FOLLOW_pathExpression_in_synpred35_Meteor932);
        path=pathExpression();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred35_Meteor

    // $ANTLR start synpred36_Meteor
    public final void synpred36_Meteor_fragment() throws RecognitionException {   
        Token field=null;

        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:170:7: ( ( '.' (field= ID ) ) )
        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:170:7: ( '.' (field= ID ) )
        {
        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:170:7: ( '.' (field= ID ) )
        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:170:8: '.' (field= ID )
        {
        match(input,57,FOLLOW_57_in_synpred36_Meteor988); if (state.failed) return ;
        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:170:12: (field= ID )
        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:170:13: field= ID
        {
        field=(Token)match(input,ID,FOLLOW_ID_in_synpred36_Meteor993); if (state.failed) return ;

        }


        }


        }
    }
    // $ANTLR end synpred36_Meteor

    // $ANTLR start synpred37_Meteor
    public final void synpred37_Meteor_fragment() throws RecognitionException {   
        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:171:11: ( arrayAccess )
        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:171:11: arrayAccess
        {
        pushFollow(FOLLOW_arrayAccess_in_synpred37_Meteor1011);
        arrayAccess();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred37_Meteor

    // $ANTLR start synpred38_Meteor
    public final void synpred38_Meteor_fragment() throws RecognitionException {   
        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:175:4: ( methodCall[null] )
        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:175:4: methodCall[null]
        {
        pushFollow(FOLLOW_methodCall_in_synpred38_Meteor1032);
        methodCall(null);

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred38_Meteor

    // $ANTLR start synpred41_Meteor
    public final void synpred41_Meteor_fragment() throws RecognitionException {   
        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:178:4: ( VAR )
        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:178:4: VAR
        {
        match(input,VAR,FOLLOW_VAR_in_synpred41_Meteor1050); if (state.failed) return ;

        }
    }
    // $ANTLR end synpred41_Meteor

    // $ANTLR start synpred42_Meteor
    public final void synpred42_Meteor_fragment() throws RecognitionException {   
        Token packageName=null;

        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:179:6: (packageName= ID ':' )
        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:179:6: packageName= ID ':'
        {
        packageName=(Token)match(input,ID,FOLLOW_ID_in_synpred42_Meteor1063); if (state.failed) return ;
        match(input,35,FOLLOW_35_in_synpred42_Meteor1065); if (state.failed) return ;

        }
    }
    // $ANTLR end synpred42_Meteor

    // $ANTLR start synpred43_Meteor
    public final void synpred43_Meteor_fragment() throws RecognitionException {   
        Token packageName=null;
        Token constant=null;

        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:179:5: ( (packageName= ID ':' )? constant= ID {...}? =>)
        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:179:5: (packageName= ID ':' )? constant= ID {...}? =>
        {
        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:179:5: (packageName= ID ':' )?
        int alt64=2;
        int LA64_0 = input.LA(1);

        if ( (LA64_0==ID) ) {
            int LA64_1 = input.LA(2);

            if ( (LA64_1==35) ) {
                alt64=1;
            }
        }
        switch (alt64) {
            case 1 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:179:6: packageName= ID ':'
                {
                packageName=(Token)match(input,ID,FOLLOW_ID_in_synpred43_Meteor1063); if (state.failed) return ;
                match(input,35,FOLLOW_35_in_synpred43_Meteor1065); if (state.failed) return ;

                }
                break;

        }

        constant=(Token)match(input,ID,FOLLOW_ID_in_synpred43_Meteor1071); if (state.failed) return ;
        if ( !(( getScope((packageName!=null?packageName.getText():null)).getConstantRegistry().get((constant!=null?constant.getText():null)) != null )) ) {
            if (state.backtracking>0) {state.failed=true; return ;}
            throw new FailedPredicateException(input, "synpred43_Meteor", " getScope($packageName.text).getConstantRegistry().get($constant.text) != null ");
        }

        }
    }
    // $ANTLR end synpred43_Meteor

    // $ANTLR start synpred44_Meteor
    public final void synpred44_Meteor_fragment() throws RecognitionException {   
        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:181:5: ( streamIndexAccess )
        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:181:5: streamIndexAccess
        {
        pushFollow(FOLLOW_streamIndexAccess_in_synpred44_Meteor1089);
        streamIndexAccess();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred44_Meteor

    // $ANTLR start synpred77_Meteor
    public final void synpred77_Meteor_fragment() throws RecognitionException {   
        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:275:22: ( ',' input )
        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:275:22: ',' input
        {
        match(input,28,FOLLOW_28_in_synpred77_Meteor1819); if (state.failed) return ;
        pushFollow(FOLLOW_input_in_synpred77_Meteor1821);
        input();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred77_Meteor

    // $ANTLR start synpred78_Meteor
    public final void synpred78_Meteor_fragment() throws RecognitionException {   
        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:276:1: ( operatorOption )
        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:276:1: operatorOption
        {
        pushFollow(FOLLOW_operatorOption_in_synpred78_Meteor1827);
        operatorOption();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred78_Meteor

    // $ANTLR start synpred81_Meteor
    public final void synpred81_Meteor_fragment() throws RecognitionException {   
        Token inputOption=null;
        MeteorParser.contextAwareExpression_return expr = null;


        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:312:2: (inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::numInputs - 1)] )
        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:312:2: inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::numInputs - 1)]
        {
        inputOption=(Token)match(input,ID,FOLLOW_ID_in_synpred81_Meteor1922); if (state.failed) return ;
        if ( !(( (((input_scope)input_stack.peek()).inputProperty = findInputPropertyRelunctantly(((genericOperator_scope)genericOperator_stack.peek()).operatorInfo, inputOption)) != null )) ) {
            if (state.backtracking>0) {state.failed=true; return ;}
            throw new FailedPredicateException(input, "synpred81_Meteor", " ($input::inputProperty = findInputPropertyRelunctantly($genericOperator::operatorInfo, $inputOption)) != null ");
        }
        pushFollow(FOLLOW_contextAwareExpression_in_synpred81_Meteor1930);
        expr=contextAwareExpression(new InputSelection(((operator_scope)operator_stack.peek()).numInputs - 1));

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred81_Meteor

    // Delegated rules

    public final boolean synpred37_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred37_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred34_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred34_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred78_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred78_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred42_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred42_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred43_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred43_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred36_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred36_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred38_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred38_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred12_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred12_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred11_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred11_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred9_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred9_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred41_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred41_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred44_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred44_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred35_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred35_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred81_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred81_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred77_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred77_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred33_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred33_Meteor_fragment(); // can never throw exception
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
    protected DFA8 dfa8 = new DFA8(this);
    protected DFA23 dfa23 = new DFA23(this);
    protected DFA24 dfa24 = new DFA24(this);
    protected DFA27 dfa27 = new DFA27(this);
    protected DFA56 dfa56 = new DFA56(this);
    static final String DFA6_eotS =
        "\20\uffff";
    static final String DFA6_eofS =
        "\2\uffff\1\1\11\uffff\1\1\3\uffff";
    static final String DFA6_minS =
        "\1\6\1\uffff\1\6\1\uffff\1\0\1\7\1\6\2\0\1\6\1\11\2\6\1\11\1\6\1"+
        "\0";
    static final String DFA6_maxS =
        "\1\103\1\uffff\1\105\1\uffff\1\0\1\14\1\105\2\0\1\103\1\100\1\103"+
        "\2\100\1\77\1\0";
    static final String DFA6_acceptS =
        "\1\uffff\1\1\1\uffff\1\2\14\uffff";
    static final String DFA6_specialS =
        "\4\uffff\1\0\2\uffff\1\1\1\2\6\uffff\1\3}>";
    static final String[] DFA6_transitionS = {
            "\1\2\2\1\1\uffff\2\1\23\uffff\1\1\24\uffff\4\1\2\uffff\1\1\1"+
            "\uffff\4\1\1\uffff\1\3\1\uffff\1\3",
            "",
            "\1\6\1\3\1\uffff\1\1\20\uffff\1\1\1\uffff\1\1\2\uffff\2\1\1"+
            "\uffff\1\1\1\4\20\1\4\uffff\2\1\1\uffff\1\1\3\uffff\1\5\1\1"+
            "\4\uffff\1\3",
            "",
            "\1\uffff",
            "\1\3\1\uffff\1\1\1\uffff\2\1",
            "\1\7\1\10\1\1\1\uffff\2\1\23\uffff\1\1\24\uffff\4\1\2\uffff"+
            "\1\1\1\uffff\3\1\1\11\1\uffff\1\1\1\uffff\1\1\1\uffff\1\3",
            "\1\uffff",
            "\1\uffff",
            "\1\1\1\12\1\1\1\uffff\2\1\23\uffff\1\1\24\uffff\4\1\2\uffff"+
            "\1\1\1\uffff\4\1\1\uffff\1\1\1\uffff\1\1",
            "\1\1\22\uffff\1\13\5\uffff\1\1\1\uffff\20\1\4\uffff\2\1\5\uffff"+
            "\1\1\1\14",
            "\1\1\1\15\1\1\1\uffff\2\1\23\uffff\1\1\24\uffff\4\1\2\uffff"+
            "\1\1\1\uffff\6\1\1\uffff\1\1",
            "\1\1\2\uffff\1\1\20\uffff\1\1\1\uffff\1\1\3\uffff\1\1\1\uffff"+
            "\10\1\1\16\11\1\4\uffff\2\1\1\uffff\1\1\3\uffff\2\1",
            "\1\1\22\uffff\1\1\5\uffff\1\1\1\uffff\20\1\4\uffff\2\1\5\uffff"+
            "\1\1\1\14",
            "\1\1\1\17\1\1\1\uffff\2\1\23\uffff\1\1\24\uffff\4\1\2\uffff"+
            "\1\1\1\uffff\4\1",
            "\1\uffff"
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
            return "98:1: expression : ( ternaryExpression | operatorExpression );";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA6_4 = input.LA(1);

                         
                        int index6_4 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred9_Meteor()) ) {s = 1;}

                        else if ( (true) ) {s = 3;}

                         
                        input.seek(index6_4);
                        if ( s>=0 ) return s;
                        break;
                    case 1 : 
                        int LA6_7 = input.LA(1);

                         
                        int index6_7 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred9_Meteor()) ) {s = 1;}

                        else if ( (true) ) {s = 3;}

                         
                        input.seek(index6_7);
                        if ( s>=0 ) return s;
                        break;
                    case 2 : 
                        int LA6_8 = input.LA(1);

                         
                        int index6_8 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred9_Meteor()) ) {s = 1;}

                        else if ( (true) ) {s = 3;}

                         
                        input.seek(index6_8);
                        if ( s>=0 ) return s;
                        break;
                    case 3 : 
                        int LA6_15 = input.LA(1);

                         
                        int index6_15 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred9_Meteor()) ) {s = 1;}

                        else if ( (true) ) {s = 3;}

                         
                        input.seek(index6_15);
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
    static final String DFA8_eotS =
        "\22\uffff";
    static final String DFA8_eofS =
        "\22\uffff";
    static final String DFA8_minS =
        "\1\6\16\0\3\uffff";
    static final String DFA8_maxS =
        "\1\77\16\0\3\uffff";
    static final String DFA8_acceptS =
        "\17\uffff\1\1\1\2\1\3";
    static final String DFA8_specialS =
        "\1\uffff\1\0\1\1\1\2\1\3\1\4\1\5\1\6\1\7\1\10\1\11\1\12\1\13\1\14"+
        "\1\15\3\uffff}>";
    static final String[] DFA8_transitionS = {
            "\1\5\1\14\1\11\1\uffff\1\10\1\12\23\uffff\1\4\24\uffff\1\1\1"+
            "\2\2\3\2\uffff\1\16\1\uffff\1\6\1\7\1\13\1\15",
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

    static final short[] DFA8_eot = DFA.unpackEncodedString(DFA8_eotS);
    static final short[] DFA8_eof = DFA.unpackEncodedString(DFA8_eofS);
    static final char[] DFA8_min = DFA.unpackEncodedStringToUnsignedChars(DFA8_minS);
    static final char[] DFA8_max = DFA.unpackEncodedStringToUnsignedChars(DFA8_maxS);
    static final short[] DFA8_accept = DFA.unpackEncodedString(DFA8_acceptS);
    static final short[] DFA8_special = DFA.unpackEncodedString(DFA8_specialS);
    static final short[][] DFA8_transition;

    static {
        int numStates = DFA8_transitionS.length;
        DFA8_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA8_transition[i] = DFA.unpackEncodedString(DFA8_transitionS[i]);
        }
    }

    class DFA8 extends DFA {

        public DFA8(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 8;
            this.eot = DFA8_eot;
            this.eof = DFA8_eof;
            this.min = DFA8_min;
            this.max = DFA8_max;
            this.accept = DFA8_accept;
            this.special = DFA8_special;
            this.transition = DFA8_transition;
        }
        public String getDescription() {
            return "102:1: ternaryExpression : (ifClause= orExpression ( '?' (ifExpr= expression )? ':' elseExpr= expression ) -> ^( EXPRESSION[\"TernaryExpression\"] $ifClause) | ifExpr2= orExpression 'if' ifClause2= expression -> ^( EXPRESSION[\"TernaryExpression\"] $ifClause2 $ifExpr2) | orExpression );";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA8_1 = input.LA(1);

                         
                        int index8_1 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred11_Meteor()) ) {s = 15;}

                        else if ( (synpred12_Meteor()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index8_1);
                        if ( s>=0 ) return s;
                        break;
                    case 1 : 
                        int LA8_2 = input.LA(1);

                         
                        int index8_2 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred11_Meteor()) ) {s = 15;}

                        else if ( (synpred12_Meteor()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index8_2);
                        if ( s>=0 ) return s;
                        break;
                    case 2 : 
                        int LA8_3 = input.LA(1);

                         
                        int index8_3 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred11_Meteor()) ) {s = 15;}

                        else if ( (synpred12_Meteor()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index8_3);
                        if ( s>=0 ) return s;
                        break;
                    case 3 : 
                        int LA8_4 = input.LA(1);

                         
                        int index8_4 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred11_Meteor()) ) {s = 15;}

                        else if ( (synpred12_Meteor()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index8_4);
                        if ( s>=0 ) return s;
                        break;
                    case 4 : 
                        int LA8_5 = input.LA(1);

                         
                        int index8_5 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred11_Meteor()) ) {s = 15;}

                        else if ( (synpred12_Meteor()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index8_5);
                        if ( s>=0 ) return s;
                        break;
                    case 5 : 
                        int LA8_6 = input.LA(1);

                         
                        int index8_6 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred11_Meteor()) ) {s = 15;}

                        else if ( (synpred12_Meteor()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index8_6);
                        if ( s>=0 ) return s;
                        break;
                    case 6 : 
                        int LA8_7 = input.LA(1);

                         
                        int index8_7 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred11_Meteor()) ) {s = 15;}

                        else if ( (synpred12_Meteor()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index8_7);
                        if ( s>=0 ) return s;
                        break;
                    case 7 : 
                        int LA8_8 = input.LA(1);

                         
                        int index8_8 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred11_Meteor()) ) {s = 15;}

                        else if ( (synpred12_Meteor()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index8_8);
                        if ( s>=0 ) return s;
                        break;
                    case 8 : 
                        int LA8_9 = input.LA(1);

                         
                        int index8_9 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred11_Meteor()) ) {s = 15;}

                        else if ( (synpred12_Meteor()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index8_9);
                        if ( s>=0 ) return s;
                        break;
                    case 9 : 
                        int LA8_10 = input.LA(1);

                         
                        int index8_10 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred11_Meteor()) ) {s = 15;}

                        else if ( (synpred12_Meteor()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index8_10);
                        if ( s>=0 ) return s;
                        break;
                    case 10 : 
                        int LA8_11 = input.LA(1);

                         
                        int index8_11 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred11_Meteor()) ) {s = 15;}

                        else if ( (synpred12_Meteor()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index8_11);
                        if ( s>=0 ) return s;
                        break;
                    case 11 : 
                        int LA8_12 = input.LA(1);

                         
                        int index8_12 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred11_Meteor()) ) {s = 15;}

                        else if ( (synpred12_Meteor()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index8_12);
                        if ( s>=0 ) return s;
                        break;
                    case 12 : 
                        int LA8_13 = input.LA(1);

                         
                        int index8_13 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred11_Meteor()) ) {s = 15;}

                        else if ( (synpred12_Meteor()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index8_13);
                        if ( s>=0 ) return s;
                        break;
                    case 13 : 
                        int LA8_14 = input.LA(1);

                         
                        int index8_14 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred11_Meteor()) ) {s = 15;}

                        else if ( (synpred12_Meteor()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index8_14);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 8, _s, input);
            error(nvae);
            throw nvae;
        }
    }
    static final String DFA23_eotS =
        "\17\uffff";
    static final String DFA23_eofS =
        "\17\uffff";
    static final String DFA23_minS =
        "\1\6\13\0\3\uffff";
    static final String DFA23_maxS =
        "\1\77\13\0\3\uffff";
    static final String DFA23_acceptS =
        "\14\uffff\1\1\1\2\1\3";
    static final String DFA23_specialS =
        "\1\uffff\1\0\1\1\1\2\1\3\1\4\1\5\1\6\1\7\1\10\1\11\1\12\3\uffff}>";
    static final String[] DFA23_transitionS = {
            "\1\2\1\11\1\6\1\uffff\1\5\1\7\23\uffff\1\1\32\uffff\1\13\1\uffff"+
            "\1\3\1\4\1\10\1\12",
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
            return "153:4: ( '(' type= ID ')' expr= generalPathExpression | expr= generalPathExpression 'as' type= ID | expr= generalPathExpression )";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA23_1 = input.LA(1);

                         
                        int index23_1 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred33_Meteor()) ) {s = 12;}

                        else if ( (synpred34_Meteor()) ) {s = 13;}

                        else if ( (true) ) {s = 14;}

                         
                        input.seek(index23_1);
                        if ( s>=0 ) return s;
                        break;
                    case 1 : 
                        int LA23_2 = input.LA(1);

                         
                        int index23_2 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred34_Meteor()) ) {s = 13;}

                        else if ( (true) ) {s = 14;}

                         
                        input.seek(index23_2);
                        if ( s>=0 ) return s;
                        break;
                    case 2 : 
                        int LA23_3 = input.LA(1);

                         
                        int index23_3 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred34_Meteor()) ) {s = 13;}

                        else if ( (true) ) {s = 14;}

                         
                        input.seek(index23_3);
                        if ( s>=0 ) return s;
                        break;
                    case 3 : 
                        int LA23_4 = input.LA(1);

                         
                        int index23_4 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred34_Meteor()) ) {s = 13;}

                        else if ( (true) ) {s = 14;}

                         
                        input.seek(index23_4);
                        if ( s>=0 ) return s;
                        break;
                    case 4 : 
                        int LA23_5 = input.LA(1);

                         
                        int index23_5 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred34_Meteor()) ) {s = 13;}

                        else if ( (true) ) {s = 14;}

                         
                        input.seek(index23_5);
                        if ( s>=0 ) return s;
                        break;
                    case 5 : 
                        int LA23_6 = input.LA(1);

                         
                        int index23_6 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred34_Meteor()) ) {s = 13;}

                        else if ( (true) ) {s = 14;}

                         
                        input.seek(index23_6);
                        if ( s>=0 ) return s;
                        break;
                    case 6 : 
                        int LA23_7 = input.LA(1);

                         
                        int index23_7 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred34_Meteor()) ) {s = 13;}

                        else if ( (true) ) {s = 14;}

                         
                        input.seek(index23_7);
                        if ( s>=0 ) return s;
                        break;
                    case 7 : 
                        int LA23_8 = input.LA(1);

                         
                        int index23_8 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred34_Meteor()) ) {s = 13;}

                        else if ( (true) ) {s = 14;}

                         
                        input.seek(index23_8);
                        if ( s>=0 ) return s;
                        break;
                    case 8 : 
                        int LA23_9 = input.LA(1);

                         
                        int index23_9 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred34_Meteor()) ) {s = 13;}

                        else if ( (true) ) {s = 14;}

                         
                        input.seek(index23_9);
                        if ( s>=0 ) return s;
                        break;
                    case 9 : 
                        int LA23_10 = input.LA(1);

                         
                        int index23_10 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred34_Meteor()) ) {s = 13;}

                        else if ( (true) ) {s = 14;}

                         
                        input.seek(index23_10);
                        if ( s>=0 ) return s;
                        break;
                    case 10 : 
                        int LA23_11 = input.LA(1);

                         
                        int index23_11 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred34_Meteor()) ) {s = 13;}

                        else if ( (true) ) {s = 14;}

                         
                        input.seek(index23_11);
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
    static final String DFA24_eotS =
        "\16\uffff";
    static final String DFA24_eofS =
        "\16\uffff";
    static final String DFA24_minS =
        "\1\6\13\0\2\uffff";
    static final String DFA24_maxS =
        "\1\77\13\0\2\uffff";
    static final String DFA24_acceptS =
        "\14\uffff\1\1\1\2";
    static final String DFA24_specialS =
        "\1\uffff\1\0\1\1\1\2\1\3\1\4\1\5\1\6\1\7\1\10\1\11\1\12\2\uffff}>";
    static final String[] DFA24_transitionS = {
            "\1\1\1\11\1\6\1\uffff\1\5\1\7\23\uffff\1\2\32\uffff\1\13\1\uffff"+
            "\1\3\1\4\1\10\1\12",
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

    static final short[] DFA24_eot = DFA.unpackEncodedString(DFA24_eotS);
    static final short[] DFA24_eof = DFA.unpackEncodedString(DFA24_eofS);
    static final char[] DFA24_min = DFA.unpackEncodedStringToUnsignedChars(DFA24_minS);
    static final char[] DFA24_max = DFA.unpackEncodedStringToUnsignedChars(DFA24_maxS);
    static final short[] DFA24_accept = DFA.unpackEncodedString(DFA24_acceptS);
    static final short[] DFA24_special = DFA.unpackEncodedString(DFA24_specialS);
    static final short[][] DFA24_transition;

    static {
        int numStates = DFA24_transitionS.length;
        DFA24_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA24_transition[i] = DFA.unpackEncodedString(DFA24_transitionS[i]);
        }
    }

    class DFA24 extends DFA {

        public DFA24(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 24;
            this.eot = DFA24_eot;
            this.eof = DFA24_eof;
            this.min = DFA24_min;
            this.max = DFA24_max;
            this.accept = DFA24_accept;
            this.special = DFA24_special;
            this.transition = DFA24_transition;
        }
        public String getDescription() {
            return "159:1: generalPathExpression : (value= valueExpression path= pathExpression -> | valueExpression );";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA24_1 = input.LA(1);

                         
                        int index24_1 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred35_Meteor()) ) {s = 12;}

                        else if ( (true) ) {s = 13;}

                         
                        input.seek(index24_1);
                        if ( s>=0 ) return s;
                        break;
                    case 1 : 
                        int LA24_2 = input.LA(1);

                         
                        int index24_2 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred35_Meteor()) ) {s = 12;}

                        else if ( (true) ) {s = 13;}

                         
                        input.seek(index24_2);
                        if ( s>=0 ) return s;
                        break;
                    case 2 : 
                        int LA24_3 = input.LA(1);

                         
                        int index24_3 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred35_Meteor()) ) {s = 12;}

                        else if ( (true) ) {s = 13;}

                         
                        input.seek(index24_3);
                        if ( s>=0 ) return s;
                        break;
                    case 3 : 
                        int LA24_4 = input.LA(1);

                         
                        int index24_4 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred35_Meteor()) ) {s = 12;}

                        else if ( (true) ) {s = 13;}

                         
                        input.seek(index24_4);
                        if ( s>=0 ) return s;
                        break;
                    case 4 : 
                        int LA24_5 = input.LA(1);

                         
                        int index24_5 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred35_Meteor()) ) {s = 12;}

                        else if ( (true) ) {s = 13;}

                         
                        input.seek(index24_5);
                        if ( s>=0 ) return s;
                        break;
                    case 5 : 
                        int LA24_6 = input.LA(1);

                         
                        int index24_6 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred35_Meteor()) ) {s = 12;}

                        else if ( (true) ) {s = 13;}

                         
                        input.seek(index24_6);
                        if ( s>=0 ) return s;
                        break;
                    case 6 : 
                        int LA24_7 = input.LA(1);

                         
                        int index24_7 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred35_Meteor()) ) {s = 12;}

                        else if ( (true) ) {s = 13;}

                         
                        input.seek(index24_7);
                        if ( s>=0 ) return s;
                        break;
                    case 7 : 
                        int LA24_8 = input.LA(1);

                         
                        int index24_8 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred35_Meteor()) ) {s = 12;}

                        else if ( (true) ) {s = 13;}

                         
                        input.seek(index24_8);
                        if ( s>=0 ) return s;
                        break;
                    case 8 : 
                        int LA24_9 = input.LA(1);

                         
                        int index24_9 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred35_Meteor()) ) {s = 12;}

                        else if ( (true) ) {s = 13;}

                         
                        input.seek(index24_9);
                        if ( s>=0 ) return s;
                        break;
                    case 9 : 
                        int LA24_10 = input.LA(1);

                         
                        int index24_10 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred35_Meteor()) ) {s = 12;}

                        else if ( (true) ) {s = 13;}

                         
                        input.seek(index24_10);
                        if ( s>=0 ) return s;
                        break;
                    case 10 : 
                        int LA24_11 = input.LA(1);

                         
                        int index24_11 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred35_Meteor()) ) {s = 12;}

                        else if ( (true) ) {s = 13;}

                         
                        input.seek(index24_11);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 24, _s, input);
            error(nvae);
            throw nvae;
        }
    }
    static final String DFA27_eotS =
        "\17\uffff";
    static final String DFA27_eofS =
        "\1\uffff\1\11\2\uffff\1\12\12\uffff";
    static final String DFA27_minS =
        "\2\6\2\uffff\1\6\2\uffff\1\0\3\uffff\1\6\1\43\1\uffff\1\0";
    static final String DFA27_maxS =
        "\1\77\1\100\2\uffff\1\100\2\uffff\1\0\3\uffff\1\77\1\100\1\uffff"+
        "\1\0";
    static final String DFA27_acceptS =
        "\2\uffff\1\2\1\3\1\uffff\1\7\1\10\1\uffff\1\1\1\5\1\4\2\uffff\1"+
        "\6\1\uffff";
    static final String DFA27_specialS =
        "\7\uffff\1\1\6\uffff\1\0}>";
    static final String[] DFA27_transitionS = {
            "\1\1\1\4\1\3\1\uffff\2\3\23\uffff\1\2\32\uffff\1\6\1\uffff\3"+
            "\3\1\5",
            "\1\11\2\uffff\1\11\20\uffff\1\11\1\uffff\1\11\2\uffff\1\10"+
            "\1\11\1\uffff\1\11\1\7\20\11\4\uffff\2\11\1\uffff\1\11\3\uffff"+
            "\2\11",
            "",
            "",
            "\1\12\2\uffff\1\12\20\uffff\1\12\1\uffff\1\12\3\uffff\1\12"+
            "\1\uffff\22\12\4\uffff\2\12\1\uffff\1\12\3\uffff\1\13\1\12",
            "",
            "",
            "\1\uffff",
            "",
            "",
            "",
            "\3\15\1\12\1\15\1\14\1\12\22\uffff\1\15\32\uffff\1\15\1\uffff"+
            "\4\15",
            "\1\12\25\uffff\1\15\5\uffff\1\15\1\16",
            "",
            "\1\uffff"
    };

    static final short[] DFA27_eot = DFA.unpackEncodedString(DFA27_eotS);
    static final short[] DFA27_eof = DFA.unpackEncodedString(DFA27_eofS);
    static final char[] DFA27_min = DFA.unpackEncodedStringToUnsignedChars(DFA27_minS);
    static final char[] DFA27_max = DFA.unpackEncodedStringToUnsignedChars(DFA27_maxS);
    static final short[] DFA27_accept = DFA.unpackEncodedString(DFA27_acceptS);
    static final short[] DFA27_special = DFA.unpackEncodedString(DFA27_specialS);
    static final short[][] DFA27_transition;

    static {
        int numStates = DFA27_transitionS.length;
        DFA27_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA27_transition[i] = DFA.unpackEncodedString(DFA27_transitionS[i]);
        }
    }

    class DFA27 extends DFA {

        public DFA27(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 27;
            this.eot = DFA27_eot;
            this.eof = DFA27_eof;
            this.min = DFA27_min;
            this.max = DFA27_max;
            this.accept = DFA27_accept;
            this.special = DFA27_special;
            this.transition = DFA27_transition;
        }
        public String getDescription() {
            return "174:1: valueExpression : ( methodCall[null] | parenthesesExpression | literal | VAR -> | (packageName= ID ':' )? constant= ID {...}? => -> | streamIndexAccess | arrayCreation | objectCreation );";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA27_14 = input.LA(1);

                         
                        int index27_14 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred41_Meteor()) ) {s = 10;}

                        else if ( (synpred44_Meteor()) ) {s = 13;}

                         
                        input.seek(index27_14);
                        if ( s>=0 ) return s;
                        break;
                    case 1 : 
                        int LA27_7 = input.LA(1);

                         
                        int index27_7 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred38_Meteor()) ) {s = 8;}

                        else if ( (synpred43_Meteor()) ) {s = 9;}

                         
                        input.seek(index27_7);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 27, _s, input);
            error(nvae);
            throw nvae;
        }
    }
    static final String DFA56_eotS =
        "\12\uffff";
    static final String DFA56_eofS =
        "\1\2\11\uffff";
    static final String DFA56_minS =
        "\1\6\1\0\10\uffff";
    static final String DFA56_maxS =
        "\1\100\1\0\10\uffff";
    static final String DFA56_acceptS =
        "\2\uffff\1\2\6\uffff\1\1";
    static final String DFA56_specialS =
        "\1\uffff\1\0\10\uffff}>";
    static final String[] DFA56_transitionS = {
            "\1\1\23\uffff\1\2\1\uffff\1\2\3\uffff\1\2\2\uffff\1\2\27\uffff"+
            "\1\2\4\uffff\1\2",
            "\1\uffff",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            ""
    };

    static final short[] DFA56_eot = DFA.unpackEncodedString(DFA56_eotS);
    static final short[] DFA56_eof = DFA.unpackEncodedString(DFA56_eofS);
    static final char[] DFA56_min = DFA.unpackEncodedStringToUnsignedChars(DFA56_minS);
    static final char[] DFA56_max = DFA.unpackEncodedStringToUnsignedChars(DFA56_maxS);
    static final short[] DFA56_accept = DFA.unpackEncodedString(DFA56_acceptS);
    static final short[] DFA56_special = DFA.unpackEncodedString(DFA56_specialS);
    static final short[][] DFA56_transition;

    static {
        int numStates = DFA56_transitionS.length;
        DFA56_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA56_transition[i] = DFA.unpackEncodedString(DFA56_transitionS[i]);
        }
    }

    class DFA56 extends DFA {

        public DFA56(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 56;
            this.eot = DFA56_eot;
            this.eof = DFA56_eof;
            this.min = DFA56_min;
            this.max = DFA56_max;
            this.accept = DFA56_accept;
            this.special = DFA56_special;
            this.transition = DFA56_transition;
        }
        public String getDescription() {
            return "312:1: (inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::numInputs - 1)] )?";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA56_1 = input.LA(1);

                         
                        int index56_1 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred81_Meteor()) ) {s = 9;}

                        else if ( (true) ) {s = 2;}

                         
                        input.seek(index56_1);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 56, _s, input);
            error(nvae);
            throw nvae;
        }
    }
 

    public static final BitSet FOLLOW_statement_in_script125 = new BitSet(new long[]{0x0000000004000000L});
    public static final BitSet FOLLOW_26_in_script127 = new BitSet(new long[]{0x00000000080000C2L,0x000000000000000AL});
    public static final BitSet FOLLOW_assignment_in_statement141 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_operator_in_statement145 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_packageImport_in_statement149 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_functionDefinition_in_statement153 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_javaudf_in_statement157 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_27_in_packageImport172 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_packageImport176 = new BitSet(new long[]{0x0000000010000000L});
    public static final BitSet FOLLOW_28_in_packageImport180 = new BitSet(new long[]{0x0000000000000042L});
    public static final BitSet FOLLOW_ID_in_packageImport191 = new BitSet(new long[]{0x0000000000000042L});
    public static final BitSet FOLLOW_VAR_in_assignment209 = new BitSet(new long[]{0x0000000020000000L});
    public static final BitSet FOLLOW_29_in_assignment211 = new BitSet(new long[]{0x0000000000000040L,0x000000000000000AL});
    public static final BitSet FOLLOW_operator_in_assignment215 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_functionDefinition237 = new BitSet(new long[]{0x0000000020000000L});
    public static final BitSet FOLLOW_29_in_functionDefinition239 = new BitSet(new long[]{0x0000000040000000L});
    public static final BitSet FOLLOW_30_in_functionDefinition241 = new BitSet(new long[]{0x0000000080000000L});
    public static final BitSet FOLLOW_31_in_functionDefinition243 = new BitSet(new long[]{0x0000000100000040L});
    public static final BitSet FOLLOW_ID_in_functionDefinition252 = new BitSet(new long[]{0x0000000110000000L});
    public static final BitSet FOLLOW_28_in_functionDefinition259 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_functionDefinition263 = new BitSet(new long[]{0x0000000110000000L});
    public static final BitSet FOLLOW_32_in_functionDefinition274 = new BitSet(new long[]{0xF4F0000080000DC0L,0x000000000000000AL});
    public static final BitSet FOLLOW_contextAwareExpression_in_functionDefinition286 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_javaudf307 = new BitSet(new long[]{0x0000000020000000L});
    public static final BitSet FOLLOW_29_in_javaudf309 = new BitSet(new long[]{0x0000000200000000L});
    public static final BitSet FOLLOW_33_in_javaudf311 = new BitSet(new long[]{0x0000000080000000L});
    public static final BitSet FOLLOW_31_in_javaudf313 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_STRING_in_javaudf317 = new BitSet(new long[]{0x0000000100000000L});
    public static final BitSet FOLLOW_32_in_javaudf319 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_expression_in_contextAwareExpression347 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ternaryExpression_in_expression357 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_operatorExpression_in_expression363 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_orExpression_in_ternaryExpression374 = new BitSet(new long[]{0x0000000400000000L});
    public static final BitSet FOLLOW_34_in_ternaryExpression377 = new BitSet(new long[]{0xF4F0000880000DC0L,0x000000000000000AL});
    public static final BitSet FOLLOW_expression_in_ternaryExpression381 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_35_in_ternaryExpression384 = new BitSet(new long[]{0xF4F0000080000DC0L,0x000000000000000AL});
    public static final BitSet FOLLOW_expression_in_ternaryExpression388 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_orExpression_in_ternaryExpression411 = new BitSet(new long[]{0x0000001000000000L});
    public static final BitSet FOLLOW_36_in_ternaryExpression413 = new BitSet(new long[]{0xF4F0000080000DC0L,0x000000000000000AL});
    public static final BitSet FOLLOW_expression_in_ternaryExpression417 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_orExpression_in_ternaryExpression439 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_andExpression_in_orExpression452 = new BitSet(new long[]{0x0000006000000002L});
    public static final BitSet FOLLOW_37_in_orExpression456 = new BitSet(new long[]{0xF4F0000080000DC0L});
    public static final BitSet FOLLOW_38_in_orExpression460 = new BitSet(new long[]{0xF4F0000080000DC0L});
    public static final BitSet FOLLOW_andExpression_in_orExpression465 = new BitSet(new long[]{0x0000006000000002L});
    public static final BitSet FOLLOW_elementExpression_in_andExpression494 = new BitSet(new long[]{0x0000018000000002L});
    public static final BitSet FOLLOW_39_in_andExpression498 = new BitSet(new long[]{0xF4F0000080000DC0L});
    public static final BitSet FOLLOW_40_in_andExpression502 = new BitSet(new long[]{0xF4F0000080000DC0L});
    public static final BitSet FOLLOW_elementExpression_in_andExpression507 = new BitSet(new long[]{0x0000018000000002L});
    public static final BitSet FOLLOW_comparisonExpression_in_elementExpression536 = new BitSet(new long[]{0x0000060000000002L});
    public static final BitSet FOLLOW_41_in_elementExpression541 = new BitSet(new long[]{0x0000040000000000L});
    public static final BitSet FOLLOW_42_in_elementExpression544 = new BitSet(new long[]{0xF4F0000080000DC0L});
    public static final BitSet FOLLOW_comparisonExpression_in_elementExpression548 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_arithmeticExpression_in_comparisonExpression589 = new BitSet(new long[]{0x0001F80000000002L});
    public static final BitSet FOLLOW_43_in_comparisonExpression595 = new BitSet(new long[]{0xF4F0000080000DC0L});
    public static final BitSet FOLLOW_44_in_comparisonExpression601 = new BitSet(new long[]{0xF4F0000080000DC0L});
    public static final BitSet FOLLOW_45_in_comparisonExpression607 = new BitSet(new long[]{0xF4F0000080000DC0L});
    public static final BitSet FOLLOW_46_in_comparisonExpression613 = new BitSet(new long[]{0xF4F0000080000DC0L});
    public static final BitSet FOLLOW_47_in_comparisonExpression619 = new BitSet(new long[]{0xF4F0000080000DC0L});
    public static final BitSet FOLLOW_48_in_comparisonExpression625 = new BitSet(new long[]{0xF4F0000080000DC0L});
    public static final BitSet FOLLOW_arithmeticExpression_in_comparisonExpression630 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_multiplicationExpression_in_arithmeticExpression710 = new BitSet(new long[]{0x0006000000000002L});
    public static final BitSet FOLLOW_49_in_arithmeticExpression716 = new BitSet(new long[]{0xF4F0000080000DC0L});
    public static final BitSet FOLLOW_50_in_arithmeticExpression722 = new BitSet(new long[]{0xF4F0000080000DC0L});
    public static final BitSet FOLLOW_multiplicationExpression_in_arithmeticExpression727 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_preincrementExpression_in_multiplicationExpression770 = new BitSet(new long[]{0x0008000000000202L});
    public static final BitSet FOLLOW_STAR_in_multiplicationExpression776 = new BitSet(new long[]{0xF4F0000080000DC0L});
    public static final BitSet FOLLOW_51_in_multiplicationExpression782 = new BitSet(new long[]{0xF4F0000080000DC0L});
    public static final BitSet FOLLOW_preincrementExpression_in_multiplicationExpression787 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_52_in_preincrementExpression828 = new BitSet(new long[]{0xF4F0000080000DC0L});
    public static final BitSet FOLLOW_preincrementExpression_in_preincrementExpression830 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_53_in_preincrementExpression835 = new BitSet(new long[]{0xF4F0000080000DC0L});
    public static final BitSet FOLLOW_preincrementExpression_in_preincrementExpression837 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_unaryExpression_in_preincrementExpression842 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_unaryExpression852 = new BitSet(new long[]{0xF4F0000080000DC0L});
    public static final BitSet FOLLOW_castExpression_in_unaryExpression861 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_31_in_castExpression871 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_castExpression875 = new BitSet(new long[]{0x0000000100000000L});
    public static final BitSet FOLLOW_32_in_castExpression877 = new BitSet(new long[]{0xF400000080000DC0L});
    public static final BitSet FOLLOW_generalPathExpression_in_castExpression881 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_generalPathExpression_in_castExpression888 = new BitSet(new long[]{0x0100000000000000L});
    public static final BitSet FOLLOW_56_in_castExpression890 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_castExpression894 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_generalPathExpression_in_castExpression901 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_valueExpression_in_generalPathExpression928 = new BitSet(new long[]{0x8200000000000000L});
    public static final BitSet FOLLOW_pathExpression_in_generalPathExpression932 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_valueExpression_in_generalPathExpression942 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_pathExpression_in_contextAwarePathExpression955 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_57_in_pathExpression988 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_pathExpression993 = new BitSet(new long[]{0x8200000000000002L});
    public static final BitSet FOLLOW_arrayAccess_in_pathExpression1011 = new BitSet(new long[]{0x8200000000000002L});
    public static final BitSet FOLLOW_methodCall_in_valueExpression1032 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_parenthesesExpression_in_valueExpression1038 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_literal_in_valueExpression1044 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_valueExpression1050 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_valueExpression1063 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_35_in_valueExpression1065 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_valueExpression1071 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_streamIndexAccess_in_valueExpression1089 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_arrayCreation_in_valueExpression1094 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_objectCreation_in_valueExpression1100 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_operator_in_operatorExpression1113 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_31_in_parenthesesExpression1134 = new BitSet(new long[]{0xF4F0000080000DC0L,0x000000000000000AL});
    public static final BitSet FOLLOW_expression_in_parenthesesExpression1136 = new BitSet(new long[]{0x0000000100000000L});
    public static final BitSet FOLLOW_32_in_parenthesesExpression1138 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_methodCall1162 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_35_in_methodCall1164 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_methodCall1170 = new BitSet(new long[]{0x0000000080000000L});
    public static final BitSet FOLLOW_31_in_methodCall1172 = new BitSet(new long[]{0xF4F0000180000DC0L,0x000000000000000AL});
    public static final BitSet FOLLOW_expression_in_methodCall1179 = new BitSet(new long[]{0x0000000110000000L});
    public static final BitSet FOLLOW_28_in_methodCall1185 = new BitSet(new long[]{0xF4F0000080000DC0L,0x000000000000000AL});
    public static final BitSet FOLLOW_expression_in_methodCall1189 = new BitSet(new long[]{0x0000000110000000L});
    public static final BitSet FOLLOW_32_in_methodCall1199 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_fieldAssignment1213 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_35_in_fieldAssignment1215 = new BitSet(new long[]{0xF4F0000080000DC0L,0x000000000000000AL});
    public static final BitSet FOLLOW_expression_in_fieldAssignment1217 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_fieldAssignment1232 = new BitSet(new long[]{0x8200000020000000L});
    public static final BitSet FOLLOW_57_in_fieldAssignment1241 = new BitSet(new long[]{0x0000000000000200L});
    public static final BitSet FOLLOW_STAR_in_fieldAssignment1243 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_29_in_fieldAssignment1257 = new BitSet(new long[]{0x0000000000000040L,0x000000000000000AL});
    public static final BitSet FOLLOW_operator_in_fieldAssignment1261 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_contextAwarePathExpression_in_fieldAssignment1276 = new BitSet(new long[]{0x0000000800000002L});
    public static final BitSet FOLLOW_35_in_fieldAssignment1287 = new BitSet(new long[]{0xF4F0000080000DC0L,0x000000000000000AL});
    public static final BitSet FOLLOW_expression_in_fieldAssignment1291 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_58_in_objectCreation1343 = new BitSet(new long[]{0x08000000000000C0L});
    public static final BitSet FOLLOW_fieldAssignment_in_objectCreation1346 = new BitSet(new long[]{0x0800000010000000L});
    public static final BitSet FOLLOW_28_in_objectCreation1349 = new BitSet(new long[]{0x00000000000000C0L});
    public static final BitSet FOLLOW_fieldAssignment_in_objectCreation1351 = new BitSet(new long[]{0x0800000010000000L});
    public static final BitSet FOLLOW_28_in_objectCreation1355 = new BitSet(new long[]{0x0800000000000000L});
    public static final BitSet FOLLOW_59_in_objectCreation1360 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_60_in_literal1380 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_61_in_literal1396 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_DECIMAL_in_literal1412 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_STRING_in_literal1428 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_INTEGER_in_literal1445 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_62_in_literal1460 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_63_in_arrayAccess1474 = new BitSet(new long[]{0x0000000000000200L});
    public static final BitSet FOLLOW_STAR_in_arrayAccess1476 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_64_in_arrayAccess1478 = new BitSet(new long[]{0x8200000000000000L});
    public static final BitSet FOLLOW_pathExpression_in_arrayAccess1482 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_63_in_arrayAccess1502 = new BitSet(new long[]{0x0000000000001800L});
    public static final BitSet FOLLOW_INTEGER_in_arrayAccess1507 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_UINT_in_arrayAccess1513 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_64_in_arrayAccess1516 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_63_in_arrayAccess1534 = new BitSet(new long[]{0x0000000000001800L});
    public static final BitSet FOLLOW_INTEGER_in_arrayAccess1539 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_UINT_in_arrayAccess1545 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_35_in_arrayAccess1548 = new BitSet(new long[]{0x0000000000001800L});
    public static final BitSet FOLLOW_INTEGER_in_arrayAccess1553 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_UINT_in_arrayAccess1559 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_64_in_arrayAccess1562 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_streamIndexAccess1590 = new BitSet(new long[]{0x8000000000000000L});
    public static final BitSet FOLLOW_63_in_streamIndexAccess1599 = new BitSet(new long[]{0xF400000080000DC0L});
    public static final BitSet FOLLOW_generalPathExpression_in_streamIndexAccess1603 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_64_in_streamIndexAccess1605 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_63_in_arrayCreation1624 = new BitSet(new long[]{0xF4F0000080000DC0L,0x000000000000000AL});
    public static final BitSet FOLLOW_expression_in_arrayCreation1628 = new BitSet(new long[]{0x0000000010000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_28_in_arrayCreation1631 = new BitSet(new long[]{0xF4F0000080000DC0L,0x000000000000000AL});
    public static final BitSet FOLLOW_expression_in_arrayCreation1635 = new BitSet(new long[]{0x0000000010000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_28_in_arrayCreation1639 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_64_in_arrayCreation1642 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_readOperator_in_operator1679 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_writeOperator_in_operator1683 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_genericOperator_in_operator1687 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_65_in_readOperator1701 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000004L});
    public static final BitSet FOLLOW_66_in_readOperator1703 = new BitSet(new long[]{0x0000000000000140L});
    public static final BitSet FOLLOW_ID_in_readOperator1708 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_STRING_in_readOperator1713 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_readOperator1719 = new BitSet(new long[]{0x0000000080000000L});
    public static final BitSet FOLLOW_31_in_readOperator1721 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_STRING_in_readOperator1725 = new BitSet(new long[]{0x0000000100000000L});
    public static final BitSet FOLLOW_32_in_readOperator1727 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_67_in_writeOperator1741 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_VAR_in_writeOperator1745 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000010L});
    public static final BitSet FOLLOW_68_in_writeOperator1747 = new BitSet(new long[]{0x0000000000000140L});
    public static final BitSet FOLLOW_ID_in_writeOperator1752 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_STRING_in_writeOperator1757 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_writeOperator1763 = new BitSet(new long[]{0x0000000080000000L});
    public static final BitSet FOLLOW_31_in_writeOperator1765 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_STRING_in_writeOperator1769 = new BitSet(new long[]{0x0000000100000000L});
    public static final BitSet FOLLOW_32_in_writeOperator1771 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_genericOperator1792 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_35_in_genericOperator1794 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_genericOperator1800 = new BitSet(new long[]{0x80000000000000C0L,0x0000000000000020L});
    public static final BitSet FOLLOW_operatorFlag_in_genericOperator1808 = new BitSet(new long[]{0x80000000000000C0L,0x0000000000000020L});
    public static final BitSet FOLLOW_arrayInput_in_genericOperator1812 = new BitSet(new long[]{0x0000000000000042L});
    public static final BitSet FOLLOW_input_in_genericOperator1816 = new BitSet(new long[]{0x0000000010000042L});
    public static final BitSet FOLLOW_28_in_genericOperator1819 = new BitSet(new long[]{0x80000000000000C0L,0x0000000000000020L});
    public static final BitSet FOLLOW_input_in_genericOperator1821 = new BitSet(new long[]{0x0000000010000042L});
    public static final BitSet FOLLOW_operatorOption_in_genericOperator1827 = new BitSet(new long[]{0x0000000000000042L});
    public static final BitSet FOLLOW_ID_in_operatorOption1847 = new BitSet(new long[]{0xF4F0000080000DC0L,0x000000000000000AL});
    public static final BitSet FOLLOW_contextAwareExpression_in_operatorOption1853 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_operatorFlag1874 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_69_in_input1896 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_VAR_in_input1904 = new BitSet(new long[]{0x0000040000000000L});
    public static final BitSet FOLLOW_42_in_input1906 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_VAR_in_input1912 = new BitSet(new long[]{0x0000000000000042L});
    public static final BitSet FOLLOW_ID_in_input1922 = new BitSet(new long[]{0xF4F0000080000DC0L,0x000000000000000AL});
    public static final BitSet FOLLOW_contextAwareExpression_in_input1930 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_63_in_arrayInput1952 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_VAR_in_arrayInput1956 = new BitSet(new long[]{0x0000000010000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_28_in_arrayInput1959 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_VAR_in_arrayInput1963 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_64_in_arrayInput1967 = new BitSet(new long[]{0x0000040000000000L});
    public static final BitSet FOLLOW_42_in_arrayInput1969 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_VAR_in_arrayInput1973 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ternaryExpression_in_synpred9_Meteor357 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_orExpression_in_synpred11_Meteor374 = new BitSet(new long[]{0x0000000400000000L});
    public static final BitSet FOLLOW_34_in_synpred11_Meteor377 = new BitSet(new long[]{0xF4F0000880000DC0L,0x000000000000000AL});
    public static final BitSet FOLLOW_expression_in_synpred11_Meteor381 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_35_in_synpred11_Meteor384 = new BitSet(new long[]{0xF4F0000080000DC0L,0x000000000000000AL});
    public static final BitSet FOLLOW_expression_in_synpred11_Meteor388 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_orExpression_in_synpred12_Meteor411 = new BitSet(new long[]{0x0000001000000000L});
    public static final BitSet FOLLOW_36_in_synpred12_Meteor413 = new BitSet(new long[]{0xF4F0000080000DC0L,0x000000000000000AL});
    public static final BitSet FOLLOW_expression_in_synpred12_Meteor417 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_31_in_synpred33_Meteor871 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_synpred33_Meteor875 = new BitSet(new long[]{0x0000000100000000L});
    public static final BitSet FOLLOW_32_in_synpred33_Meteor877 = new BitSet(new long[]{0xF400000080000DC0L});
    public static final BitSet FOLLOW_generalPathExpression_in_synpred33_Meteor881 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_generalPathExpression_in_synpred34_Meteor888 = new BitSet(new long[]{0x0100000000000000L});
    public static final BitSet FOLLOW_56_in_synpred34_Meteor890 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_synpred34_Meteor894 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_valueExpression_in_synpred35_Meteor928 = new BitSet(new long[]{0x8200000000000000L});
    public static final BitSet FOLLOW_pathExpression_in_synpred35_Meteor932 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_57_in_synpred36_Meteor988 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_synpred36_Meteor993 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_arrayAccess_in_synpred37_Meteor1011 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_methodCall_in_synpred38_Meteor1032 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_synpred41_Meteor1050 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_synpred42_Meteor1063 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_35_in_synpred42_Meteor1065 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_synpred43_Meteor1063 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_35_in_synpred43_Meteor1065 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_synpred43_Meteor1071 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_streamIndexAccess_in_synpred44_Meteor1089 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_28_in_synpred77_Meteor1819 = new BitSet(new long[]{0x80000000000000C0L,0x0000000000000020L});
    public static final BitSet FOLLOW_input_in_synpred77_Meteor1821 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_operatorOption_in_synpred78_Meteor1827 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_synpred81_Meteor1922 = new BitSet(new long[]{0xF4F0000080000DC0L,0x000000000000000AL});
    public static final BitSet FOLLOW_contextAwareExpression_in_synpred81_Meteor1930 = new BitSet(new long[]{0x0000000000000002L});

}