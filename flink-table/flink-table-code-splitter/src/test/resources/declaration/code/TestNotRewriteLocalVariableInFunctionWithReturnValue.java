public class TestNotRewriteLocalVariableInFunctionWithReturnValue {
    public int myFun() {
        int local1;
        String local2 = "local2";
        final long local3;
        local3 = 100L;
    }
}
