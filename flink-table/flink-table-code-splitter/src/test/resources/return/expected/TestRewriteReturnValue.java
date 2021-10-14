public class TestRewriteReturnValue {
String fun1ReturnValue$0;
String fun2ReturnValue$1;

    public String fun1(int a, String b) { fun1Impl(a, b); return fun1ReturnValue$0; }

void fun1Impl(int a, String b) {
        if (a > 0) {
            a += 5;
            { fun1ReturnValue$0 = b + "test" + a; return; }
        }
        a -= 5;
        { fun1ReturnValue$0 = b + "test" + a; return; }
    }

    public String fun2(int a, String b) throws Exception { fun2Impl(a, b); return fun2ReturnValue$1; }

void fun2Impl(int a, String b) throws Exception {
        if (a > 0) {
            a += 5;
            if (a > 100) {
                throw new RuntimeException();
            }
            { fun2ReturnValue$1 = b + "test" + a; return; }
        }
        a -= 5;
        { fun2ReturnValue$1 = b + "test" + a; return; }
    }
}
