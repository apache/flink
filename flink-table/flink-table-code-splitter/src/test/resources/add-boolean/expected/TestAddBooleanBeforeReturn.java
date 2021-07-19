public class TestAddBooleanBeforeReturn {
boolean codeSplitHasReturned$1;
boolean codeSplitHasReturned$0;
    public void fun1(int a) {
        if (a > 0) {
            a += 5;
            { codeSplitHasReturned$0 = true; return; }
        }
        a -= 5;
        { codeSplitHasReturned$0 = true; return; }
    }

    public void fun2(String b) {
        if (b.length() > 5) {
            b += "b";
            { codeSplitHasReturned$1 = true; return; }
        }
        b += "a";
        { codeSplitHasReturned$1 = true; return; }
    }
}
