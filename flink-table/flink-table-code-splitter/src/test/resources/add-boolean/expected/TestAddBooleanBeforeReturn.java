public class TestAddBooleanBeforeReturn {
boolean fun2HasReturned$1;
boolean fun1HasReturned$0;
    public void fun1(int a) {
        if (a > 0) {
            a += 5;
            { fun1HasReturned$0 = true; return; }
        }
        a -= 5;
        { fun1HasReturned$0 = true; return; }
    }

    public void fun2(String b) {
        if (b.length() > 5) {
            b += "b";
            { fun2HasReturned$1 = true; return; }
        }
        b += "a";
        { fun2HasReturned$1 = true; return; }
    }
}
