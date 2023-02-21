public class TestAddBooleanBeforeReturn {
    public void fun1(int a) {
        if (a > 0) {
            a += 5;
            return;
        }
        a -= 5;
        return;
    }

    public void fun2(String b) {
        if (b.length() > 5) {
            b += "b";
            return;
        }
        b += "a";
        return;
    }
}
