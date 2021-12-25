public class TestNotRewrite {
    public void fun1(int a) {
        if (a > 0) {
            a += 5;
            return;
        }
        a -= 5;
        return;
    }

    public int fun2(int a) {
        a += 1;
        return a;
    }

    public String fun3() {
        return "aVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryLongString";
    }
}
