public class TestNotRewrite {
    public void fun1(int[] a, int[] b) {
        a[0] += b[1];
        b[1] += a[1];
        a[1] += b[2];
        b[2] += a[3];
        a[3] += b[4];
    }

    public void fun2() {
        return;
    }

    public void fun3(int a) {
        a += 5;
        return;
    }
}
