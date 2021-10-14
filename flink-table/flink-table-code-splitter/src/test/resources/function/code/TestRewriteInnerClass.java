public class TestRewriteInnerClass {
    public void myFun(int[] a, int[] b) throws RuntimeException {
        if (a[0] != 0) {
            a[0] += b[0];
            b[0] += a[1];
            return;
        }
        a[1] += b[1];
        b[1] += a[2];
        if (a[2] != 0) {
            a[2] += b[2];
            b[2] += a[3];
            return;
        }
        a[3] += b[3];
        b[3] += a[4];
    }

    public class InnerClass {
        public void myFun(int[] a, int[] b) throws RuntimeException {
            if (a[0] != 0) {
                a[0] += b[0];
                b[0] += a[1];
                return;
            }
            a[1] += b[1];
            b[1] += a[2];
            if (a[2] != 0) {
                a[2] += b[2];
                b[2] += a[3];
                return;
            }
            a[3] += b[3];
            b[3] += a[4];
        }
    }
}
