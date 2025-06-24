public class TestRewriteInnerClass {
    public void myFun(int[] a, int[] b) {

        if (a[0] == 0) {
            myFun_0_0(a, b);
        } else {
            myFun_0_1(a, b);
        }
    }

    void myFun_0_1(int[] a, int[] b) {
        a[0] += b[1];
        a[1] += b[0];
    }

    void myFun_0_0(int[] a, int[] b) {
        a[0] += b[0];
        a[1] += b[1];
    }


    public class InnerClass {
        public void myFun(int[] a, int[] b) {

            if (a[0] == 0) {
                myFun_0_0(a, b);
            } else {
                myFun_0_1(a, b);
            }
        }

        void myFun_0_1(int[] a, int[] b) {
            a[0] += b[1];
            a[1] += b[0];
        }

        void myFun_0_0(int[] a, int[] b) {
            a[0] += b[0];
            a[1] += b[1];
        }

    }
}
