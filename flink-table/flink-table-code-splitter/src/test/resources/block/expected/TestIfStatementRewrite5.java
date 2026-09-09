public class TestIfStatementRewrite5 {
    public void myFun1(int[] a, int[] b) throws RuntimeException {

        if (a[0] == 0) {
            myFun1_0_0(a, b);
        } else {
            a[13] = b[12];
        }
    }

    void myFun1_0_0(int[] a, int[] b) throws RuntimeException {
        a[0] = 1;
        for (int i = 0; i < a[3]; i++) {
            a[0] += b[i];
            if (a[0] > 999) {
                break;
            }
        }
    }

}
