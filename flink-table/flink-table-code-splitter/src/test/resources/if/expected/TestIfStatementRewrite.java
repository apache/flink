public class TestIfStatementRewrite {
    public void myFun(int[] a, int[] b) throws RuntimeException {
        if (a[0] == 0) {
myFun_trueFilter1(a, b);

}
 else {
myFun_falseFilter2(a, b);

}

    }
void myFun_trueFilter1(int[] a, int[] b) throws RuntimeException{
            a[0] = 1;
            if (a[1] == 0) {
myFun_trueFilter1_trueFilter3(a, b);

}
 else {
myFun_trueFilter1_falseFilter4(a, b);

}

        }
void myFun_trueFilter1_trueFilter3(int[] a, int[] b) throws RuntimeException{
                a[1] = 1;
                if (a[2] == 0) {
                    a[2] = 1;
                } else {
                    a[2] = b[2];
                }
            }


void myFun_trueFilter1_falseFilter4(int[] a, int[] b) throws RuntimeException{
                a[1] = b[1];
                a[2] = b[2];
            }




void myFun_falseFilter2(int[] a, int[] b) throws RuntimeException{
            a[0] = b[0];
            a[1] = b[1];
            a[2] = b[2];
        }


}
