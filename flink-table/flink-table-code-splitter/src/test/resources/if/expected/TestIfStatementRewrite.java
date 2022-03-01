public class TestIfStatementRewrite {
    public void myFun1(int[] a, int[] b) throws RuntimeException {
        if (a[0] == 0) {
myFun1_trueFilter1(a, b);

}
 else {
myFun1_falseFilter2(a, b);

}

    }
void myFun1_trueFilter1(int[] a, int[] b) throws RuntimeException{
            a[0] = 1;
            if (a[1] == 0) {
myFun1_trueFilter1_trueFilter4(a, b);

}
 else {
myFun1_trueFilter1_falseFilter5(a, b);

}

        }
void myFun1_trueFilter1_trueFilter4(int[] a, int[] b) throws RuntimeException{
                a[1] = 1;
                if (a[2] == 0) {
                    a[2] = 1;
                } else {
                    a[2] = b[2];
                }
            }


void myFun1_trueFilter1_falseFilter5(int[] a, int[] b) throws RuntimeException{
                a[1] = b[1];
                a[2] = b[2];
            }




void myFun1_falseFilter2(int[] a, int[] b) throws RuntimeException{
            a[0] = b[0];
            a[1] = b[1];
            a[2] = b[2];
        }



    public void myFun2(int[] a, int[] b) throws RuntimeException {
        if (a[0] == 0) {
            a[0] = 1;
            if (a[1] == 0) {
                a[1] = 1;
                if (a[2] == 0) {
                    a[2] = 1;
                } else {
                    a[2] = b[2];
                }
            } else {
                a[1] = b[1];
                a[2] = b[2];
                return;
            }
        } else {
myFun2_falseFilter3(a, b);

}

    }
void myFun2_falseFilter3(int[] a, int[] b) throws RuntimeException{
            a[0] = b[0];
            a[1] = b[1];
            a[2] = b[2];
        }


}
