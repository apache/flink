public class TestSplitFunction {
boolean codeSplitHasReturned$0;
    public void myFun(int[] a, int[] b) throws RuntimeException {codeSplitHasReturned$0 = false;
        myFun_split1(a, b);
if (codeSplitHasReturned$0) { return; }

        myFun_split2(a, b);

        myFun_split3(a, b);
if (codeSplitHasReturned$0) { return; }

        myFun_split4(a, b);

        
        
    }
void myFun_split1(int[] a, int[] b) throws RuntimeException {
if (a[0] != 0) {
            a[0] += b[0];
            b[0] += a[1];
            { codeSplitHasReturned$0 = true; return; }
        }
}

void myFun_split2(int[] a, int[] b) throws RuntimeException {
a[1] += b[1];
b[1] += a[2];
}

void myFun_split3(int[] a, int[] b) throws RuntimeException {
if (a[2] != 0) {
            a[2] += b[2];
            b[2] += a[3];
            { codeSplitHasReturned$0 = true; return; }
        }
}

void myFun_split4(int[] a, int[] b) throws RuntimeException {
a[3] += b[3];
b[3] += a[4];
}

}
