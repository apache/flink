public class TestSplitJavaCode {
    boolean[] rewrite$16 = new boolean[2];
    int[][] rewrite$17 = new int[1][];
    int[] rewrite$18 = new int[6];

    {
        rewrite$18[5] = 1;
    }













    public TestSplitJavaCode(int[] b) {
        this.rewrite$17[0] = b;
    }

    public void myFun1(int a) {
        rewrite$16[0] = false;
        myFun1_split6(a);

        myFun1_split7(a);

        myFun1_split8(a);

        myFun1_split9(a);
        if (rewrite$16[0]) { return; }








    }
    void myFun1_split6(int a) {

        this.rewrite$18[5] = a;
        rewrite$17[0][0] += rewrite$17[0][1];
        rewrite$17[0][1] += rewrite$17[0][2];
        rewrite$17[0][2] += rewrite$17[0][3];
        rewrite$18[0] = rewrite$17[0][0] + rewrite$17[0][1];
        rewrite$18[1] = rewrite$17[0][1] + rewrite$17[0][2];
    }

    void myFun1_split7(int a) {
        rewrite$18[2] = rewrite$17[0][2] + rewrite$17[0][0];
    }

    void myFun1_split8(int a) {
        for (int x : rewrite$17[0]) {rewrite$18[3] = x;
            System.out.println(rewrite$18[3] + rewrite$18[0] + rewrite$18[1] + rewrite$18[2]);
        }
    }

    void myFun1_split9(int a) {
        if (rewrite$18[0] > 0) {
            myFun1_0_0_rewriteGroup5(a);
        } else {
            rewrite$17[0][0] += 50;
            rewrite$17[0][1] += 100;
            rewrite$17[0][2] += 150;
            rewrite$17[0][3] += 200;
            rewrite$17[0][4] += 250;
            rewrite$17[0][5] += 300;
            rewrite$17[0][6] += 350;
            { rewrite$16[0] = true; return; }
        }
    }


    void myFun1_0_0_rewriteGroup5(int a) {
        myFun1_0_0_rewriteGroup5_split10(a);

        myFun1_0_0_rewriteGroup5_split11(a);

    }
    void myFun1_0_0_rewriteGroup5_split10(int a) {

        rewrite$17[0][0] += 100;
    }

    void myFun1_0_0_rewriteGroup5_split11(int a) {
        if (rewrite$18[1] > 0) {
            myFun1_0_0_rewriteGroup1_2_rewriteGroup4(a);
        } else {
            rewrite$17[0][1] += 50;
        }
    }


    void myFun1_0_0_rewriteGroup1_2_rewriteGroup4(int a) {
        myFun1_0_0_rewriteGroup1_2_rewriteGroup4_split12(a);

        myFun1_0_0_rewriteGroup1_2_rewriteGroup4_split13(a);

    }
    void myFun1_0_0_rewriteGroup1_2_rewriteGroup4_split12(int a) {

        rewrite$17[0][1] += 100;
    }

    void myFun1_0_0_rewriteGroup1_2_rewriteGroup4_split13(int a) {
        if (rewrite$18[2] > 0) {
            rewrite$17[0][2] += 100;
        } else {
            rewrite$17[0][2] += 50;
        }
    }



    public int myFun2(int[] a) { myFun2Impl(a); return rewrite$18[4]; }

    void myFun2Impl(int[] a) {
        rewrite$16[1] = false;
        myFun2Impl_split14(a);

        myFun2Impl_split15(a);
        if (rewrite$16[1]) { return; }





    }
    void myFun2Impl_split14(int[] a) {

        a[0] += 1;
        a[1] += 2;
        a[2] += 3;
        a[3] += 4;
        a[4] += 5;
    }

    void myFun2Impl_split15(int[] a) {
        { rewrite$18[4] = a[0] + a[1] + a[2] + a[3] + a[4]; { rewrite$16[1] = true; return; } }
    }

}
