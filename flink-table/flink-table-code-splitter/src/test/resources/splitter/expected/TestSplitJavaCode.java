public class TestSplitJavaCode {
int[][] rewrite$8 = new int[1][];
int[] rewrite$9 = new int[5];

{
rewrite$9[4] = 1;
}






    
    

    public TestSplitJavaCode(int[] b) {
        this.rewrite$8[0] = b;
    }

    public void myFun(int a) {
        myFun_split2(a);

        myFun_split3(a);

        myFun_split4(a);

        myFun_split5(a);

         
         
         
        

        
    }
void myFun_split2(int a) {

this.rewrite$9[4] = a;
rewrite$8[0][0] += rewrite$8[0][1];
rewrite$8[0][1] += rewrite$8[0][2];
rewrite$8[0][2] += rewrite$8[0][3];
rewrite$9[0] = rewrite$8[0][0] + rewrite$8[0][1];
rewrite$9[1] = rewrite$8[0][1] + rewrite$8[0][2];
}

void myFun_split3(int a) {
rewrite$9[2] = rewrite$8[0][2] + rewrite$8[0][0];
}

void myFun_split4(int a) {
for (int x : rewrite$8[0]) {rewrite$9[3] = x;
            System.out.println(rewrite$9[3] + rewrite$9[0] + rewrite$9[1] + rewrite$9[2]);
        }
}

void myFun_split5(int a) {
if (rewrite$9[0] > 0) {
myFun_trueFilter2(a);

}
 else {
            rewrite$8[0][0] += 50;
        }
}

void myFun_trueFilter2(int a){
            myFun_trueFilter2_split6(a);

            myFun_trueFilter2_split7(a);

        }
void myFun_trueFilter2_split6(int a) {

rewrite$8[0][0] += 100;
}

void myFun_trueFilter2_split7(int a) {
if (rewrite$9[1] > 0) {
                rewrite$8[0][1] += 100;
                if (rewrite$9[2] > 0) {
                    rewrite$8[0][2] += 100;
                } else {
                    rewrite$8[0][2] += 50;
                }
            } else {
                rewrite$8[0][1] += 50;
            }
}



}
