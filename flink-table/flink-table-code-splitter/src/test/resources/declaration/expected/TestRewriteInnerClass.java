public class TestRewriteInnerClass {
int local1;
String local2;
long local3;

    public void myFun() {
        
         local2 = "AAAAA";
        
        local3 = 100;
    }

    public class InnerClass {
int local$0;
long local$1;
String local$2;

        public void myFun() {
            
            local$0 = 5;
            
              local$2 = "BBBBB";
        }
    }
}
