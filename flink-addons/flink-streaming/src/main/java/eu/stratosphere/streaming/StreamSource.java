package eu.stratosphere.streaming;



import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.core.io.StringRecord;
import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractInputTask;

 
public class StreamSource extends AbstractInputTask<RandIS> {

     private RecordWriter<IOReadableWritable> output;
     
     @Override
     public RandIS[] computeInputSplits(int requestedMinNumber)
         throws Exception {
       // TODO Auto-generated method stub
       return null;
     }

     @Override
     public Class<RandIS> getInputSplitType() {
       // TODO Auto-generated method stub
       return null;
     }

     @Override
     public void registerInputOutput() {
       
       Class<? extends ChannelSelector<IOReadableWritable>> MyPartitioner = getTaskConfiguration().getClass("partitioner",DefaultPartitioner.class, ChannelSelector.class);
       try {
         ChannelSelector<IOReadableWritable> myPartitioner = MyPartitioner.newInstance();
         output = new RecordWriter<IOReadableWritable>(this, IOReadableWritable.class, myPartitioner);

       }
       catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
       }
     }

     @Override
     public void invoke() throws Exception {
       
       
       for(int i=0; i<10; i++) {
         //output.emit(new StringRecord(rnd.nextInt(10)+" "+rnd.nextInt(1000)));
         output.emit(new StringRecord("5 500"));
         output.emit(new StringRecord("4 500"));

       }
       
     }

   }

  
