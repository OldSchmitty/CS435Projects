import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.TreeMap;


public class Job2 {

    public static class DocIDMapper extends Mapper< Object, Text, Text, Text > {
        private Text out = new Text();
        private Text docId = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (!value.toString().isEmpty()) {
                String[] strArray = value.toString().split("\\s+");
                String docID = strArray[0];
                String word = strArray[1];
                String count = strArray[2];
                out.set(word + "\t" + count);
                docId.set(docID);
                context.write(docId, out);
            }
        }
    }

    public static class DocIDWordReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            context.getCounter(DocIDCounter.COUNTERS.DOC_ID_COUNTER).increment(1);
            HashMap<String,Integer> map = new HashMap<String, Integer>();
            int max = 0;
            for(Text val : values){
                String[] strArray = val.toString().split("\\s+");
                int currentCount = Integer.parseInt(strArray[1]);
                if (currentCount > max){
                    max = currentCount;
                }
                map.put(strArray[0], currentCount);
            }

           for(String mapKey : map.keySet()){
                double currentCount = map.get(mapKey);
                double TF = .5+.5*(currentCount/max);
                Text outKey = new Text(key.toString()+"\t"+mapKey);
                context.write(outKey, new DoubleWritable(TF));
           }

        }
    }
}
