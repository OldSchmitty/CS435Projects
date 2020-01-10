import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.hash.Hash;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class Job3 {
    public static class Job3Mapper extends Mapper< Object, Text, Text, Text > {
        private Text word = new Text();
        private Text docIDTF = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (!value.toString().isEmpty()) {
                String[] strArray = value.toString().split("\\s+");
                String docID = strArray[0];
                String sWord = strArray[1];
                String TF = strArray[2];
                word.set(sWord);
                docIDTF.set(docID + "\t" + TF);
                context.write(word, docIDTF);
            }
        }
    }

    public static class Job3Reducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            HashMap<String, String> map = new HashMap<String, String>();
            int count = 0;
            for(Text val : values){
                String[] strArray = val.toString().split("\\s+");
                String docID = strArray[0];
                String TF = strArray[1];
                if (!map.containsKey(docID)){
                    map.put(docID, TF);
                    count ++;
                }
            }
            for(String docID : map.keySet()){
                Text docIDOut = new Text(docID);
                Text valOut = new Text(key.toString() + "\t"+ map.get(docID) + "\t" + count);
                context.write(docIDOut, valOut);
            }
        }
    }
}
