import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.StringTokenizer;
import java.util.TreeMap;

public class Job1 {
    public static class WordCountMapper extends Mapper< Object, Text, Text, IntWritable > {
        private Text word = new Text();
        private IntWritable one = new IntWritable(1);
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (!value.toString().isEmpty()) {
                String[] strArray = value.toString().split("<====>");
                String docID = strArray[1];
                StringTokenizer token = new StringTokenizer(strArray[2]);
                while (token.hasMoreTokens()) {
                    String out = token.nextToken().replaceAll("[^A-Za-z0-9]", "").toLowerCase();
                    out.replaceAll("\\s+","");
                    if (!out.equals("")) {
                        word.set(docID+'\t'+out);
                        context.write(word, one);
                    }
                }
            }
        }
    }

    public static class DistinctWordReducer extends Reducer<Text,
            IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

}