import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class Job4 {
    public static class Job4Mapper extends Mapper< Object, Text, Text, Text > {
        private long N;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            this.N  = context.getConfiguration().getLong(DocIDCounter.COUNTERS.DOC_ID_COUNTER.name(), 0);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (!value.toString().isEmpty()) {
                String[] strArray = value.toString().split("\\s+");
                String docID = strArray[0];
                String word = strArray[1];
                double TF = Double.parseDouble(strArray[2]);
                int n = Integer.parseInt(strArray[3]);
                Text outKey = new Text(docID);
                double IDF = Math.log10(N/n);
                double TFIDF = TF * IDF;
                Text outVal = new Text(word + "\t" + TFIDF);
                context.write(outKey, outVal);
            }
        }
    }
}
