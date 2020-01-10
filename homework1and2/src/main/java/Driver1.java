import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver1 {
    public static void main(String[] args) throws Exception{
        if(job1(args)) {
            Counter counter = job2(args);
            if(counter.getValue() != 0) {
                if(job3(args)) {
                    if(job4(args, counter)){
                        System.exit(0);
                    }
                }
            }
        }
        System.exit(1);
    }

    private static boolean job1(String[] args)throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Job1 phase 1");
        job.setJarByClass(Job1.class);
        job.setMapperClass(Job1.WordCountMapper.class);
        job.setCombinerClass(Job1.DistinctWordReducer.class);
        job.setReducerClass(Job1.DistinctWordReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(10);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]+"/Job1Output"));
        return job.waitForCompletion(true);
    }

    private static Counter job2(String[] args)throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Job 2");
        job.setJarByClass(Job2.class);
        job.setMapperClass(Job2.DocIDMapper.class);
        job.setReducerClass(Job2.DocIDWordReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(10);
        FileInputFormat.addInputPath(job, new Path(args[1] + "/Job1Output"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]+"/Job2Output"));
        boolean status = job.waitForCompletion(true);
        Counter counter = job.getCounters().findCounter(DocIDCounter.COUNTERS.DOC_ID_COUNTER);
        if (!status){
            counter.setValue(0);
        }
        return counter;
    }

    private static boolean job3(String[] args)throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Job 3");
        job.setJarByClass(Job3.class);
        job.setMapperClass(Job3.Job3Mapper.class);
        job.setReducerClass(Job3.Job3Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(10);
        FileInputFormat.addInputPath(job, new Path(args[1] + "/Job2Output"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]+"/Job3Output"));
        return job.waitForCompletion(true);
    }

    private static boolean job4(String[] args, Counter counter) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Job 3");
        job.setJarByClass(Job4.class);
        job.setMapperClass(Job4.Job4Mapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[1] + "/Job3Output"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]+"/Job4Output"));
        job.getConfiguration().setLong(DocIDCounter.COUNTERS.DOC_ID_COUNTER.name(), counter.getValue());
        return job.waitForCompletion(true);
    }


}
