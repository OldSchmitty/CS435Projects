import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;


public class Driver2 {
    public static void main( String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Job 5");
        job.setJarByClass(Job5.class);
        //job.setMapperClass(Job5.Job5SentenceMapper.class);
        job.setNumReduceTasks(10);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        Path[] pathArr = new Path[1];
        pathArr[0]= new Path(args[1]);
        //FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        FileSystem fs = FileSystem.get(job.getConfiguration());
        Path srcPath = new Path(args[1]);
        Path dstPath = new Path(args[1]+"/TFIDF");
        if (!(fs.exists(dstPath))) {
            //FileUtil.copyMerge(fs, srcPath, fs, dstPath, false, job.getConfiguration(), null);
            copyMerge(fs, srcPath, fs, dstPath, false, job.getConfiguration(), null);
        }
        //DistributedCache.addCacheFile(new URI(args[1]+"/TFIDF"), job.getConfiguration());
        MultipleInputs.addInputPath(job, dstPath, TextInputFormat.class, Job5.Job5TFIDFMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Job5.Job5SentenceMapper.class);
        job.setReducerClass(Job5.Job5Reducer.class);
        job.waitForCompletion(true);


    }

    /* The Following is the orginal implementation for the copymerge functionality in Hadoop 2.7. It was deprecated in
        version 3, not allowing my code to compile on some machines. The code was taken from https://github.com/c9n/hadoop
     */

    public static boolean copyMerge(FileSystem srcFS, Path srcDir,
                                    FileSystem dstFS, Path dstFile,
                                    boolean deleteSource,
                                    Configuration conf, String addString) throws IOException {
        dstFile = checkDest(srcDir.getName(), dstFS, dstFile, false);

        if (!srcFS.getFileStatus(srcDir).isDirectory())
            return false;

        OutputStream out = dstFS.create(dstFile);

        try {
            FileStatus contents[] = srcFS.listStatus(srcDir);
            Arrays.sort(contents);
            for (int i = 0; i < contents.length; i++) {
                if (contents[i].isFile()) {
                    InputStream in = srcFS.open(contents[i].getPath());
                    try {
                        IOUtils.copyBytes(in, out, conf, false);
                        if (addString!=null)
                            out.write(addString.getBytes("UTF-8"));

                    } finally {
                        in.close();
                    }
                }
            }
        } finally {
            out.close();
        }


        if (deleteSource) {
            return srcFS.delete(srcDir, true);
        } else {
            return true;
        }
    }

    private static Path checkDest(String srcName, FileSystem dstFS, Path dst,
                                  boolean overwrite) throws IOException {
        if (dstFS.exists(dst)) {
            FileStatus sdst = dstFS.getFileStatus(dst);
            if (sdst.isDirectory()) {
                if (null == srcName) {
                    throw new IOException("Target " + dst + " is a directory");
                }
                return checkDest(null, dstFS, new Path(dst, srcName), overwrite);
            } else if (!overwrite) {
                throw new IOException("Target " + dst + " already exists");
            }
        }
        return dst;
    }


}
