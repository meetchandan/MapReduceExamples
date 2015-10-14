package CommonFriends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class CFDriver {
  public static void  main(String []args) throws IOException, ClassNotFoundException, InterruptedException{
    if(args.length !=2){
        System.out.printf("Usage: CFDriver <input dir> <output dir>\n");
        System.exit(-1);
    }
    Job job = Job.getInstance(new Configuration());
    job.setMapperClass(CFMapper.class);
    job.setReducerClass(CFReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setJobName("CommonFriends");
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setJarByClass(CFDriver.class);
    boolean success = job.waitForCompletion(true);
    System.exit(success ? 0:1);
  }
}
