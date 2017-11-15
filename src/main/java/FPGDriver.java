import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FPGDriver {
    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException {
        Path input = new Path(args[0]);
        Path temp = new Path("temp");

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJobName("build-header");
        job.setJarByClass(FPGDriver.class);
        job.setMapperClass(FPGHeader.Map.class);
        job.setCombinerClass(FPGHeader.Reduce.class);
        job.setReducerClass(FPGHeader.Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, temp);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
