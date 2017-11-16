import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FPGDriver {
    private static Job getJobHeader(Configuration conf, Path output, Path input)
            throws IOException {

        Job job = Job.getInstance(conf);
        job.setJobName("step-header");
        job.setJarByClass(FPGDriver.class);
        job.setMapperClass(FPGHeader.Map.class);
        job.setCombinerClass(FPGHeader.Reduce.class);
        job.setReducerClass(FPGHeader.Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        return job;
    }

    private static Job getJobRecords(Configuration conf, Path output, Path input)
            throws IOException {

        Job job = Job.getInstance(conf);
        job.setJobName("step-records");
        job.setJarByClass(FPGRecords.class);
        job.setMapperClass(FPGRecords.MapFGPRecords.class);
        job.setCombinerClass(FPGRecords.ReduceFPGRecords.class);
        job.setReducerClass(FPGRecords.ReduceFPGRecords.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        return job;
    }

    private static Job getJobFPGFrequentSets(Configuration conf, Path output, Path input)
            throws IOException {

        Job job = Job.getInstance(conf);
        job.setJobName("step-frequentsets");
        job.setJarByClass(FPGFrequentSets.class);
        job.setMapperClass(FPGFrequentSets.FPGSetsMap.class);
        job.setCombinerClass(FPGFrequentSets.FPGSetsReduce.class);
        job.setReducerClass(FPGFrequentSets.FPGSetsReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        return job;
    }

    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException {
        Path input = new Path(args[0]);
        Path temp1 = new Path("temp1");
        Path temp2 = new Path("temp2");
        Path output = new Path(args[1]);

        Configuration conf = new Configuration();

        Job jobFPGHeader = getJobHeader(conf, temp1, input);

        jobFPGHeader.waitForCompletion(true);
        Job jobFPGRecords = getJobRecords(conf, temp2, input);

        jobFPGRecords.waitForCompletion(true);
        Job jobFPGFrequentSets = getJobFPGFrequentSets(conf, output, temp2);

        System.exit(jobFPGFrequentSets.waitForCompletion(true) ? 0 : 1);
    }
}
