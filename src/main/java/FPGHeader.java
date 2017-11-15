import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class FPGHeader {

    public static class Map
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final IntWritable one = new IntWritable(1);
        private Text item = new Text();

        public void map(LongWritable key, Text input, Context context)
            throws IOException, InterruptedException {

            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();

            String line = input.toString().replace("\n", "").replace(" ", "");


            if (key.get() == 0 && !line.contains(",")) {
                context.write(
                        new Text(fileName+":minsup"),
                        new IntWritable(Integer.parseInt(line))
                );
            } else {
                String[] tokens = line.split(",");
                for (int i=1;i<tokens.length;i++) {
                    item.set(fileName+":"+tokens[i]);
                    context.write(item, one);
                }
            }
        }
    }

    public static class Reduce
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private MultipleOutputs<Text, IntWritable> mos;
        public void setup(Context context) {
            mos = new MultipleOutputs<Text, IntWritable>(context);
        }

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {

            String[] keySplit = key.toString().split(":");
            int sum = 0;
            for(IntWritable val: values) {
                sum += val.get();
            }
//            context.write(key, new IntWritable(sum));
            mos.write(new Text(keySplit[1]), new IntWritable(sum), keySplit[0]);
        }
    }

    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance();
        job.setJobName("build-header");
        job.setJarByClass(FPGHeader.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("temp"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
