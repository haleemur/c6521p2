/**
 * Author: Haleemur Ali
 * Email: haleemur@gmail.com
 * Github: github.com/haleemur
 * StackOverflow: https://stackoverflow.com/users/2570261/haleemur-ali
 *
 * This is a straight forward implementation of Frequent Pair Mining
 * Apriori Algorithm MapFGPRecords Reduce Variant.
 *
 * The algorithm is implemented using Hadoop MapFGPRecords Reduce.
 *
 * In the map stage, for every pair of attributes from a transaction
 * record is mapped out with a count of 1.
 *
 * In the reduce stage, attributes are grouped, their counts summed
 * and if the count exceeds the minimum support, written out to the
 * output file.
 *
 * The combiner class is the same as the reducer class.
 *
 * This program accepts two arguments
 * 1. input_directory
 * 2. output_directory
 *
 * For each transaction file in the input directory, the program will
 * generate an output file of the same name + a suffix given by hadoop
 * which will contain all the frequent pairs.
 */


import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Apriori {

    /**
     * The map operation emits each encountered item-pairs with a count of 1.
     */
    public static class Map
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final IntWritable ONE = new IntWritable(1);
        private Text item = new Text();

        public void map(LongWritable key, Text input, Context context)
                throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();


            String line = input.toString().replace("\n", "").replace(" ", "");

            // key.get() == 0 && !line.contains(",") for the first line of the file
            // which contains the header. The if-branch skips over the first line.
            if (key.get() != 0 || line.contains(",")) {

                // split the line into tokens & skip the first token
                // which represents the line number.
                // sort the tokens from 2nd position onwards
                // sorting ensures that the pair 1,2
                String[] tokens = line.split(",");
                Arrays.sort(tokens,1,tokens.length);

                for (int i=1;i<tokens.length-1;i++) {

                    for (int j=i+1;j<tokens.length;j++) {

                        // set the key to be of the format filename:item1,item2
                        // this way the reducers can identify which source file
                        // the record belongs to.
                        item.set(fileName + ":" + tokens[i] + "," + tokens[j]);
                        context.write(item, ONE);
                    }
                }
            }
        }
    }

    /**
     * The reduce operation aggregates the count per observed item pair.
     * pairs that are observed more times than the minimum support
     * threshold are written to disk.
     */

    public static class Reduce
            extends Reducer<Text, IntWritable, Text, NullWritable> {

        private MultipleOutputs<Text, NullWritable> moz;
        public void setup(Context context) {
            moz = new MultipleOutputs<Text, NullWritable>(context);
        }

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {


            String[] keySplit = key.toString().split(":");

            String minsupString = context.getConfiguration().get(keySplit[0] + ".minsup");
            Integer minsup = Integer.parseInt(minsupString);

            int sum = 0;
            for(IntWritable val: values) {
                sum += val.get();
            }

            // output just the keys in the same format as programming assignment part 1.
            if (sum >= minsup) {
                moz.write(new Text("{"+keySplit[1]+"}"), NullWritable.get(), keySplit[0]);
            }
        }
    }

    public static void setMinSupport(Path inputDir, Configuration conf)
            throws IOException {

        FileSystem fs = FileSystem.get(conf);
        RemoteIterator it = fs.listFiles(inputDir, false);
        String line;

        while (it.hasNext()) {
            FileStatus f = (FileStatus)it.next();
            BufferedReader br= new BufferedReader(new InputStreamReader(fs.open(f.getPath())));
            line = br.readLine().replace("\n", "").replace(" ", "");
            conf.set(f.getPath().getName() + ".minsup", line);

            br.close();
        }
        fs.close();

    }

    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException {
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        Configuration conf = new Configuration();
        setMinSupport(input, conf);


        Job job = Job.getInstance(conf);
        job.setJobName("apriori");
        job.setJarByClass(Apriori.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}