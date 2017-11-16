import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


public class FPGRecords {


    public static class MapFGPRecords
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Map<String, Integer> itemSupport;
        private List<String> header;
        public void setup(Context context)
            throws IOException {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();

            FileSystem fs = FileSystem.get(context.getConfiguration());
            header = FPUtility.readHeader(fs, "temp1", fileName);
        }

        public void map(LongWritable key, Text input, Context context)
            throws IOException, InterruptedException {

            String line = input.toString();
            String[] tokens = line.split(",");
            ArrayList<String> outputKey = new ArrayList<>();


            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();

            if (key.get() == 0 && !line.contains(",")) {
                context.write(
                        new Text(fileName + ":minsup"),
                        new IntWritable(Integer.parseInt(tokens[0])));
            } else {

                for (String k: header) {
                    for (int i=1;i<tokens.length;i++) {
                        if (k.equals(tokens[i])) {
                            outputKey.add(k);
                        }
                    }
                }
                if (outputKey.size() > 1) {
                    StringBuilder builder = new StringBuilder(fileName + ":");
                    for (String k : outputKey) {
                        builder.append(k);
                        builder.append(',');
                    }
                    builder.deleteCharAt(builder.length()-1);
                    context.write(new Text(builder.toString()), new IntWritable(1));
                }

            }
        }
    }

    public static class ReduceFPGRecords
        extends Reducer<Text, IntWritable, Text, IntWritable> {

        private MultipleOutputs<Text, IntWritable> mos;
        public void setup(Context context) {
            mos = new MultipleOutputs<>(context);
        }
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            String[] keySplit = key.toString().split(":");
            int sum = 0;
            for(IntWritable val: values) {
                sum += val.get();
            }
//            context.write(key, new IntWritable(sum));
            mos.write(new Text(keySplit[1]), new IntWritable(sum), keySplit[0]);
        }
    }
}
