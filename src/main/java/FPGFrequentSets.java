import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FPGFrequentSets {
    public static class FPGSetsMap
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text input, Context context)
                throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            String sourceName = fileName.replaceFirst("-m-\\d+$", "");
            context.write(new Text(sourceName), input);
        }
    }

    public static class FPGSetsReduce
            extends Reducer<Text, Text, Text, NullWritable> {

        private MultipleOutputs<Text, NullWritable> mos;
        public void setup(Context context) {
            mos = new MultipleOutputs<>(context);
        }

        public void reduce(Text fileName, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {

            FileSystem fs = FileSystem.get(context.getConfiguration());
            List<String> headerString = FPUtility.readHeader(fs, "temp1", fileName.toString());
            int[] head = new int[headerString.size()];
            for (int i=0;i<head.length;i++) {
                head[i] = Integer.parseInt(headerString.get(i));
            }

            FPTree tree = new FPTree(10);
            tree.init(head);
            String[] fields;
            ArrayList<Integer> symbols;
            for (Text line: values) {
                fields = line.toString().split("\t");
                if (fields[0].equals("minsup")) {
                    tree.setSupport(Integer.parseInt(fields[1]));
                } else {
                    String[] pathString = fields[0].split(",");
                    symbols = new ArrayList<>();
                    for (String el:pathString) {
                        symbols.add(Integer.parseInt(el));
                    }
                    tree.addPath(symbols, Integer.parseInt(fields[1]));
                }
            }
//            mos.write(new Text("hello"), NullWritable.get(), fileName.toString());
            tree.writeSets(true, fileName.toString(), mos);
        }
    }
}
