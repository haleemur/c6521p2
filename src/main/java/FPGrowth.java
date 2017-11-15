import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.*;
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class FPGrowth {


    public static class Map
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void setup(Context context)
            throws IOException {
            FileSystem fs = FileSystem.get(context.getConfiguration());
            RemoteIterator it = fs.listFiles(new Path("temp"), false);
        }
    }
}
