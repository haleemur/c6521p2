import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FPUtility {
    public static List<String> readHeader(FileSystem fs, String dirName, String fileName )
        throws IOException {
        String filePath =  (dirName.endsWith("/"))? dirName + fileName + "-m-00000" :dirName + "/" + fileName + "-m-00000";

        fs.open(new Path(filePath));

        BufferedReader br = new BufferedReader(
                new InputStreamReader(
                        fs.open(new Path("temp1/" + fileName + "-m-00000"))
                )
        );

        Map<String, Integer> itemSupport = new HashMap<>();
        String line;
        String[] tokens;
        int minsup = 0;

        while ((line = br.readLine()) != null) {
            tokens = line.replace("\n", "").split("\t");
            if (tokens[0].equals("minsup")) {
                minsup = Integer.parseInt(tokens[1]);
            } else {
                itemSupport.put(tokens[0], Integer.parseInt(tokens[1]));
            }
        }

        List<String> header = new ArrayList<>();
        for (Map.Entry<String, Integer> e: itemSupport.entrySet()) {
            if (e.getValue() >= minsup) {
                header.add(e.getKey());
            }
        }

        header.sort((p1, p2)->itemSupport.get(p2).compareTo(itemSupport.get(p1)));
        return header;
    }
}
