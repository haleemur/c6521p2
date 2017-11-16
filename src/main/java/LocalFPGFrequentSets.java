//This file is used for testing the algorithm locally

import org.apache.commons.lang.ArrayUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LocalFPGFrequentSets {
    public static void main(String[] args)
            throws InterruptedException, IOException {
        FPTree tree = new FPTree(10);
        String line;
        String[] pathStr;
        String[] fields;
        Integer minsup = 0;
        HashMap<Integer, Integer> itemSupport = new HashMap<>();
        ArrayList<Integer> header = new ArrayList<>();
        BufferedReader hd = new BufferedReader(new FileReader("input.txt.head"));
        while ((line = hd.readLine()) != null) {
            fields = line.split("\t");
            if (fields[0].equals("minsup")) {
                minsup = Integer.parseInt(fields[1]);
            } else {
                itemSupport.put(Integer.parseInt(fields[0]), Integer.parseInt(fields[1]));
            }

        }


        for (Map.Entry<Integer, Integer> e: itemSupport.entrySet()) {
            if (e.getValue() >= minsup) {
                header.add(e.getKey());
            }
        }
        header.sort((p1, p2)->itemSupport.get(p2).compareTo(itemSupport.get(p1)));
        for (int h: header) {
            System.out.println(h);
        }

        tree.setSupport(minsup);
        int[] head = new int[header.size()];
        for (int z =0; z<head.length; z++) {
            head[z] = header.get(z);
        }
        tree.init(head);
        BufferedReader br = new BufferedReader(new FileReader("input.txt.path"));
        while ((line = br.readLine()) != null) {

            fields = line.split("\t");
            if (!fields[0].equals("minsup")) {
                pathStr = fields[0].split(",");
                List<Integer> symbols = new ArrayList<>();
                for (int i=0;i<pathStr.length;i++) {
                    symbols.add(Integer.parseInt(pathStr[i]));
                }
                tree.buildTree(symbols, Integer.parseInt(fields[1]));
            }
        }
        BufferedWriter bw = new BufferedWriter(new FileWriter("input.txt.sets"));
        tree.writeSets(true, "localfile", bw);
        bw.close();
    }
}
