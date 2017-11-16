import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.*;
import java.util.*;


public class FPTree {
    private int[] elements;
    private int[] header;
    private int support;
    private FPNode root;
    private Map<Integer, FPNode> lastNodes;
    private Deque<Integer> suffix;

    public Deque<Integer> getSuffix() {
        return suffix;
    }

    public int size() {
        return header.length;
    }

    public void addSuffix(int s) {
        suffix.push(s);
    }

    public int getElementCount() {
        return elements.length;
    }


    public int[] copyHeader(int to) {
        return Arrays.copyOf(header, to);
    }

    public void addPath(List<Integer> path, int count) {
        root.addChildren(path, lastNodes, count);
    }


    FPTree(int elementCount) {
        elements = new int[elementCount];
        suffix = new ArrayDeque<>();

    }

    FPTree(int elementCount, int support, Deque<Integer> suffix) {
        // build the initial elements;
        elements = new int[elementCount];
        this.support = support;
        this.suffix = new ArrayDeque<>(suffix);
    }

    public void init(int[] head) {
        header = head;
        lastNodes = new HashMap<>();
        for (int entry: header) {
            lastNodes.put(entry, null);
        }
        root = new FPNode();
    }


    public void buildTree(List<Integer> path, int count) {
        root.addChildren(path, lastNodes, count);
    }


    public int getSumCounts(int e) {
        FPNode node = lastNodes.get(e);
        if (node == null)
            return 0;

        int sum = node.getCount();
        while ((node = node.next()) != null) {
            sum += node.getCount();
        }
        return sum;
    }

    public void writeSets(boolean all_nsets, String outName, MultipleOutputs<Text, NullWritable> writer)
            throws IOException, InterruptedException {
        int e;
        for (int i = header.length - 1; i >= 0; i--) {
            e = header[i];
            (new FPWriteItemsets(writer, outName, e, this, all_nsets)).run();
        }
    }

    public void writeSets(boolean all_nsets, String outName, BufferedWriter writer)
            throws IOException, InterruptedException {
        int e;
        for (int i = header.length - 1; i >= 0; i--) {
            e = header[i];
            (new FPWriteItemsets(writer, outName, e, this, all_nsets)).run();
        }
    }

    public FPNode getLastNodes(Integer c) {
        return lastNodes.get(c);
    }

    public int getHeaderIndexOf(int ns) {
        for (int i=header.length-1; i>=0; i--) {
            if (header[i] == ns) return i;
        }
        return -1;
    }

    public int getSupport() {
        return support;
    }

    public FPNode getRoot() {
        return root;
    }

    public void removeLowSupport() {
        // needs to be redone
        int e, sum;
        int next = 0;
        FPNode node;
        int[] tempHeader = new int[header.length];
        for (int i = header.length - 1; i >= 0; i--) {
            e = header[i];
            sum = getSumCounts(e);
            if (sum < support) {
                node = lastNodes.get(e);
                if (node != null) {
                    FPNode parent = node.parent();
                    for (FPNode child: node.getChildren()) {
                        child.setParent(parent);
                    }
                }
                lastNodes.remove(e);
            }
            else {
                tempHeader[next++] = e;
            }
        }
        header = tempHeader;
    }

    public int getHeaderElement(int i) {
        return header[i];
    }
    public Set<Integer> getNodeKeys() {
        return lastNodes.keySet();
    }

    public void setSupport(int support) {
        this.support = support;
    }
}
