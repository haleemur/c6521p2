import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;


public class FPWriteItemsets {


    private Integer ns;
    private FPTree tree;
    private boolean all_nsets;
    private MultipleOutputs<Text, NullWritable> remoteWriter;
    private BufferedWriter localWriter;
    private String outName;

    FPWriteItemsets(MultipleOutputs<Text, NullWritable> remoteWriter, String outName, Integer ns, FPTree tree, boolean all_nsets) throws IOException {
        this.ns = ns;
        this.tree = tree;
        this.all_nsets = all_nsets;
        this.remoteWriter = remoteWriter;
        this.outName = outName;
    }

    FPWriteItemsets(BufferedWriter localWriter, String outName, Integer ns, FPTree tree, boolean all_nsets) throws IOException {
        this.ns = ns;
        this.tree = tree;
        this.all_nsets = all_nsets;
        this.localWriter = localWriter;
        this.outName = outName;
    }

    private FPTree buildConditionalPrefixTree(Integer ns, FPTree oldTree) {
        ArrayList<Integer> rkBuilder;
        FPNode parent, node, root;
        node = oldTree.getLastNodes(ns);
        root = oldTree.getRoot();
        int newHeaderSize = oldTree.getHeaderIndexOf(ns);
        FPTree tree = new FPTree(oldTree.getElementCount(), oldTree.getSupport(), oldTree.getSuffix());
        tree.init(oldTree.copyHeader(newHeaderSize));

        tree.addSuffix(ns);
        do {
            parent = node.parent();
            if (parent == oldTree.getRoot()) continue;
            rkBuilder = new ArrayList<>();
            rkBuilder.add(parent.getSymbol());
            while ((parent = parent.parent()) != root) {
                rkBuilder.add(0, parent.getSymbol());
            }
            tree.buildTree(rkBuilder, node.getCount());
        } while ((node = node.next()) != null);
//        tree.removeLowSupport();
        return tree;
    }

    private FPTree conditionalPrefix(Integer ns, FPTree subTree)
            throws IOException, InterruptedException {

        StringBuilder builder;
        FPTree prefixTree = buildConditionalPrefixTree(ns, subTree);
        for (Integer prefix : prefixTree.getNodeKeys()) {
            builder = new StringBuilder("{");
            builder.append(prefix);
            for (Integer k : prefixTree.getSuffix()) {
                builder.append(",");
                builder.append(k);
            }
            builder.append("}");
            if (remoteWriter != null) {
                remoteWriter.write(new Text(builder.toString()), NullWritable.get(), outName);
            } else {
                localWriter.write(builder.toString());
                localWriter.newLine();
            }
        }
        return prefixTree;
    }

    private void conditionalPrefix(Integer ns, FPTree subTree, boolean recursive)
            throws IOException, InterruptedException {
        FPTree prefixTree = conditionalPrefix(ns, subTree);
        if (recursive && prefixTree.size() > 1) {
            Integer c;
            for (int i = prefixTree.size()-1; i > 0; i--) {
                c = prefixTree.getHeaderElement(i);
                conditionalPrefix(c, prefixTree);
            }
        }
    }


    public void run() throws InterruptedException {
        try {
            conditionalPrefix(ns, tree, all_nsets);

        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
}
