import java.util.*;

public class FPNode {
    public Integer getSymbol() {
        return symbol;
    }

    public int getCount() {
        return count;
    }

    private Integer symbol;
    private int count;
    private FPNode parent = null;
    private FPNode next;
    private Map<Integer, FPNode> children;

    public void next(FPNode next) {
        this.next = next;
    }

    public FPNode next() {
        return next;
    }

    public FPNode parent() {
        return parent;
    }

    FPNode() { children = new HashMap<>();}

    FPNode(Integer symbol, FPNode parent, Map<Integer, FPNode> nextNodes) {
        this(symbol, parent, nextNodes, 1);

    }
    FPNode(Integer symbol, FPNode parent, Map<Integer, FPNode> nextNodes, int count) {
        this.symbol = symbol;
        this.count = count;
        this.parent = parent;
        this.next = nextNodes.get(symbol);
        nextNodes.put(symbol, this);
        children = new HashMap<>();

    }

    public void increment (int n) {
        count += n;
    }

    public FPNode addChild(Integer symbol, Map<Integer, FPNode> nextNodes) {
        if (children.get(symbol) == null) {
            children.put(symbol, new FPNode(symbol, this, nextNodes));
        } else {
            children.get(symbol).increment(1);
        }

        return children.get(symbol);
    }

    public FPNode addChild(Integer symbol, Map<Integer, FPNode> nextNodes, int count) {

        if (children.get(symbol) == null) {
            children.put(symbol, new FPNode(symbol, this, nextNodes, count));
        } else {
            children.get(symbol).increment(count);
        }

        return children.get(symbol);
    }

    public void addChildren(int[] symbols, int length, Map<Integer, FPNode> nextNodes) {
        int i=0;
        FPNode child;
        child = addChild(symbols[i++], nextNodes);
        while(i < length) {
            child  = child.addChild(symbols[i++], nextNodes);
        }
    }

    public void addChildren(List<Integer> symbols, Map<Integer, FPNode> nextNodes, int count) {
        int i=0;
        FPNode child;
        child = addChild(symbols.get(i++), nextNodes, count);
        while(i < symbols.size()) {
            child  = child.addChild(symbols.get(i++), nextNodes, count);
        }
    }

    public Collection<FPNode> getChildren() {
        return children.values();
    }

    public void setParent(FPNode parent) {
        this.parent = parent;
    }

    public void print() {
        List<Integer> path = new ArrayList<>();
        FPNode node = this;
        while(node.parent != null) {
            path.add(node.symbol);
            node = node.parent;
        }
        if (path.size() > 0) {
            for (int i=path.size()-1; i>=0; i--) {
                System.out.print(i + " ");
            }
            System.out.println("\tcount:" + node.count);
        }
    }
}
