import java.util.*;

public class HashRing {
    
    private final SortedMap<Long, String> ring = new TreeMap<>();
    private final int virtualNodes;
    private static final int DEFAULT_VIRTUAL_NODES = 160; // 160 virtual nodes
    
    public HashRing(int virtualNodes) {
        this.virtualNodes = virtualNodes;
    }
    
    public HashRing() {
        this(DEFAULT_VIRTUAL_NODES);
    }
    
    public void addNode(String nodeName) {
        for (int i = 0; i < virtualNodes; i++) {
            long hash = hash(nodeName + ":" + i);
            ring.put(hash, nodeName);
        }
    }
    
    public void removeNode(String nodeName) {
        for (int i = 0; i < virtualNodes; i++) {
            long hash = hash(nodeName + ":" + i);
            ring.remove(hash);
        }
    }
    
    public String getNode(String subscriptionKey) {
        if (ring.isEmpty()) {
            throw new IllegalStateException("Hash ring is empty. Add at least one node.");
        }
        
        long hash = hash(subscriptionKey);
        
        // Find the first node with hash >= subscription hash
        SortedMap<Long, String> tailMap = ring.tailMap(hash);
        
        if (tailMap.isEmpty()) {
            // Wrap around to the first node in the ring
            return ring.firstEntry().getValue();
        }
        
        return tailMap.firstEntry().getValue();
    }
    
    private long hash(String key) {
        long hash = 14695981039346656037L; // FNV offset basis
        for (byte b : key.getBytes()) {
            hash ^= b;
            hash *= 1099511628211L; // FNV prime
        }
        return Math.abs(hash);
    }
    
    public Set<String> getNodes() {
        return new HashSet<>(ring.values());
    }
    
    public int getNodeCount() {
        return getNodes().size();
    }
    
    public boolean containsNode(String nodeName) {
        return getNodes().contains(nodeName);
    }
    
    public Map<String, Integer> getDistribution() {
        Map<String, Integer> distribution = new HashMap<>();
        for (String node : getNodes()) {
            distribution.put(node, 0);
        }
        for (String node : ring.values()) {
            distribution.put(node, distribution.get(node) + 1);
        }
        return distribution;
    }
}
