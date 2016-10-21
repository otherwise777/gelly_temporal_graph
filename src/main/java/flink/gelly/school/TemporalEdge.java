package flink.gelly.school;
import org.apache.flink.api.java.tuple.Tuple5;


/**
 * Created by Wouter Ligtenberg
 *
 * A temporal edge represents a link between two vertices
 * This class should've extended a normal edge in Gelly that disabled the
 * use of a tuple5 class in the dataset API. That's why most functions were
 * simply copied from the Edge class from Gelly
 *
 * @param <K> the key type for the sources and target vertices
 * @param <V> the edge value type
 * @param <N> The numeric type for the time values
 */
public class TemporalEdge<K, V, N> extends Tuple5<K, K, V, N, N> {

    /*
    Creates new temporaledge with only null values
     */
    public TemporalEdge() {}



    public TemporalEdge(K src, K trg, V val, N start, N end) {
        this.f0 = src;
        this.f1 = trg;
        this.f2 = val;
        this.f3 = start;
        this.f4 = end;
    }

    /**
     * Reverses the direction of this Edge.
     * @return a new Edge, where the source is the original Edge's target
     * and the target is the original Edge's source.
     */
//    public Edge<K, V> reverse() {
//        return new Edge<K, V>(this.f1, this.f0, this.f2);
//    }

    public void setSource(K src) {
        this.f0 = src;
    }

    public K getSource() {
        return this.f0;
    }

    public void setTarget(K target) {
        this.f1 = target;
    }

    public K getTarget() {
        return f1;
    }

    public void setValue(V value) {
        this.f2 = value;
    }

    public V getValue() {
        return f2;
    }

    public void setStarttime(N start) { this.f3 = start; }

    public N getStarttime() { return f3; }

    public void setEndtime(N start) { this.f4 = start; }

    public N getEndtime() { return f4; }

}
