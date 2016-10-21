package flink.gelly.school;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;


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
 */
public class TemporalEdgev3<K, V> extends Edge<K, Tuple3<V,Integer,Integer>> {

    /*
    Creates new temporaledge with only null values
     */
    public TemporalEdgev3() {}

    /*
    * Constructor to make a temporal edge version 2, has 5 input values but makes a
    * typle 3 which is compatible with Gelly
    * */
    public TemporalEdgev3(K src, K trg, V val, Integer start, Integer end) {
        this.f0 = src;
        this.f1 = trg;
        this.f2 = new Tuple3<V,Integer,Integer>(val,start,end);
    }

    public TemporalEdgev3(K src, K trg, V val) {
        this.f0 = src;
        this.f1 = trg;
        this.f2 = new Tuple3<V,Integer,Integer>(val, null, null);
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

    public void seteValue(V value) {
        this.f2.setField(value,1);
    }

    public V geteValue() {
        return f2.getField(1);
    }

    public void setStarttime(Integer start) { this.f2.setField(start,2); }

    public Integer getStarttime() { return f2.getField(2); }

    public void setEndtime(Integer end) { this.f2.setField(end,2); }

    public Integer getEndtime() { return f2.getField(3); }

}
