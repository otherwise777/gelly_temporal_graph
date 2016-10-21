package flink.gelly.school;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;


/**
 * Created by Wouter Ligtenberg
 *
 * this edge is basicaly the edge value of an edge
 *
 * @param <V> the edge value type
 */
public class EdgeTime<V, N> extends Tuple3<V,N,N> {

    /*
    Creates new temporaledge with only null values
     */
    public EdgeTime() {}

    /*
    * Constructor to make a temporal edge version 2, has 5 input values but makes a
    * typle 3 which is compatible with Gelly
    * */
    public EdgeTime(V val, N start, N end) {
        this.f0 = val;
        this.f1 = start;
        this.f2 = end;
    }
    /**
     * Reverses the direction of this Edge.
     * @return a new Edge, where the source is the original Edge's target
     * and the target is the original Edge's source.
     */
//    public Edge<K, V> reverse() {
//        return new Edge<K, V>(this.f1, this.f0, this.f2);
//    }


    public void seteValue(V value) {
        this.f0 = value;
    }

    public V geteValue() {
        return f0;
    }

    public void setStarttime(N start) { this.f1 = start; }

    public N getStarttime() { return f1; }

    public void setEndtime(N end) { this.f2 = end; }

    public N getEndtime() { return f2; }

}
