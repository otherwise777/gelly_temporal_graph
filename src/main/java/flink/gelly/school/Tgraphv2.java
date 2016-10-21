package flink.gelly.school;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

/**
 * Created by s133781 on 18-Oct-16.
 */
public class Tgraphv2<K,EV,N>{

    private final ExecutionEnvironment context;
    private final Graph<K,NullValue,Tuple3<EV,N,N>> graph;

    /*
    * Constructor that creates the temporal graph from the Tuple5 set
    * */
    public Tgraphv2(DataSet<Tuple5<K,K,EV,N,N>> TupleSet, ExecutionEnvironment context) throws Exception {
        DataSet<Edge<K,Tuple3<EV,N,N>>> edges = TupleSet.map(new MapFunction<Tuple5<K, K, EV, N, N>, Edge<K, Tuple3<EV, N, N>>>() {
            @Override
            public Edge<K, Tuple3<EV, N, N>> map(Tuple5<K, K, EV, N, N> value) throws Exception {
                return new Edge<>(value.f0, value.f1, new Tuple3<>(value.f2, value.f3, value.f4));
            }
        });

        Graph<K,NullValue,Tuple3<EV,N,N>> temporalgraph = Graph.fromDataSet(edges,context);

        this.graph = temporalgraph;
        this.context = context;
    }
    /**
     * @return the edge DataSet.
     */
    public DataSet<Edge<K, EV>> getEdges() {
        DataSet<Edge<K, EV>> newedges = graph.getEdges().map(new MapFunction<Edge<K, Tuple3<EV, N, N>>, Edge<K, EV>>() {
            @Override
            public Edge<K, EV> map(Edge<K, Tuple3<EV, N, N>> value) throws Exception {
                return new Edge<>(value.f0,value.f1,value.f2.getField(0));
            }
        });
        return newedges;
    }
}
