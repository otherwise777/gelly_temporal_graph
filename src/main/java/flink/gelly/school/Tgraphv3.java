package flink.gelly.school;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.pregel.MessageIterator;
import org.apache.flink.types.NullValue;

/**
 * Created by s133781 on 18-Oct-16.
 */
public class Tgraphv3<K,VV,EV,N> {

    private final ExecutionEnvironment context;
    private final DataSet<Edge<K,Tuple3<EV,N,N>>> edges;
    private final DataSet<Vertex<K, VV>> vertices;
    /*
    * Constructor that creates the temporal graph from the Tuple5 set
    * */
    public Tgraphv3(DataSet<Vertex<K, VV>> vertices, DataSet<Edge<K,Tuple3<EV,N,N>>> edges, ExecutionEnvironment context) throws Exception {
        this.vertices = vertices;
        this.edges = edges;
        this.context = context;
    }


    /*
    * Transforms a tuple5 dataset with (source node, target node, edge value, start time, end time) to a
    * temporalgraph set with no vertex values.
    * @param tupleset DataSet Tuple5 with (source node, target node, edge value, start time, end time)
    * @param context the flink execution environment.
    * @return newly created Tgraphv3
    * */
    public static <K,EV,N> Tgraphv3<K,NullValue,EV,N> From5TupleNoVertexes(DataSet<Tuple5<K,K,EV,N,N>> tupleset, ExecutionEnvironment context) throws Exception {
        DataSet<Edge<K,Tuple3<EV,N,N>>> edges = tupleset.map(new MapFunction<Tuple5<K, K, EV, N, N>, Edge<K, Tuple3<EV, N, N>>>() {
            @Override
            public Edge<K, Tuple3<EV, N, N>> map(Tuple5<K, K, EV, N, N> value) throws Exception {
                return new Edge<K,Tuple3<EV,N,N>>(value.f0, value.f1, new Tuple3<EV,N,N>(value.f2, value.f3, value.f4));
            }
        });
        return FromEdgeSet(edges,context);
    }
    /*
    * Transforms a tuple5 dataset with (source node, target node, edge value, start time, end time) to a
    * temporalgraph set with no vertex values.
    * @param tupleset DataSet Tuple5 with (source node, target node, edge value, start time, end time)
    * @param context the flink execution environment.
    * @return newly created Tgraphv3
    * */
    public static <K,VV,EV,N> Tgraphv3<K,VV,EV,N> From5Tuple(DataSet<Tuple5<K,K,EV,N,N>> tupleset,DataSet<Vertex<K,VV>> vertices,ExecutionEnvironment context) throws Exception {
        DataSet<Edge<K,Tuple3<EV,N,N>>> edges = tupleset.map(new MapFunction<Tuple5<K, K, EV, N, N>, Edge<K, Tuple3<EV, N, N>>>() {
            @Override
            public Edge<K, Tuple3<EV, N, N>> map(Tuple5<K, K, EV, N, N> value) throws Exception {
                return new Edge<K,Tuple3<EV,N,N>>(value.f0, value.f1, new Tuple3<EV,N,N>(value.f2, value.f3, value.f4));
            }
        });
        return new Tgraphv3(vertices,edges,context);
    }

    /*
    * Transforms a tuple4 of (source node, target node, start time, end time) to a temporal graph set
    * with no vertex values and no edge values
    * @param tupleset DataSet Tuple4 with (source node, target node, start time, end time)
    * @param context the flink execution environment.
    * @return newly created Tgraphv3
    * */
    public static <K,N> Tgraphv3<K,NullValue,NullValue,N> From4TupleNoEdgesNoVertexes(DataSet<Tuple4<K,K,N,N>> tupleset, ExecutionEnvironment context) throws Exception {
        DataSet<Edge<K,Tuple3<NullValue,N,N>>> edges = tupleset.map(new MapFunction<Tuple4<K, K, N, N>, Edge<K, Tuple3<NullValue, N, N>>>() {
            @Override
            public Edge<K, Tuple3<NullValue, N, N>> map(Tuple4<K, K, N, N> value) throws Exception {
                return new Edge<>(value.f0, value.f1, new Tuple3<>(NullValue.getInstance(), value.f2, value.f3));
            }
        });
        return FromEdgeSet(edges,context);
    }

    /*
    * @param edges edge dataset
    * @param context the flink execution environment.
    * @return newly created Tgraphv3
    * */
    public static <K,EV,N> Tgraphv3<K,NullValue,EV,N> FromEdgeSet(DataSet<Edge<K, Tuple3<EV, N, N>>> edges, ExecutionEnvironment context) throws Exception {
        Graph<K,NullValue,Tuple3<EV,N,N>> temporalgraph = Graph.fromDataSet(edges,context);
        return new Tgraphv3<K,NullValue,EV,N>(temporalgraph.getVertices(),edges,context);
    }
    /**
     * @return a DataSet<Edge> with source/ target/ edge value of the temporal graph
     */
    public DataSet<Edge<K, EV>> getEdges() {
        DataSet<Edge<K, EV>> newedges = edges.map(new MapFunction<Edge<K, Tuple3<EV, N, N>>, Edge<K, EV>>() {
            @Override
            public Edge<K, EV> map(Edge<K, Tuple3<EV, N, N>> value) throws Exception {
                return new Edge<>(value.f0,value.f1,value.f2.getField(0));
            }
        });
        return newedges;
    }
    /*
    * @return Dataset(Vertex) of vertexes
    * */
    public DataSet<Vertex<K, VV>> getVertices() { return vertices; }

    /**
     * @return a long integer representing the number of vertices
     */
    public long numberOfVertices() throws Exception {
        return vertices.count();
    }
    /**
     * @return a long integer representing the number of edges
     */
    public long numberOfEdges() throws Exception {
        return edges.count();
    }

    /*
    * @return temporal graph as a Gelly Graph
    * */
    public Graph<K,VV,Tuple3<EV,N,N>> getGellyGraph() {
        Graph<K,VV,Tuple3<EV,N,N>> tempgraph = Graph.fromDataSet(vertices,edges,context);
        return tempgraph;
    }


}


