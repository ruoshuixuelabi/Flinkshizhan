package gelly

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.graph.scala.{Graph, NeighborsFunctionWithVertexValue}
import org.apache.flink.graph.{Edge, EdgeDirection, Vertex, VertexJoinFunction}
import org.apache.flink.util.Collector

/**
 * 合并平行边
 */
object GellyJoinExample {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    //1.创建顶点集合和边集合
    //创建顶点集合(用户ID,(姓名,年龄))
    val vertexArray = Array(
      new Vertex(1L, ("Alice", 30)),
      new Vertex(2L, ("Henry", 27)),
      new Vertex(3L, ("Charlie", 25)),
      new Vertex(4L, ("Peter", 22)),
      new Vertex(5L, ("Mike", 29)),
      new Vertex(6L, ("Kate", 23))
    )
    //创建边集合
    val edgeArray = Array(
      new Edge(2L, 1L, "关注"),
      new Edge(2L, 1L, "喜欢"), //此为重复（平行）边
      new Edge(2L, 4L, "喜欢"),
      new Edge(3L, 2L, "关注"),
      new Edge(3L, 6L, "关注"),
      new Edge(5L, 2L, "喜欢"),
      new Edge(5L, 3L, "关注"),
      new Edge(5L, 6L, "关注")
    )
    //2.转为分布式集合DataSet
    val vertices: DataSet[Vertex[Long, (String, Int)]] = env.fromCollection(vertexArray)
    val edges: DataSet[Edge[Long, String]] = env.fromCollection(edgeArray)
    //3.构建Graph图
    val graph = Graph.fromDataSet(vertices, edges, env)
    val addressArr = Array(
      (3L, "北京"), //(顶点ID、地址)
      (4L, "上海"),
      (5L, "山东"),
      (6L, "江苏"),
      (7L, "河北")
    )
    val addressDataSet: DataSet[(Long, String)] = env.fromCollection(addressArr)
    //左连接
    graph.joinWithVertices(
      addressDataSet,
      new VertexJoinFunction[(String, Int), String] {
        /**
         * 对当前顶点值和输入DataSet的匹配顶点应用转换
         *
         * @param vertexValue 当前顶点值
         * @param inputValue  匹配的Tuple2输入的值
         * @return 新的顶点值
         */
        override def vertexJoin(vertexValue: (String, Int), inputValue: String): (String, Int) = {
          (vertexValue._1 + "&" + inputValue, vertexValue._2)
        }
      })
      .getVertices
      .print()
  }
}