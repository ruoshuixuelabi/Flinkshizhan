package gelly

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.graph.scala.Graph
import org.apache.flink.graph.{Edge, Vertex}
import org.apache.flink.types.LongValue

/**
 * Gelly计算年龄大于25的顶点（用户）
 */
object GellyFunctionExample {
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
    //4.过滤年龄大于25的顶点
    val resultGraph = graph.subgraph(
      (vertex => vertex.getValue._2 > 25),
      (edge => true)
    )
    //输出所有顶点数据
    resultGraph.getVertices.print()
    //或者
    /*resultGraph.getVertices.collect().foreach(v=>{
        println(v.getValue._1+"的年龄是"+v.getValue._2)
    })*/
    //输出该图的人物关系
    graph.getTriplets().collect().foreach(triplet => {
      println(
        triplet.getSrcVertex.getValue._1
          + triplet.getEdge.getValue
          + triplet.getTrgVertex.getValue._1
      )
    })

    //计算每个顶点的入度
    val inDegrees: DataSet[(Long, LongValue)] = graph.inDegrees()
    //循环打印到控制台
    inDegrees.collect().foreach(println)

    //计算每个顶点的出度
    val outDegrees: DataSet[(Long, LongValue)] = graph.outDegrees()
    //循环打印到控制台
    outDegrees.collect().foreach(println)

    /*//计算每个顶点的度
    val degrees: DataSet[(Long, LongValue)] = graph.getDegrees()
    //循环打印到控制台
    degrees.collect().foreach(println)*/

    /*//最大入度
    graph.inDegrees().maxBy(1).print()
    //最大出度
    graph.outDegrees().maxBy(1).print()
    //最大度
    graph.getDegrees().maxBy(1).print()*/

    graph.getVertices.filter(v=>
        v.getValue._2==25
    ).print()
    //计算源顶点ID大于目标顶点ID的边的数量
    val count=graph.getEdges.filter(e=>
        e.getSource>e.getTarget
    ).count()
    println(count)
    //计算边值为"喜欢"的边数量
    val count喜欢=graph.getEdges.filter(e=>
        e.getValue=="喜欢"
    ).count()
    println(count喜欢)
    println()
    //修改图中的所有顶点属性，生成一个新的图
    graph.mapVertices(v=>
        //保持每个顶点的第一个属性不变，第二个属性加10
        (v.getValue._1,v.getValue._2+10)
    )
     .getVertices
     .print()
    //也可以使用以下代码代替，但生成的新顶点DataSet不会保留原图的结构索引，从而不会继承原图的优化效果：
    graph.getVertices.map(v =>
        //(顶点ID,(顶点第一个属性值,顶点第二个属性值+10))
        (v.getId, (v.getValue._1, v.getValue._2 + 10))
    ).print()
    println("使用mapEdges函数将所有边的属性改为'喜欢'")
    //使用mapEdges函数将所有边的属性改为"喜欢"
    graph.subgraph(
        (vertex => vertex.getValue._2 > 25),
        (edge => edge.getValue=="喜欢")
    )
    .getVertices
    .print()
  }
}