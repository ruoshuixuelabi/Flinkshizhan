package gelly

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.graph.scala.{Graph, NeighborsFunctionWithVertexValue}
import org.apache.flink.graph.{Edge, EdgeDirection, Vertex}
import org.apache.flink.util.Collector

/**
  * 计算社交网络中每个人的粉丝平均年龄
  */
object GellyFansAvgAgeExample {
    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment
        //1.创建顶点集合和边集合
        //创建顶点集合(用户ID,(姓名,年龄))
        val vertexArray = Array(
            new Vertex(1L,("Alice", 30)),
            new Vertex(2L,("Henry", 27)),
            new Vertex(3L,("Charlie", 25)),
            new Vertex(4L,("Peter", 22)),
            new Vertex(5L,("Mike", 29)),
            new Vertex(6L,("Kate", 23))
        )
        //创建边集合
        val edgeArray = Array(
            new Edge(2L, 1L, "关注"),
            new Edge(2L, 1L, "喜欢"),//此为重复（平行）边
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
        val graph = Graph.fromDataSet(vertices,edges,env)

        val resultDataSet = graph.groupReduceOnNeighbors(
            new SelectFansNeighbors,
            EdgeDirection.IN //计算的是顶点的入边
        ).print()

    }
}

/**
  * 自定义聚合函数，进行聚合计算
  *
  * 第一个泛型参数类型：顶点ID类型
  * 第二个泛型参数类型：顶点值类型
  * 第三个泛型参数类型：边值类型
  * 第四个泛型参数类型：最终发出的结果类型
  */
final class SelectFansNeighbors extends NeighborsFunctionWithVertexValue[Long, (String,Int), String,String] {
    /**
      * 循环某个顶点的所有相邻顶点（即某个顶点的所有边连接的顶点）
      * @param vertex 需要计算的某个顶点，每个顶点调用一次该方法
      * @param neighbors 需要计算的某个顶点的所有入边及入边的连接顶点组成的集合
      * @param out 收集并发出元素数据
      */
    override def iterateNeighbors(vertex: Vertex[Long,(String,Int)],
                                  neighbors: Iterable[(Edge[Long, String], Vertex[Long,(String,Int)])],
                                  out: Collector[String]) = {
        //循环相邻顶点集合的每个元素
        var fansTotalAge=0//存储顶点（粉丝）年龄总和
        var fansCount=0//存储顶点（粉丝）数量
        for (neighbor <- neighbors) {
            //获取顶点年龄值,累加到变量totalAge
            fansTotalAge+=neighbor._2.getValue._2
            fansCount=fansCount+1
        }
        if(fansCount!=0){
            val fansAvgAge=fansTotalAge/fansCount
            out.collect("顶点ID："+vertex.getId+",姓名："+vertex.getValue._1+",粉丝平均年龄："+fansAvgAge)
        }
    }
}

