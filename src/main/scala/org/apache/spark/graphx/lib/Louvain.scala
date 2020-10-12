package org.apache.spark.graphx.lib

import org.apache.spark.graphx._
import org.apache.spark.graphx.Graph._
import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.rdd.RDD

import scala.util.control.Breaks
import scala.collection.mutable
import scala.reflect.ClassTag


object Louvain extends Serializable {

  private var currentIter: Int = 0
  private var totWeight: Float = 0

  private def genRandNum(seed: Int): Int = {
    (48271 * seed) %  255
  }

  private def modularity(graph: Graph[VertexState, Int], norm: Float): Float = {
    graph.mapTriplets[Float](
      (edge: EdgeTriplet[VertexState, Int]) =>
        if (edge.srcAttr.currentCommunityInfo.communityId != edge.dstAttr.currentCommunityInfo.communityId) 0.0f
        else edge.attr - edge.srcAttr.degree * edge.dstAttr.degree / norm
    ).edges.reduce((e1, e2) => Edge(attr = e1.attr + e2.attr)).attr / norm
  }

  private def initCommunityGraph[VD: ClassTag](graph: Graph[VD, Int]): Graph[VertexState, Int] = {
    // DONE, 聚合两个顶点之间不同方向的边
    graph.convertToCanonicalEdges(_+_)
      .outerJoinVertices(graph.degrees)(
        (vid, _, degOpt) => {
          val vertexState = new VertexState()
          vertexState.degree = degOpt.getOrElse(0)
          // 在每一轮迭代时，为每个顶点分配一个随机数，
          // 奇数轮 even顶点 -> odd顶点；偶数轮 odd顶点 -> even顶点
          // 确保在每一轮迭代时，一个顶点不能即接收又发送消息
          vertexState.randNum = genRandNum(vid.toInt)
          vertexState.currentCommunityInfo = CommunityInfo(vid)
          vertexState.currentCommunityInfo.numEdges = 0
          vertexState.currentCommunityInfo.totalDegree = degOpt.getOrElse(0)
          vertexState
        }
      )
  }

  /**
   * send massage function for `aggregateMessages`
   *
   * @param ec EdgeContext[VD, ED, msg], VD: VertexState, 顶点的度degree,
   *           顶点与当前社区的连边数numEdges2CurrentCommunity, 顶点当前社区信息currentCommunityInfo，
   *           msg: Set[CommunityInfo]，新社区的信息，集合形式方便聚合多个社区信息
   */
  private def sendMsg(ec: EdgeContext[VertexState, Int, Set[CommunityInfo]]): Unit = {
    if (ec.srcAttr.currentCommunityInfo.communityId != ec.dstAttr.currentCommunityInfo.communityId) {
      // 检查顶点的社区是否改变，或者有其他顶点加入到此顶点社区
      // 1) 如果此顶点的社区已改变，即此顶点已经移动过，则不再移动此顶点
      // 2) 如果此有其他顶点加入到此顶点社区，则不移动此顶点（否则会出现A属于B社区，而B属于C社区），
      // 但其他顶点仍然可以继续加入到此顶点社区
      val srcNotChangedYet: Boolean = ec.srcAttr.degree == ec.srcAttr.currentCommunityInfo.totalDegree
      val dstNotChangedYet: Boolean = ec.dstAttr.degree == ec.dstAttr.currentCommunityInfo.totalDegree
      currentIter % 2 match {
        // odd -> even
        case 0 =>
          if (ec.srcAttr.randNum % 2 == 1 && ec.dstAttr.randNum % 2 == 0 && dstNotChangedYet)
            ec.sendToDst(Set(ec.srcAttr.currentCommunityInfo))
          else if (ec.srcAttr.randNum % 2 == 0 && ec.dstAttr.randNum % 2 == 1 && srcNotChangedYet)
            ec.sendToSrc(Set(ec.dstAttr.currentCommunityInfo))
        // even -> odd
        case 1 =>
          if (ec.srcAttr.randNum % 2 == 1 && ec.dstAttr.randNum % 2 == 0 && srcNotChangedYet)
            ec.sendToSrc(Set(ec.dstAttr.currentCommunityInfo))
          else if (ec.srcAttr.randNum % 2 == 0 && ec.dstAttr.randNum % 2 == 1 && dstNotChangedYet)
            ec.sendToDst(Set(ec.srcAttr.currentCommunityInfo))
        case _ => Unit
      }
    }
  }

  /**
   * 获得顶点与各个社区的连边数信息
   *
   * @param graph 输入的图，VD: VertexState, 顶点
   *          的度degree，顶点与当前社区的连边数numEdges2CurrentCommunity，
   *          当前社区的信息currentCommunityInfo（社区id、
   *          社区内边的数量、社区内顶点的度之和），ED: [Int]，边的权重
   * @return 顶点与各个社区的连边数信息，map: communityId => numEdges
   * vertex.
   */
  private def getNumEdgesVertices2Community(graph: Graph[VertexState, Int]): VertexRDD[Map[VertexId, Int]] = {
    // DONE, 计算连边数时，考虑边上的权重
    val sendWeight = (ec: EdgeContext[VertexState, Int, Map[VertexId, Int]]) => {
      ec.sendToSrc(Map(ec.dstAttr.currentCommunityInfo.communityId->ec.attr))
      ec.sendToDst(Map(ec.srcAttr.currentCommunityInfo.communityId->ec.attr))
    }

    val mergeWeight = (map1: Map[VertexId, Int], map2: Map[VertexId, Int]) => {
      val mapMerged: mutable.Map[VertexId, Int] = mutable.Map()
      map1.keys.foreach{
        (k: VertexId) => mapMerged(k) = mapMerged.getOrElse(k, 0)+map1(k)
      }
      map2.keys.foreach{
        (k: VertexId) => mapMerged(k) = mapMerged.getOrElse(k, 0)+map2(k)
      }
      mapMerged.toMap
    }
    graph.aggregateMessages[Map[VertexId, Int]](sendWeight, mergeWeight)
  }

  private def getNumEdgesWithinCommunity(graph: Graph[(VertexId, Int), Int]): RDD[(VertexId, Int)] = {
    val sendEdgeAttr = (ec: EdgeContext[(VertexId, Int), Int, (VertexId, Int)]) => {
      if (ec.srcAttr._1 == ec.dstAttr._1) ec.sendToSrc((ec.srcAttr._1, ec.attr))
    }
    graph.aggregateMessages[(VertexId, Int)](sendEdgeAttr, (x1, x2) => (x1._1, x1._2+x2._2)).values.reduceByKey(_+_)
  }

  private def deltaQ(originalState: VertexState, targetCom: CommunityInfo, numEdges: Map[VertexId, Int]): Float = {
    // 新的顶点移动策略下（每一轮只移动未曾移动的顶点），deltaQ的简化计算
    (numEdges.getOrElse(targetCom.communityId, 0)-targetCom.totalDegree*originalState.degree/totWeight)/(2*totWeight)
  }

  /**
   * assign to community which maximize Q
   *
   * @param vid 当前顶点id
   * @param vd 当前顶点的状态：顶点的度，当前社区的信息
   * @param u 当前顶点接收的消息，各个社区的信息，以及当前顶点与各社区的连边数
   * @return （最优社区， 顶点的度， 顶点社区是否发生改变）
   */
  private def selectCommunity(vid: VertexId, vd: VertexState, u: Option[(Set[CommunityInfo], Map[VertexId, Int])]):
  (VertexId, Int, Boolean) = {
    var optimalCommunity: VertexId = vd.currentCommunityInfo.communityId
    if (u.isDefined) {
      var maxQ: Float = .0f
      var q: Float = .0f
      val numEdgesMap: Map[VertexId, Int] = u.get._2
      u.get._1.foreach{
        x => {
          q = deltaQ(vd, x, numEdgesMap)
          if (q > maxQ) {
            maxQ = q
            optimalCommunity = x.communityId
          }
        }
      }
    }
    (optimalCommunity, vd.degree, optimalCommunity!=vd.currentCommunityInfo.communityId)
  }

  def initLouvain[VD: ClassTag](graph: Graph[VD, Int]): Graph[VertexState, Int] = {
    totWeight = graph.edges.map(e=>e.attr).reduce(_+_).toFloat
    initCommunityGraph(graph).cache()
  }

  /**
   * run louvain stage#1
   *
   * @param cmGraph 同上
   * @param maxIter 最大迭代次数
   * @param minDeltaQ 每轮迭代Q的最小增加值
   * @return 新的社区划分
   */
  def stage1(cmGraph: Graph[VertexState, Int], maxIter: Int, minDeltaQ: Float): Graph[VertexState, Int] = {

    currentIter = 1

    var currentGraph: Graph[VertexState, Int] = cmGraph
    var newGraph: Graph[VertexState, Int] = null
    val loopInner = new Breaks
    var currentModularity: Float = modularity(currentGraph, totWeight)
    loopInner.breakable {
      var numItersNoVertexChange: Int = 0
      while (currentIter <= maxIter) {
        val msgRdd: VertexRDD[Set[CommunityInfo]] = currentGraph.aggregateMessages[Set[CommunityInfo]](sendMsg, _++_).cache()
        // VD: Map[VertexId, Int], communityId -> numEdges
        val numEdgesVertices2Community: VertexRDD[Map[VertexId, Int]] = msgRdd.aggregateUsingIndex(
          getNumEdgesVertices2Community(currentGraph),
          (v1: Map[VertexId, Int], v2: Map[VertexId, Int]) => v1
        )
        // 发送到各个顶点上的消息，包括计算deltaQ所需要的信息
        // VD: Set[(CommunityInfo, Int)], (communityInfo, numEdges)
        val msgRddWithNumEdges: VertexRDD[(Set[CommunityInfo], Map[VertexId, Int])] = msgRdd.innerJoin(
          numEdgesVertices2Community
        )((vid, vd, u) => (vd, u))
        msgRdd.unpersist(false)
        // 临时结果，包括顶点分配的新社区、顶点的度以及顶点的社区是否改变
        // VD: (VertexId, Int, Boolean) : (communityId, vertexDegree, isChanged)
        val tmpGraph: Graph[(VertexId, Int, Boolean), Int] = currentGraph.outerJoinVertices(msgRddWithNumEdges)(selectCommunity)
        val numVerticesChangedCommunity: Long = tmpGraph.vertices.filter(x=>x._2._3).count()
        if (numVerticesChangedCommunity < 1L) {
          numItersNoVertexChange += 1
          if (numItersNoVertexChange >= 4) {
            println("NO VERTEX CHANGE COMMUNITY, BREAK INNER LOOP")
            // 需要重新创建一个图，否则在下一轮outer迭代计算modularity的时候会报错
            currentGraph = GraphImpl(currentGraph.vertices, currentGraph.edges).cache()
            loopInner.break
          }
        } else numItersNoVertexChange = 0
        println(numVerticesChangedCommunity + " vertices will update communities")
        // 中间结果，包括顶点分配的新社区、顶点的度
        // VD: (VertexId, Int) : (communityId, vertexDegree)
        val intermediateGraph: Graph[(VertexId, Int), Int] = tmpGraph.mapVertices((vid, vd)=>(vd._1, vd._2)).cache()
        // 社区的总度数
        val totDegreePerCommunity: RDD[(VertexId, Int)] = intermediateGraph.vertices.values.reduceByKey(_+_)
        // 社区内的连边数
        val numEdgesWithinCommunity: RDD[(VertexId, Int)] = getNumEdgesWithinCommunity(intermediateGraph)
        // 社区的总度数和社区内的连边数
        val communityInfo: RDD[(VertexId, CommunityInfo)] = totDegreePerCommunity.leftOuterJoin(
          numEdgesWithinCommunity
        ).map(
          x => {
            val cInfo: CommunityInfo = CommunityInfo(x._1)
            cInfo.totalDegree = x._2._1
            cInfo.numEdges = x._2._2.getOrElse(0)
            (x._1, cInfo)
          }
        )
        val vertexCommunityInfo: RDD[(VertexId, CommunityInfo)] = intermediateGraph.vertices.map(
          x => (x._2._1, x._1)
        ).join(communityInfo).map(x => x._2)
        intermediateGraph.unpersist()
        // 为顶点分配新的社区id
        newGraph = currentGraph.outerJoinVertices(
          currentGraph.vertices.aggregateUsingIndex(vertexCommunityInfo, (x1: CommunityInfo, x2: CommunityInfo) => x1)
        )(
          (vid, vd, u) => {
            if (u.isDefined) vd.currentCommunityInfo = u.get
            vd.randNum = genRandNum(vd.randNum)
            vd
          }
        )
        newGraph = GraphImpl(newGraph.vertices, newGraph.edges).cache()

        val newModularity: Float = modularity(newGraph, totWeight)
        println("current INNER iteration: "+currentIter+", modularity: "+newModularity)
        if (newModularity < currentModularity + minDeltaQ) {
          println("MODULARITY NOT IMPROVE, BREAK INNER LOOP")
          // 需要重新创建一个图，否则在下一轮outer迭代计算modularity的时候会报错
          currentGraph = GraphImpl(currentGraph.vertices, currentGraph.edges).cache()
          loopInner.break
        }
        currentModularity = newModularity

        currentGraph = newGraph

        currentIter += 1
      }
    }
    currentGraph
  }

  def stage2(graph: Graph[VertexState, Int]): Graph[VertexState, Int] = {
    val vertexRdd: RDD[(VertexId, VertexState)] = graph.vertices.map(
      x => x._2.currentCommunityInfo.toTuple()
    ).distinct().map(
      x => {
        val vd: VertexState = new VertexState()
        vd.degree = x._2._1
        vd.randNum = genRandNum(x._1.toInt)
        vd.currentCommunityInfo = CommunityInfo(x._1)
        vd.currentCommunityInfo.numEdges = x._2._2
        vd.currentCommunityInfo.totalDegree = x._2._1
        (x._1, vd)
      }
    )
    // 包含自环
    val edgeRdd: RDD[Edge[Int]] = graph.mapTriplets(
      et=>(et.srcAttr.currentCommunityInfo.communityId, et.dstAttr.currentCommunityInfo.communityId, et.attr)
    ).edges.map(x=>Edge(x.attr._1, x.attr._2, x.attr._3))

    GraphImpl(VertexRDD[VertexState](vertexRdd), EdgeRDD.fromEdges(edgeRdd)).convertToCanonicalEdges(_+_).cache()
  }

  def run[VD: ClassTag](graph: Graph[VD, Int], maxIter: Int,
                        maxIterStage1: Int)
  : RDD[(VertexId, VertexId)] = {
    var cmGraph: Graph[VertexState, Int] = initLouvain(graph)
    var vertexCommunity: RDD[(VertexId, VertexId)] = cmGraph.vertices.map(x => (x._1, x._2.currentCommunityInfo.communityId))
    var iter: Int = 1
    val loopOuter = new Breaks
    loopOuter.breakable{
      while (iter <= maxIter) {
        println("current OUTER iteration: "+iter+", communities: "+cmGraph.vertices.count()+", modularity: "+modularity(cmGraph, totWeight))
        println("---------------------------- START --------------------------------")
        // 动态更新minDeltaQ参数，迭代次数越大，minDeltaQ越小
        cmGraph = stage1(cmGraph, maxIterStage1, 0f)
        vertexCommunity = vertexCommunity.map(
          x=>(x._2, x._1)
        ).leftOuterJoin[VertexId](
          cmGraph.vertices.map(x=>(x._1, x._2.currentCommunityInfo.communityId))
        ).map(x=>(x._2._1, x._2._2.getOrElse(-1L)))
        cmGraph = stage2(cmGraph)
        println("edge count:" + cmGraph.edges.count())
        println("---------------------------- END --------------------------------")
        iter += 1
      }
    }
    vertexCommunity
  }
}