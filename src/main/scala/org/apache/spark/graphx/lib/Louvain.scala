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
//          vertexState.numEdges2CurrentCommunity = 0
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
      currentIter % 2 match {
        // odd -> even
        case 0 =>
          if (ec.srcAttr.randNum % 2 == 1 && ec.dstAttr.randNum % 2 == 0)
            ec.sendToDst(Set(ec.srcAttr.currentCommunityInfo))
          else if (ec.srcAttr.randNum % 2 == 0 && ec.dstAttr.randNum % 2 == 1)
            ec.sendToSrc(Set(ec.dstAttr.currentCommunityInfo))
        // even -> odd
        case 1 =>
          if (ec.srcAttr.randNum % 2 == 1 && ec.dstAttr.randNum % 2 == 0)
            ec.sendToSrc(Set(ec.dstAttr.currentCommunityInfo))
          else if (ec.srcAttr.randNum % 2 == 0 && ec.dstAttr.randNum % 2 == 1)
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

  // 添加顶点与各个社区的连边数
  private def addNumEdges(vid: VertexId, vd: Set[CommunityInfo], u: Map[VertexId, Int]): Set[(CommunityInfo, Int)]= {
    val numEdges = mutable.Set[(CommunityInfo, Int)]()
    vd.foreach{
      communityInfo => numEdges.add((communityInfo, u.getOrElse(communityInfo.communityId, 0)))
    }
    numEdges.toSet
  }

  private def getNumEdgesWithinCommunity(graph: Graph[(VertexId, Int), Int]): RDD[(VertexId, Int)] = {
    val sendEdgeAttr = (ec: EdgeContext[(VertexId, Int), Int, (VertexId, Int)]) => {
      if (ec.srcAttr._1 == ec.dstAttr._1) ec.sendToSrc((ec.srcAttr._1, ec.attr))
    }
    graph.aggregateMessages[(VertexId, Int)](sendEdgeAttr, (x1, x2) => (x1._1, x1._2+x2._2)).values.reduceByKey(_+_)
  }

  private def deltaQ(originalState: VertexState, targetCom: CommunityInfo, numEdges: Map[VertexId, Int]): Float = {
    // deltaQ1：顶点从原社区剥离之后，原社区Q的变化
    // deltaQ1 = sum_{j in community}(degree_{j})*degree_{i}/totWeight - numEdges_{i} - degree_{i}*degree_{i}/(2*totWeight)
    val deltaQ1: Float = originalState.currentCommunityInfo.totalDegree*originalState.degree/totWeight -
      numEdges.getOrElse(originalState.currentCommunityInfo.communityId, 0) -
      originalState.degree*originalState.degree/(2*totWeight)
    // deltaQ2：顶点加入新社区之后，新社区Q的变化
    // deltaQ2 = numEdges_{i} - degree_{i}*degree_{i}/(2*totWeight) - sum_{j in community}(degree_{j})*degree_{i}/totWeight
    val deltaQ2: Float = numEdges.getOrElse(targetCom.communityId, 0) -
      originalState.degree*originalState.degree/(2*totWeight) -
      targetCom.totalDegree*originalState.degree/totWeight
    deltaQ1 + deltaQ2
  }

  /**
   * assign to community which maximize Q
   *
   * @param vid 当前顶点id
   * @param vd 当前顶点的状态：顶点的度，顶点与当前社区的连边数，当前社区的信息
   * @param u 当前顶点接收的消息，各个社区的信息，以及当前顶点与该社区的连边数
   * @return （顶点的度，顶点与新社区的连边数，新社区的id）
   */
  private def selectCommunity(vid: VertexId, vd: VertexState, u: Option[(Set[CommunityInfo], Map[VertexId, Int])]):
  (VertexId, Int) = {
    var optimalCommunity: VertexId = vd.currentCommunityInfo.communityId
    if (u.isDefined) {
      var maxQ: Float = .0f
      var q: Float = .0f
      val numEdgesMap: Map[VertexId, Int] = u.get._2
      u.get._1.foreach{
        x => {
          q = deltaQ(vd, x, numEdgesMap)
//          println("vid: "+vid+", deltaQ: "+q+", target community: "+x._1+", numEdges to target community: "+x._2)
          if (q > maxQ) {
            maxQ = q
            optimalCommunity = x.communityId
          }
        }
      }
    }
    (optimalCommunity, vd.degree)
  }

  private def initLouvain[VD: ClassTag](graph: Graph[VD, Int]): Graph[VertexState, Int] = {
    totWeight = graph.edges.map(e=>e.attr).reduce(_+_).toFloat
    currentIter = 1
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
  private def stage1(cmGraph: Graph[VertexState, Int], maxIter: Int, minDeltaQ: Float): Graph[VertexState, Int] = {

    val toArray = (m: Map[VertexId, (Int, Int)]) => m.toArray

    var currentGraph: Graph[VertexState, Int] = cmGraph
    var newGraph: Graph[VertexState, Int] = null
    val loop = new Breaks
    var currentModularity: Float = modularity(currentGraph, totWeight)
    loop.breakable {
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
        // 中间结果，包括顶点分配的新社区、顶点的度
        // VD: (VertexId, Int) : (communityId, vertexDegree)
        val intermediateGraph: Graph[(VertexId, Int), Int] =
        currentGraph.outerJoinVertices(msgRddWithNumEdges)(selectCommunity).cache()
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
        println("current iteration: "+currentIter+", modularity: "+newModularity)
        if (newModularity < currentModularity + minDeltaQ) {
          println("MODULARITY NOT IMPROVE, BREAK LOOP")
          loop.break
        }
        currentModularity = newModularity
        println(currentGraph.vertices.innerJoin(
          newGraph.vertices
        )(
          (vid, vd, u) => vd.currentCommunityInfo.communityId != u.currentCommunityInfo.communityId
        ).filter(x=>x._2).count() + " vertices changed communities")
        currentGraph = newGraph

        currentIter += 1
      }
    }
    currentGraph
  }

//  def stage2(graph: Graph[VertexState, Int]): Graph[VertexState, Int] = {
//
//  }

  def run[VD: ClassTag](graph: Graph[VD, Int], maxIterStage1: Int, minDeltaQStage1: Float)
  : Graph[VertexState, Int] = {
    var cmGraph: Graph[VertexState, Int] = initLouvain(graph)
    cmGraph = stage1(cmGraph, maxIterStage1, minDeltaQStage1)
    cmGraph
  }
}