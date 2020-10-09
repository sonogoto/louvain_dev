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
          vertexState.numEdges2CurrentCommunity = 0
          vertexState.currentCommunityInfo = CommunityInfo(vid)
          vertexState.currentCommunityInfo.numEdges = 0
          vertexState.currentCommunityInfo.totalDegree = degOpt.getOrElse(0)
          vertexState
        }
      )
  }

  /**
   * 为同一个社区内的边两端的顶点指定flag，在一轮迭代中，
   * 边的两个顶点，至多只有一个能够从当前社区中剥离出去，
   * 避免出现社区度数或边数更新错误
   */
  private def assignFlag(ec: EdgeContext[VertexState, Int, (Int, Int)]): Unit = {
//    if (ec.srcAttr.currentCommunityInfo.communityId == ec.dstAttr.currentCommunityInfo.communityId) {
      if (currentIter % 2 == 0) {
        ec.sendToSrc(0)
        ec.sendToDst(1)
      }
      else {
        ec.sendToSrc(1)
        ec.sendToDst(0)
      }
//    }
//    ec.sendToSrc((0, 1))
//    ec.sendToDst((1, 1))
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
//      println("VERTICES %s AND %s ARE IN DIFFERENT COMMUNITIES: %s AND %s".format(
//        ec.srcId, ec.dstId, ec.srcAttr.currentCommunityInfo.communityId, ec.dstAttr.currentCommunityInfo.communityId
//      ))
//      currentIter % 2 match {
//        // odd -> even
//        case 0 =>
//          if (ec.srcAttr.currentCommunityInfo.communityId % 2 == 1 && ec.dstAttr.currentCommunityInfo.communityId % 2 == 0)
//            ec.sendToDst(Set(ec.srcAttr.currentCommunityInfo))
//          else if (ec.srcAttr.currentCommunityInfo.communityId % 2 == 0 && ec.dstAttr.currentCommunityInfo.communityId % 2 == 1)
//            ec.sendToSrc(Set(ec.dstAttr.currentCommunityInfo))
//        // even -> odd
//        case 1 =>
//          if (ec.srcAttr.currentCommunityInfo.communityId % 2 == 1 && ec.dstAttr.currentCommunityInfo.communityId % 2 == 0)
//            ec.sendToSrc(Set(ec.dstAttr.currentCommunityInfo))
//          else if (ec.srcAttr.currentCommunityInfo.communityId % 2 == 0 && ec.dstAttr.currentCommunityInfo.communityId % 2 == 1)
//            ec.sendToDst(Set(ec.srcAttr.currentCommunityInfo))
//        case _ => Unit
//      }
      // even iteration, low --> high
      if (currentIter % 2 == 0) {
        if (ec.srcAttr.flag == 0) ec.sendToSrc(Set(ec.dstAttr.currentCommunityInfo))
//        if (ec.srcAttr.currentCommunityInfo.communityId < ec.dstAttr.currentCommunityInfo.communityId)
//          {if(ec.srcAttr.flag==1) ec.sendToSrc(Set(ec.dstAttr.currentCommunityInfo))} // dst invite src join it's community
//        else
//          {if(ec.dstAttr.flag==1) ec.sendToDst(Set(ec.srcAttr.currentCommunityInfo))} // src invite dst join it's community
      }
      // odd iteration, high --> low
      else {
        if (ec.dstAttr.flag == 1) ec.sendToDst(Set(ec.srcAttr.currentCommunityInfo))
//        if (ec.srcAttr.currentCommunityInfo.communityId < ec.dstAttr.currentCommunityInfo.communityId)
//          {if(ec.dstAttr.flag==1) ec.sendToDst(Set(ec.srcAttr.currentCommunityInfo))} // src invite dst join it's community
//        else
//          {if(ec.srcAttr.flag==1) ec.sendToSrc(Set(ec.dstAttr.currentCommunityInfo))} // dst invite src join it's community
      }
    }
//    else {
//      println("VERTICES %s AND %s ARE IN SAME COMMUNITY: %s".format(
//        ec.srcId, ec.dstId, ec.srcAttr.currentCommunityInfo.communityId
//      ))
//    }
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
//    graph.collectNeighbors(EdgeDirection.Either).mapValues {
//      (neighbors: Array[(VertexId, VertexState)]) => {
//        val map = mutable.Map[VertexId, Int]()
//        neighbors.foreach{
//          (x: (VertexId, VertexState)) =>
//            map.put(x._2.currentCommunityInfo.communityId, map.getOrElse(x._2.currentCommunityInfo.communityId, 0)+1)
//        }
//        map.toMap
//      }
//    }
  }

  // 添加顶点与各个社区的连边数
  private def addNumEdges(vid: VertexId, vd: Set[CommunityInfo], u: Map[VertexId, Int]): Set[(CommunityInfo, Int)]= {
    val numEdges = mutable.Set[(CommunityInfo, Int)]()
    vd.foreach{
      communityInfo => numEdges.add((communityInfo, u.getOrElse(communityInfo.communityId, 0)))
    }
    numEdges.toSet
  }

  private def deltaQ(originalState: VertexState, targetCom: (CommunityInfo, Int)): Float = {
    // deltaQ1：顶点从原社区剥离之后，原社区Q的变化
    // deltaQ1 = sum_{j in community}(degree_{j})*degree_{i}/totWeight - numEdges_{i} - degree_{i}*degree_{i}/(2*totWeight)
    val deltaQ1: Float = originalState.currentCommunityInfo.totalDegree*originalState.degree/totWeight -
      originalState.numEdges2CurrentCommunity -
      originalState.degree*originalState.degree/(2*totWeight)
    // deltaQ2：顶点加入新社区之后，新社区Q的变化
    // deltaQ2 = numEdges_{i} - degree_{i}*degree_{i}/(2*totWeight) - sum_{j in community}(degree_{j})*degree_{i}/totWeight
    val deltaQ2: Float = targetCom._2 -
      originalState.degree*originalState.degree/(2*totWeight) -
      targetCom._1.totalDegree*originalState.degree/totWeight
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
  private def selectCommunity(vid: VertexId, vd: VertexState, u: Option[Set[(CommunityInfo, Int)]]):
  (Int, Int, VertexId, Option[Map[VertexId, (Int, Int)]]) = {
    var optimalCommunity: VertexId = vd.currentCommunityInfo.communityId
    var numEdges2Community: Int = vd.numEdges2CurrentCommunity
    val map = mutable.Map[VertexId, (Int, Int)]()
    if (u.isDefined) {
      var maxQ: Float = .0f
      var q: Float = .0f
      u.get.foreach{
        x => {
          q = deltaQ(vd, x)
//          println("vid: "+vid+", deltaQ: "+q+", target community: "+x._1+", numEdges to target community: "+x._2)
          if (q > maxQ) {
            maxQ = q
            optimalCommunity = x._1.communityId
            numEdges2Community = x._2
          }
        }
      }
    }
    if (optimalCommunity != vd.currentCommunityInfo.communityId) {
      map.put(optimalCommunity, (vd.degree, numEdges2Community))
      map.put(vd.currentCommunityInfo.communityId, (-vd.degree, -vd.numEdges2CurrentCommunity))
    }
    val communityInfoUpdate: Option[Map[VertexId, (Int, Int)]] = if (map.nonEmpty) Option(map.toMap) else None
    (vd.degree, numEdges2Community, optimalCommunity, communityInfoUpdate)
  }

  def initLouvain[VD: ClassTag](graph: Graph[VD, Int]): Graph[VertexState, Int] = {
    totWeight = graph.edges.map(e=>e.attr).reduce(_+_).toFloat
    currentIter = 1
    val cmGraph: Graph[VertexState, Int] = initCommunityGraph(graph)
    val flagRdd: VertexRDD[Int] = cmGraph.aggregateMessages[(Int, Int)](
      assignFlag,
      (x1, x2) => (x1._1+x2._1, x1._2+x2._2)
    ).mapValues(x=>{val avg: Float = 1.0f*x._1/x._2; if (avg > .0f && avg < 1.0f) -1 else avg.toInt})
    cmGraph.outerJoinVertices(flagRdd)((vid, vd, u)=> {vd.flag = if(u.isDefined) u.get else -1; vd}).cache()
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
        val msgRddWithNumEdges: VertexRDD[Set[(CommunityInfo, Int)]] = msgRdd.innerJoin(numEdgesVertices2Community)(addNumEdges)
        msgRdd.unpersist(false)
        // 中间结果，包括顶点分配的新社区、与新社区之间的连边数，以及，顶点加入新社区后，
        // 对原社区/新社区的总度数和总边数的影响
        // VD: (Int, Int, VertexId, Option[Map[VertexId, (Int, Int)]])
        // (degree, numEdges, communityId, communityId -> (deltaDegree, deltaNumEdges))
        val intermediateGraph: Graph[(Int, Int, VertexId, Option[Map[VertexId, (Int, Int)]]), Int] =
        currentGraph.outerJoinVertices(msgRddWithNumEdges)(selectCommunity).cache()
        println(intermediateGraph.vertices.filter(x => x._1 != x._2._3).count()+" vertices was assigned new community")
        // 按照新的社区划分方案，各个社区信息的更新
        // (communityId, (deltaDegree, deltaNumEdges))
        val communityInfoUpdateRdd: RDD[(VertexId, (Int, Int))] =  intermediateGraph.vertices.filter(
          x => x._2._4.isDefined
        ).map(x=>x._2._4.get).flatMap(
          x => toArray(x)
        ).reduceByKey((x1, x2) => (x1._1+x2._1, x1._2+x2._2)).cache()
        // 如果没有社区信息更新，则退出迭代
        if (communityInfoUpdateRdd.count() <= 0) {
          println("NO COMMUNITY UPDATE, BREAK LOOP")
          loop.break
        }
        // 更新后的社区信息
        val newCommunityInfo: RDD[(VertexId, CommunityInfo)] = currentGraph.vertices.map(
          x => (x._2.currentCommunityInfo.communityId,
            (x._2.currentCommunityInfo.totalDegree, x._2.currentCommunityInfo.numEdges))
        ).join(communityInfoUpdateRdd).map(
          (x: (VertexId, ((Int, Int), (Int, Int)))) => {
            val communityInfo = CommunityInfo(x._1)
            communityInfo.totalDegree = x._2._1._1 + x._2._2._1
            communityInfo.numEdges = x._2._1._2 + x._2._2._2
            (x._1, communityInfo)
          }
        )
        communityInfoUpdateRdd.unpersist(false)
        // 为顶点分配新的社区id以及与此社区的连边数
        newGraph = currentGraph.outerJoinVertices(intermediateGraph.vertices)(
          (vid, vd, u) => {
            if (u.isDefined) {
              vd.numEdges2CurrentCommunity = u.get._2
              vd.currentCommunityInfo = CommunityInfo(u.get._3)
            }
            vd
          }
        ).cache()
        intermediateGraph.unpersist(false)
        // 关联社区的总度数和总边数
        val vertexCommunityInfoRdd: RDD[(VertexId, CommunityInfo)] =
          newGraph.vertices.map(x => (x._2.currentCommunityInfo.communityId, x._1)).join(newCommunityInfo).map(
          (x: (VertexId, (VertexId, CommunityInfo))) => {
            x._2
          }
        )
        newGraph = newGraph.outerJoinVertices(
          newGraph.vertices.aggregateUsingIndex(vertexCommunityInfoRdd, (v1: CommunityInfo, v2: CommunityInfo) => v1)
        )(
          (vid, vd, u) => {
            if (u.isDefined) vd.currentCommunityInfo = u.get
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