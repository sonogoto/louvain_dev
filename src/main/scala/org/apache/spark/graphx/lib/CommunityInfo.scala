package org.apache.spark.graphx.lib


import org.apache.spark.graphx.VertexId


case class CommunityInfo(communityId: VertexId) extends Serializable {

//  var communityId: VertexId = -1L
  var numEdges: Int = 0
  var totalDegree: Int = 0

  override def toString(): String = {
    "{communityId:" + communityId + ",numEdges:" + numEdges +
      ",totalDegree:" + totalDegree + "}"
  }

  def toTuple(): (VertexId, (Int, Int)) = {
    (communityId, (totalDegree, numEdges))
  }
}