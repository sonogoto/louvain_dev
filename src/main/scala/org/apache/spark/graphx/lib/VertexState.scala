package org.apache.spark.graphx.lib

class VertexState extends Serializable {
  var degree: Int = 0
  var numEdges2CurrentCommunity: Int = 0
  var flag: Int = 1
  var currentCommunityInfo: CommunityInfo = CommunityInfo(-1L)

  override def toString(): String = {
    "{degree:" + degree + ",numEdges2CurrentCommunity:" + numEdges2CurrentCommunity +
      ",flag:" + flag + ",currentCommunityInfo:" + currentCommunityInfo.toString() + "}"
  }

}
