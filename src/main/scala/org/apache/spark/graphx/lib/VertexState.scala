package org.apache.spark.graphx.lib

class VertexState extends Serializable {
  var degree: Int = 0
//  var numEdges2CurrentCommunity: Int = 0
  var randNum: Int = -1
  var currentCommunityInfo: CommunityInfo = CommunityInfo(-1L)

  override def toString(): String = {
    "{degree:" + degree +
      ",randNum:" + randNum + ",currentCommunityInfo:" + currentCommunityInfo.toString() + "}"
  }

}
