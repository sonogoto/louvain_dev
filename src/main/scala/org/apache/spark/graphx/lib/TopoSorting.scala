package org.apache.spark.graphx.lib

import org.apache.spark.graphx._
import scala.reflect.ClassTag


object TopoSorting extends Serializable {

  def run[VD, ED: ClassTag](graph: Graph[VD, ED], maxSteps: Int): Graph[Long, ED] = {
    val tsGraph = graph.mapVertices { (vid, attr) => 0L }

    val initialMessage = 1L

    def vertexProgram(id: VertexId, attr: Long, msg: Long): Long = {
      math.max(attr, msg)
    }

    def sendMessage(edge: EdgeTriplet[Long, _]): Iterator[(VertexId, Long)] = {
      Iterator((edge.dstId, edge.srcAttr+1))
    }

    Pregel(tsGraph, initialMessage, maxSteps, EdgeDirection.Out)(vertexProgram, sendMessage, math.max)
  }
}
