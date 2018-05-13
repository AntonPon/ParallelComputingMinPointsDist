package scala

import parallel._

import scala.annotation.tailrec
import scala.util.Random

object ParalleCountPoints {

  def closest_pair(points: Seq[(Int, Int)]): (Double, Seq[(Int, Int)]) = {

    def getMiddle(xList: Seq[Int]): Int= {
      xList.length / 2
    }

    def mergeByY(seqL: Seq[(Int, Int)], seqR: Seq[(Int, Int)] ): Seq[(Int, Int)] = {
      (seqL ++ seqR).sortBy(_._2)
    }


    def boundaryMerge(seqMerged: Seq[(Int, Int)], deltaL: Double, deltaR: Double, median: Double ): Double = {
      val deltaMin = Math.min(deltaL, deltaR)
      val m = seqMerged.partition(point => point._1 >= median - deltaMin || point._1 <= median + deltaMin)._1
      //val numberOfExamples = Math.min(8, m.length - 2)

      def distance(left: (Int, Int), right: (Int, Int)): Double = {
        Math.sqrt(Math.pow(left._1 - right._1, 2) + Math.pow(left._2 - right._2, 2))
      }



      def getMinDistance(seq: Seq[(Int, Int)], currentIdx: Int, endIdx: Int, minValue: Double): Double = {
        def getCurrentMin(seq: Seq[(Int, Int)], pointOneIdx: Int, currentPointIdx: Int, maxPointIdx: Int, minPointValue: Double): Double = {
                if (currentPointIdx == maxPointIdx) minPointValue
                else {
                  val pointOne = seq(pointOneIdx)
                  val pointTwo = seq(pointOneIdx + currentPointIdx)
                  val minCurPointValue = Math.min(minPointValue, distance(pointOne, pointTwo)) // Math.sqrt(Math.pow(pointOne._1-pointTwo._1, 2) + Math.pow(pointOne._2 - pointTwo._2, 2))

                  getCurrentMin(seq, pointOneIdx, currentPointIdx+1, maxPointIdx, minCurPointValue)
                }
              }

              if (currentIdx > endIdx) minValue
              else {
                if (seq.length > 8){
                  val maxPointIdx = 8
                  val minCurValue = Math.min(minValue, getCurrentMin(seq, currentIdx, 1, maxPointIdx, minValue))
                  getMinDistance(seq, currentIdx+1, endIdx, minCurValue)
                }else {
                  val maxPointIdx = seq.length - currentIdx
                  val minCurValue = Math.min(minValue, getCurrentMin(seq, currentIdx, 1, maxPointIdx, minValue))
                  getMinDistance(seq, currentIdx+1, endIdx, minCurValue)
                }

              }

            }
      if (m.length > 7) getMinDistance(m, 0, m.length-8, deltaMin)
      else getMinDistance(m, 0, m.length-1, deltaMin)

    }

    if (points.length < 2) (Double.PositiveInfinity, points)
    else {
      val middle = getMiddle(points.map(_._1).sorted)
      val (l, r) = points.splitAt(middle)
      val ((deltaL, seqL), (deltaR, seqR)) = parallel(closest_pair(l), closest_pair(r))

      val seqMerged = mergeByY(seqL, seqR)
      val deltaMerged = boundaryMerge(seqMerged, deltaL, deltaR, points(middle)._1)
      (deltaMerged, seqMerged)
    }

  }

  @tailrec
  def seqClosestPair(points: Seq[(Int, Int)], startIdx: Int, endIdx: Int, distance: Double): Double = {

    def closestPerPoint(seq: Seq[(Int, Int)], startCurIdx: Int, endCurIdx: Int, distanceCur: Double): Double = {
      if (startCurIdx == endCurIdx + 1) distanceCur
      else {
        val pointOne = seq(startIdx)
        val pointTwo = seq(startCurIdx)
        val minDist = Math.min(distanceCur, Math.sqrt(Math.pow(pointOne._1-pointTwo._1, 2) + Math.pow(pointOne._2-pointTwo._2, 2)))
        closestPerPoint(seq, startCurIdx + 1, endCurIdx, minDist)
      }
    }

    if (startIdx == endIdx -1) distance
    else {
      val minTotalDist = Math.min(distance, closestPerPoint(points, startIdx + 1, endIdx, distance))
      seqClosestPair(points, startIdx + 1, endIdx, minTotalDist)
    }
  }

  def main(args: Array[String]): Unit = {

    val randomX = new Random()
    val randomY = new Random()

    var idx = 3
    val maxPower = 4
    while(idx != maxPower){

      val totalNumberOfPoints: Int = Math.pow(10, idx).toInt
      val maxPossibleValue: Int  =  Math.pow(10, maxPower).toInt
      val seq = Seq.fill(totalNumberOfPoints)(randomX.nextInt(maxPossibleValue), randomY.nextInt(maxPossibleValue))

      val timeStart = System.currentTimeMillis()
      val (dist, points) = closest_pair(seq)
      val parallelTime = (System.currentTimeMillis() - timeStart)/100
      println(s"parallel end $parallelTime")
      println(s"the distance computed by parallel $dist")


      val timeStartPar = System.currentTimeMillis()
      val distNotPar = seqClosestPair(seq, 0, seq.length-1, Double.PositiveInfinity)
      val nonParallelTime = (System.currentTimeMillis() - timeStartPar)/100
      println(s"non parallel end $nonParallelTime")
      println(s"the distance computed by non-parallel $distNotPar")
      println(s"the speedup for $totalNumberOfPoints points is  ${nonParallelTime/parallelTime}")
      idx += 1

    }
  }
}
