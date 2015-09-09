package com.soundcloud.lsh

import org.apache.spark.mllib.linalg.distributed.{
  CoordinateMatrix,
  IndexedRow,
  IndexedRowMatrix,
  MatrixEntry
}

/**
 * Brute force O(n^2) method to compute exact nearest neighbours.
 *
 * @param distance a function defining a metric over a vector space
 * @param threshold pairs that are >= to the distance are discarded
 * @param sample compute neighbours for the given random sample value
 *
 */
class NearestNeighbours(
  distance: VectorSimilarity,
  threshold: Double,
  sample: Double = 0.1) extends Joiner with Serializable {

  def join(inputMatrix: IndexedRowMatrix): CoordinateMatrix = {
    val rows = inputMatrix.rows
    val sampledRows = rows.sample(false, sample)
    sampledRows.cache()

    val joined = sampledRows.cartesian(rows)

    val similarity = joined.map {
      case ((rowA: IndexedRow), (rowB: IndexedRow)) =>
        ((rowA.index, rowB.index), distance(rowA.vector, rowB.vector))
    }

    val neighbours = similarity.filter {
      case ((indexA: Long, indexB: Long), similarity) =>
        similarity >= threshold &&
          indexA < indexB // make upper triangular and remove self similarities
    }

    val resultRows = neighbours.map {
      case ((indexA: Long, indexB: Long), similarity) =>
        MatrixEntry(indexA, indexB, similarity)
    }

    new CoordinateMatrix(resultRows)
  }
}
