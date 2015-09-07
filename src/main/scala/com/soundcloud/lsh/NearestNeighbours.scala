package com.soundcloud.lsh

import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, IndexedRow, IndexedRowMatrix, MatrixEntry}

/**
 * Uses brute force method to compute nearest neighbours. As this is a very expensive computation O(n^2)
 * an additional sample parameter may be passed such that neighbours are just computed for a random fraction.
 * This method is provided to compare the LSH quality to the exact KNN solution on a random subset.
 */
class NearestNeighbours(vectorSimilarity: VectorSimilarity, threshold: Double, sample: Double = 0.1)
  extends Joiner
  with Serializable {

  def join(inputMatrix: IndexedRowMatrix): CoordinateMatrix = {
    val rows = inputMatrix.rows
    val sampledRows = rows.sample(false, sample)
    sampledRows.cache()

    val joined = sampledRows.cartesian(rows)

    val similarity = joined.map {
      case ((rowA: IndexedRow), (rowB: IndexedRow)) =>
        ((rowA.index, rowB.index), vectorSimilarity(rowA.vector, rowB.vector))
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