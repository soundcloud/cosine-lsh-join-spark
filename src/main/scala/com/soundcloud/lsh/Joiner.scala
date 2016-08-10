package com.soundcloud.lsh

import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, IndexedRowMatrix}

trait Joiner {
  /**
   * Find the k nearest neighbors from a data set for every other object in the
   * same data set. Implementations may be either exact or approximate.
   *
   * @param matrix a row oriented matrix. Each row in the matrix represents
   *               a record in the dataset. Records are identified by their
   *               matrix index.
   * @return a similarity matrix with MatrixEntry(index_a, index_b, similarity).
   *
   */
  def join(matrix: IndexedRowMatrix): CoordinateMatrix

}

trait QueryJoiner {
  /**
   * Find the k nearest neighbours in catalogMatrix for each entry in queryMatrix.
   * Implementations may be either exact or approximate.
   *
   * @param queryMatrix   a row oriented matrix. Each row in the matrix represents a
   *                      record in the data set. Records are identified by their matrix
   *                      index.
   * @param catalogMatrix a row oriented matrix. Each row in the matrix represents a
   *                      record in the data set. Records are identified by their matrix
   *                      index.
   */
  def join(queryMatrix: IndexedRowMatrix, catalogMatrix: IndexedRowMatrix): CoordinateMatrix
}
