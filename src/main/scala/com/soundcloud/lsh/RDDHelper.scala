package com.soundcloud.lsh

import org.apache.spark
import org.apache.spark.rdd.RDD

import scala.language.implicitConversions
import scala.reflect.ClassTag

final case class HelperRDDFunctions[A](self: RDD[A]) extends AnyVal {
  /**
   * Applies a partial function to an RDD. This is useful for chaining map or
   * filter functions onto an RDD as you might with a core Scala collection.
   */
  def andThen[T](fn: (RDD[A]) => RDD[T]): RDD[T] = fn(self)

}

object SparkImplicits {
  implicit def rddToHelperRDDFunctions[A](rdd: RDD[A]): HelperRDDFunctions[A] =
    HelperRDDFunctions(rdd)
}
