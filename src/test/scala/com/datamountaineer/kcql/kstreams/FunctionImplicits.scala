package com.datamountaineer.kcql.kstreams

import org.apache.kafka.streams.kstream.Reducer

/**
  * Implicit conversions that provide us with some syntactic sugar when writing stream transformations.
  */
object FunctionImplicits {

  implicit def functionToReducer[V](f: ((V, V) => V)): Reducer[V] = new Reducer[V] {
    override def apply(l: V, r: V): V = f(l, r)
  }


}
