package com.datamountaineer.kcql.kstreams

import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.KeyValueMapper

object KeyValueImplicits {

  implicit def Tuple2ToKeyValue[K, V](tuple: (K, V)): KeyValue[K, V] = {
    new KeyValue(tuple._1, tuple._2)
  }


  implicit def Tuple2ToKeyValueMapper[K, V,KR,VR](fn: (K,V)=>(KR, VR)): KeyValueMapper[K, V,KeyValue[KR,VR]] = {
    new KeyValueMapper[K,V,KeyValue[KR, VR]] {
      override def apply(key: K, value: V): KeyValue[KR, VR] = {
        Tuple2ToKeyValue(fn(key, value))
      }
    }
  }

}
