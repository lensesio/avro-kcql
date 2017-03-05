package com.datamountaineer.kcql.kstreams

import com.sksamuel.avro4s.RecordFormat
import org.apache.avro.generic.GenericRecord

case class Order(id: Int, created: Long, product: String, buySell: String, price: Double)

object Order {
  private val avroFormat = RecordFormat[Order]

  implicit class ToAvroConverter(val order: Order) extends AnyVal {
    def toAvro: GenericRecord = {
      avroFormat.to(order)
    }
  }

}