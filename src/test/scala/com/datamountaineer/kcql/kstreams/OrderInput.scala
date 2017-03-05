package com.datamountaineer.kcql.kstreams

import com.sksamuel.avro4s.{FromRecord, RecordFormat, ToRecord, ToValue}
import org.apache.avro.generic.GenericRecord


case class OrderInput(i: Int,
                      metadata: OrderInputMetadata,
                      price: Double)

case class OrderInputMetadata(timestamp: Long,
                              byUser: String,
                              buySell: String)

object OrderInput {
  private implicit val toRecordMetadata=ToRecord[OrderInputMetadata]
  private implicit val fromRecordMetadata=FromRecord[OrderInputMetadata]

  private implicit val toRecord=ToRecord[OrderInput]
  private implicit val fromRecord=FromRecord[OrderInput]
  private implicit val toValue=ToValue[OrderInput]

  private val avroFormat = RecordFormat[OrderInput]

  implicit class ToAvroConverter(val order: OrderInput) extends AnyVal {
    def toAvro: GenericRecord = {
      avroFormat.to(order)
    }
  }


  implicit def convertToAvro(order: OrderInput): GenericRecord = {
    avroFormat.to(order)
  }
}

class OrderInputSerde()(implicit fromRecord: FromRecord[OrderInput], toRecord: ToRecord[OrderInput]) extends AvroSerde[OrderInput]