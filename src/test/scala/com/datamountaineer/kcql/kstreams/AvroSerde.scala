package com.datamountaineer.kcql.kstreams

import java.util

import com.sksamuel.avro4s.{FromRecord, RecordFormat, ToRecord}
import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}


class AvroSerde[T <: Product]()(
  implicit toRecord: ToRecord[T], fromRecord: FromRecord[T]) extends Serde[T] {
  private val s = new Serializer[T] {
    private val inner = new KafkaAvroSerializer()

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
      inner.configure(configs, isKey)
    }

    override def serialize(topic: String, data: T): Array[Byte] = {
      inner.serialize(topic, data)
    }


    override def close(): Unit = inner.close()
  }
  private val recordFormat = RecordFormat[T]

  val d = new Deserializer[T] {
    private val inner = new KafkaAvroDeserializer()

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ???

    override def close(): Unit = inner.close()

    override def deserialize(topic: String, data: Array[Byte]): T = {
      val record = inner.deserialize(topic, data).asInstanceOf[GenericRecord]
      recordFormat.from(record)
    }
  }

  def configure(map: util.Map[String, _], isKey: Boolean) = {
    s.configure(map, isKey)
    d.configure(map, isKey)
  }

  def close = {
    s.close
    d.close
  }

  def serializer: Serializer[T] = s

  def deserializer: Deserializer[T] = d
}


class GenericAvroSerde extends Serde[GenericRecord] {
  private val s = new Serializer[GenericRecord] {
    private val inner = new KafkaAvroSerializer()

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
      inner.configure(configs, isKey)
    }

    override def serialize(topic: String, data: GenericRecord): Array[Byte] = {
      inner.serialize(topic, data)
    }

    override def close(): Unit = inner.close()
  }

  val d = new Deserializer[GenericRecord] {
    private val inner = new KafkaAvroDeserializer()

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
      inner.configure(configs, isKey)
    }

    override def close(): Unit = inner.close()

    override def deserialize(topic: String, data: Array[Byte]): GenericRecord = {
      inner.deserialize(topic, data).asInstanceOf[GenericRecord]
    }
  }

  def configure(map: util.Map[String, _], isKey: Boolean) = {
    s.configure(map, isKey)
    d.configure(map, isKey)
  }

  def close = {
    s.close
    d.close
  }

  def serializer: Serializer[GenericRecord] = s

  def deserializer: Deserializer[GenericRecord] = d
}