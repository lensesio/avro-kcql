package com.datamountaineer.kcql.kstreams

import java.util.Properties

import com.datamountaineer.avro.kcql.AvroKcql._
import com.datamountaineer.kcql.cluster.KCluster
import com.datamountaineer.kcql.kstreams.OrderInput._
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

class KStreamTransformationTest extends WordSpec with Matchers with BeforeAndAfterAll {
  var cluster: KCluster = _

  override def beforeAll(): Unit = {
    cluster = new KCluster()
  }

  override def afterAll(): Unit = {
    cluster.close()
  }

  def getProducerProps() = {
    val props = new Properties()
    props.put("bootstrap.servers", cluster.BrokersList)
    props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer")
    props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer")
    props.put("schema.registry.url", cluster.SchemaRegistryService.get.Endpoint)
    props
  }

  "AvroKcql" should {
    "sample using kcql to avoid having an extra topic" ignore {

      cluster.createTopic("oders_in")

      val topic = "orders-topic"

      val producer = new KafkaProducer[String, GenericRecord](getProducerProps())

      Seq(
        OrderInput(1, OrderInputMetadata(System.currentTimeMillis(), "peter", "buy"), 100.0),
        OrderInput(2, OrderInputMetadata(System.currentTimeMillis(), "peter", "buy"), 10.0),
        OrderInput(3, OrderInputMetadata(System.currentTimeMillis(), "peter", "buy"), 20.0)
      ).foreach { o =>
        val data = new ProducerRecord[String, GenericRecord](topic, o.toString, o)
        producer.send(data)
      }

      val builder = new KStreamBuilder()

      val streamsConfiguration = new Properties()
      streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "sum-orders-test")
      streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.BrokersList)
      streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, classOf[GenericAvroSerde].getName)

      val input: KStream[String, GenericRecord] = builder.stream("orders-input")
      import KeyValueImplicits._
      val mapped: KStream[String, GenericRecord] = input.map { (k: String, v: GenericRecord) =>
        val result = v.kcql(
          """
            |SELECT i as id,
            |       metadata.timestamp as created,
            |       metadata.buySell,
            |       price
            |FROM topic""".stripMargin).asInstanceOf[GenericRecord]

        (k ,result)
      }

      mapped.to(Serdes.String(), new GenericAvroSerde(), "order")
      val stream: KafkaStreams = new KafkaStreams(builder, streamsConfiguration)
      stream.start()
    }
  }
}
