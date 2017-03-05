package com.datamountaineer.kcql.cluster

import java.util
import java.util.Properties

import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.kafka.clients.consumer.{Consumer, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.collection.JavaConversions._

trait ClusterTestingCapabilities extends WordSpec with Matchers with BeforeAndAfterAll {

  System.setProperty("http.nonProxyHosts", "localhost|0.0.0.0|127.0.0.1")

  val SCHEMA_REGISTRY_URL = "schema.registry.url"

  val kafkaCluster: KCluster = new KCluster()

  protected override def afterAll(): Unit = {
    kafkaCluster.close()
  }

  /** Helpful Producers **/
  def avroAvroProducerProps: Properties = {
    val props = new Properties
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.BrokersList)
    props.put(SCHEMA_REGISTRY_URL, kafkaCluster.SchemaRegistryService.get.Endpoint)
    props
  }

  def stringAvroProducerProps: Properties = {
    val props = new Properties
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.BrokersList)
    props.put(SCHEMA_REGISTRY_URL, kafkaCluster.SchemaRegistryService.get.Endpoint)
    props
  }

  def createProducer(props: Properties): KafkaProducer[String, Any] = {
    new KafkaProducer[String, Any](props)
  }

  /** Helpful Consumers **/
  def stringAvroConsumerProps(group: String = "stringAvroGroup"): Properties = {
    val props = new Properties
    props.put("bootstrap.servers", kafkaCluster.BrokersList)
    props.put("group.id", group)
    props.put("session.timeout.ms", "6000") // default value of group.min.session.timeout.ms.
    props.put("heartbeat.interval.ms", "2000")
    props.put("auto.commit.interval.ms", "1000")
    props.put("auto.offset.reset", "earliest")
    props.put("key.deserializer", classOf[StringDeserializer])
    props.put("value.deserializer", classOf[KafkaAvroDeserializer])
    props.put(SCHEMA_REGISTRY_URL, kafkaCluster.SchemaRegistryService.get.Endpoint)
    props
  }

  def createStringAvroConsumer(props: Properties): KafkaConsumer[String, AnyRef] = {
    new KafkaConsumer[String, AnyRef](props)
  }

  /** Consume **/
  def consumeStringAvro(consumer: Consumer[String, AnyRef], topic: String, numMessages: Int): Seq[AnyRef] = {

    consumer.subscribe(util.Arrays.asList(topic))

    def accum(records: Seq[AnyRef]): Seq[AnyRef] = {
      if (records.size < numMessages) {
        val consumedRecords = consumer.poll(1000)
        accum(consumedRecords.foldLeft(records) { case (acc, r) =>
          acc :+ r.value()
        })
      } else {
        consumer.close()
        records
      }
    }

    accum(Vector.empty)
  }

}
