package com.datamountaineer.kcql.cluster

import java.util.Properties

import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel
import io.confluent.kafka.schemaregistry.client.rest.RestService
import io.confluent.kafka.schemaregistry.rest.{SchemaRegistryConfig, SchemaRegistryRestApplication}
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry
import io.confluent.kafka.schemaregistry.zookeeper.SchemaRegistryIdentity
import org.eclipse.jetty.server.Server

class SchemaRegistryService(val port: Int,
                            val zookeeperConnection: String,
                            val kafkaTopic: String,
                            val avroCompatibilityLevel: AvroCompatibilityLevel,
                            val masterEligibility: Boolean) {

  private val app = new SchemaRegistryRestApplication({
    val prop = new Properties
    prop.setProperty("port", port.asInstanceOf[Integer].toString)
    prop.setProperty(SchemaRegistryConfig.KAFKASTORE_CONNECTION_URL_CONFIG, zookeeperConnection)
    prop.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, kafkaTopic)
    prop.put(SchemaRegistryConfig.COMPATIBILITY_CONFIG, avroCompatibilityLevel.toString)
    prop.put(SchemaRegistryConfig.MASTER_ELIGIBILITY, masterEligibility.asInstanceOf[AnyRef])

    prop
  })

  val restServer: Server = app.createServer
  restServer.start()

  var Endpoint: String = {
    val uri = restServer.getURI.toString
    if (uri.endsWith("/")) uri.substring(0, uri.length - 1)
    else uri
  }

  val restClient = new RestService(Endpoint)

  def close() {
    if (restServer != null) {
      restServer.stop()
      restServer.join()
    }
  }

  def isMaster: Boolean = app.schemaRegistry.isMaster

  def setMaster(schemaRegistryIdentity: SchemaRegistryIdentity) {
    app.schemaRegistry.setMaster(schemaRegistryIdentity)
  }

  def myIdentity: SchemaRegistryIdentity = app.schemaRegistry.myIdentity

  def masterIdentity: SchemaRegistryIdentity = app.schemaRegistry.masterIdentity

  def schemaRegistry: SchemaRegistry = app.schemaRegistry
}
