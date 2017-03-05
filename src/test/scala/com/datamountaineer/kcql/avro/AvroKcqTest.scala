package com.datamountaineer.kcql.avro

import com.sksamuel.avro4s.{RecordFormat, SchemaFor, ToRecord}
import io.confluent.kafka.serializers.NonRecordContainer
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericContainer, GenericRecord}
import org.scalatest.{Matchers, WordSpec}
import com.datamountaineer.avro.kcql.AvroKcql._

class AvroKcqTest extends WordSpec with Matchers {

  private def compare[T](actual: GenericContainer, t: T)(implicit schemaFor: SchemaFor[T], toRecord: ToRecord[T]) = {
    val expectedSchema = schemaFor().toString()
      .replace("LocalPerson", "Person")
      .replace("LocalAddress", "Address")
      .replace("LocalStreet", "Street")
      .replace("LocalPizza", "Pizza")
      .replace("LocalSimpleAddress", "SimpleAddress")

    actual.getSchema.toString() shouldBe expectedSchema

    val expectedRecord = toRecord.apply(t)
    actual.toString shouldBe expectedRecord.toString
  }

  "AvroKcqlExtractor" should {
    "handle null payload" in {
      null.asInstanceOf[GenericContainer].kcql("SELECT * FROM topic") shouldBe null.asInstanceOf[Any]
    }

    "throw an exception when the parameter is not a GenericRecord or NonRecordContainer" in {
      intercept[IllegalArgumentException] {
        new GenericContainer {
          override def getSchema = null
        }.kcql("SELECT * FROM topic")
      }
    }

    "handle Int avro record" in {
      val container = new NonRecordContainer(Schema.create(Schema.Type.INT), 2000)

      val expected = new NonRecordContainer(Schema.create(Schema.Type.INT), 2000)
      container.kcql("SELECT * FROM topic") shouldBe expected
    }

    "handle Nullable Int avro record with a integer value" in {
      val nullSchema = Schema.create(Schema.Type.NULL)
      val intSchema = Schema.create(Schema.Type.INT)
      val schema = Schema.createUnion(nullSchema, intSchema)

      val container = new NonRecordContainer(schema, 2000)
      val expected = new NonRecordContainer(schema, 2000)

      container.kcql("SELECT * FROM topic") shouldBe expected
    }

    "handle Nullable Int avro record with a null value" in {
      val nullSchema = Schema.create(Schema.Type.NULL)
      val intSchema = Schema.create(Schema.Type.INT)
      val schema = Schema.createUnion(nullSchema, intSchema)

      val container = new NonRecordContainer(schema, null)
      val expected = new NonRecordContainer(schema, null)
      container.kcql("SELECT * FROM topic") shouldBe expected
    }

    "throw an exception when trying to select field of an Int avro record" in {
      val expected = 2000
      val container = new NonRecordContainer(Schema.create(Schema.Type.INT), expected)
      intercept[IllegalArgumentException] {
        container.kcql("SELECT field1 FROM topic")
      }
    }

    "handle 'SELECT name,vegan, calories  FROM topic' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      val schema = SchemaFor[Pizza]()
      val toRecord = ToRecord[Pizza]
      val record = RecordFormat[Pizza].to(pepperoni)

      val actual = record.kcql("SELECT name,vegan, calories FROM topic")

      case class LocalPizza(name: String, vegan: Boolean, calories: Int)
      val expected = LocalPizza(pepperoni.name, pepperoni.vegan, pepperoni.calories)

      compare(actual, expected)
    }

    "handle 'SELECT name as fieldName,vegan as V, calories as C FROM topic' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      val schema = SchemaFor[Pizza]()
      val toRecord = ToRecord[Pizza]
      val record = RecordFormat[Pizza].to(pepperoni)

      val actual = record.kcql("SELECT name as fieldName,vegan as V, calories as C FROM topic")

      case class LocalPizza(fieldName: String, V: Boolean, C: Int)
      val expected = LocalPizza(pepperoni.name, pepperoni.vegan, pepperoni.calories)

      compare(actual, expected)
    }

    "handle 'SELECT calories as C ,vegan as V ,name as fieldName FROM topic' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      val schema = SchemaFor[Pizza]()
      val toRecord = ToRecord[Pizza]
      val record = RecordFormat[Pizza].to(pepperoni)

      val actual = record.kcql("SELECT  calories as C,vegan as V,name as fieldName FROM topic")

      case class LocalPizza(C: Int, V: Boolean, fieldName: String)
      val expected = LocalPizza(pepperoni.calories, pepperoni.vegan, pepperoni.name)

      compare(actual, expected)
    }

    "throw an exception when selecting arrays ' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      val schema = SchemaFor[Pizza]()
      val toRecord = ToRecord[Pizza]
      val record = RecordFormat[Pizza].to(pepperoni)

      intercept[IllegalArgumentException] {
        record.kcql("SELECT *, name as fieldName FROM topic")
      }
    }

    "handle 'SELECT name, address.street.name FROM topic' from record" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))

      val schema = SchemaFor[Person]()
      val toRecord = ToRecord[Person]
      val record = RecordFormat[Person].to(person)

      val actual = record.kcql("SELECT name, address.street.name FROM topic")

      case class LocalPerson(name: String, name_1: String)
      val localPerson = LocalPerson(person.name, person.address.street.name)
      compare(actual, localPerson)
    }

    "handle 'SELECT name, address.street.name as streetName FROM topic' from record" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))

      val schema = SchemaFor[Person]()
      val toRecord = ToRecord[Person]
      val record: GenericRecord = RecordFormat[Person].to(person)

      val actual = record.kcql("SELECT name, address.street.name as streetName FROM topic")

      case class LocalPerson(name: String, streetName: String)
      val localPerson = LocalPerson(person.name, person.address.street.name)
      compare(actual, localPerson)
    }

    "handle 'SELECT name, address.street.name as streetName, address.street2.name as streetName2 FROM topic' from record" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))

      val schema = SchemaFor[Person]()
      val toRecord = ToRecord[Person]
      val record = RecordFormat[Person].to(person)

      val actual = record.kcql("SELECT name, address.street.name as streetName, address.street2.name as streetName2 FROM topic")

      case class LocalPerson(name: String, streetName: String, streetName2: String)
      val localPerson = LocalPerson(person.name, person.address.street.name, person.address.street2.map(_.name).orNull)
      compare(actual, localPerson)
    }

    "handle 'SELECT name, address.street.*, address.street2.name as streetName2 FROM topic' from record" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))

      val schema = SchemaFor[Person]()
      val toRecord = ToRecord[Person]
      val record = RecordFormat[Person].to(person)

      val actual = record.kcql("SELECT name, address.street.*, address.street2.name as streetName2 FROM topic")

      case class LocalPerson(name: String, name_1: String, streetName2: String)
      val localPerson = LocalPerson(person.name, person.address.street.name, person.address.street2.map(_.name).orNull)
      compare(actual, localPerson)
    }

    "handle 'SELECT name, address.street.*, address.street2.* FROM topic' from record" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))

      val schema = SchemaFor[Person]()
      val toRecord = ToRecord[Person]
      val record = RecordFormat[Person].to(person)

      val actual = record.kcql("SELECT name, address.street.*, address.street2.* FROM topic")

      case class LocalPerson(name: String, name_1: String, name_2: String)
      val localPerson = LocalPerson(person.name, person.address.street.name, person.address.street2.map(_.name).orNull)
      compare(actual, localPerson)

      val person1 = Person("Rick", Address(Street("Rock St"), Some(Street("412 East")), "MtV", "CA", "94041", "USA"))
      val record1 = RecordFormat[Person].to(person1)

      val actual1 = record.kcql("SELECT name, address.street.*, address.street2.* FROM topic")
      val localPerson1 = LocalPerson(person.name, person.address.street.name, person.address.street2.map(_.name).orNull)
      compare(actual1, localPerson1)

    }

    "handle 'SELECT address.state, address.city,name, address.street.name FROM topic' from record" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))

      val schema = SchemaFor[Person]()
      val toRecord = ToRecord[Person]
      val record = RecordFormat[Person].to(person)

      val actual = record.kcql("SELECT address.state, address.city,name, address.street.name FROM topic")

      case class LocalPerson(state: String, city: String, name: String, name_1: String)
      val localPerson = LocalPerson(person.address.state, person.address.city, person.name, person.address.street.name)
      compare(actual, localPerson)
    }

    "handle 'SELECT address.state as S, address.city as C,name, address.street.name FROM topic' from record" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))

      val schema = SchemaFor[Person]()
      val toRecord = ToRecord[Person]
      val record = RecordFormat[Person].to(person)

      val actual = record.kcql("SELECT address.state as S, address.city as C,name, address.street.name FROM topic")

      case class LocalPerson(S: String, C: String, name: String, name_1: String)
      val localPerson = LocalPerson(person.address.state, person.address.city, person.name, person.address.street.name)
      compare(actual, localPerson)
    }

    "throw an exception if the field doesn't exist in the schema" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))

      val schema = SchemaFor[Person]()
      val toRecord = ToRecord[Person]
      val record = RecordFormat[Person].to(person)

      intercept[IllegalArgumentException] {
        record.kcql("SELECT address.bam, address.city,name, address.street.name FROM topic")
      }
    }


    "handle 'SELECT * FROM simpleAddress' from record" in {
      val address = SimpleAddress("Rock St", "MtV", "CA", "94041", "USA")

      val schema = SchemaFor[SimpleAddress]()
      val toRecord = ToRecord[SimpleAddress]
      val record = RecordFormat[SimpleAddress].to(address)

      val actual = record.kcql("SELECT * FROM simpleAddress")
      actual shouldBe record
    }

    "handle 'SELECT street as S, city, state, zip as Z, country as C  FROM simpleAddress' from record" in {
      val address = SimpleAddress("Rock St", "MtV", "CA", "94041", "USA")

      val schema = SchemaFor[SimpleAddress]()
      val toRecord = ToRecord[SimpleAddress]
      val record = RecordFormat[SimpleAddress].to(address)

      val actual = record.kcql("SELECT street as S, city, state, zip as Z, country as C  FROM simpleAddress")

      case class LocalSimpleAddress(S: String, city: String, state: String, Z: String, C: String)
      val expected = LocalSimpleAddress(address.street, address.city, address.state, address.zip, address.country)

      compare(actual, expected)
    }

    "handle 'SELECT  zip as Z, * FROM simpleAddress' from record" in {
      val address = SimpleAddress("Rock St", "MtV", "CA", "94041", "USA")

      val schema = SchemaFor[SimpleAddress]()
      val toRecord = ToRecord[SimpleAddress]
      val record = RecordFormat[SimpleAddress].to(address)

      val actual = record.kcql("SELECT zip as Z, * FROM simpleAddress")

      case class LocalSimpleAddress(Z: String, street: String, city: String, state: String, country: String)
      val expected = LocalSimpleAddress(address.zip, address.street, address.city, address.state, address.country)

      compare(actual, expected)
    }

    "handle 'SELECT  zip as Z, *, state as S FROM simpleAddress' from record" in {
      val address = SimpleAddress("Rock St", "MtV", "CA", "94041", "USA")

      val schema = SchemaFor[SimpleAddress]()
      val toRecord = ToRecord[SimpleAddress]
      val record = RecordFormat[SimpleAddress].to(address)

      val actual = record.kcql("SELECT zip as Z, *, state as S FROM simpleAddress")

      case class LocalSimpleAddress(Z: String, street: String, city: String, country: String, S: String)
      val expected = LocalSimpleAddress(address.zip, address.street, address.city, address.country, address.state)

      compare(actual, expected)
    }
  }
}
