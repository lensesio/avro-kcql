package com.datamountaineer.avro.kcql

import java.util

import com.datamountaineer.avro.kcql.AvroSchemaKcql._
import com.datamountaineer.kcql.{Kcql, Field => KcqlField}
import io.confluent.kafka.serializers.NonRecordContainer
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericContainer, GenericData, IndexedRecord}
import org.apache.avro.util.Utf8

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object AvroKcql extends AvroFieldValueGetter {
  private val StringSchema = Schema.create(Schema.Type.STRING)

  implicit class IndexedRecordExtension(val record: IndexedRecord) extends AnyVal {
    def get(fieldName: String): Any = {
      Option(record.getSchema.getField(fieldName))
        .map(f => record.get(f.pos))
        .orNull
    }
  }

  implicit class GenericContainerKcqlConverter(val from: GenericContainer) extends AnyVal {
    def kcql(query: String): GenericContainer = {
      val k = Kcql.parse(query)
      kcql(k)
    }

    def kcql(query: Kcql): GenericContainer = {
      Option(from).map { _ =>
        from match {
          case _: NonRecordContainer =>
          case _: IndexedRecord =>
          case other => throw new IllegalArgumentException(s"Avro type ${other.getClass.getName} is not supported")
        }
        if (query.hasRetainStructure) {
          implicit val kcqlContext = new KcqlContext(query.getFields)
          val schema = from.getSchema.copy()
          kcql(schema)
        } else {
          implicit val fields = query.getFields.asScala
          val schema = from.getSchema.flatten(query.getFields)
          kcqlFlatten(schema)
        }
      }
    }.orNull

    def kcql(fields: Seq[KcqlField], flatten: Boolean): GenericContainer = {
      Option(from).map { _ =>
        from match {
          case _: NonRecordContainer =>
          case _: IndexedRecord =>
          case other => throw new IllegalArgumentException(s"Avro type ${other.getClass.getName} is not supported")
        }
        if (!flatten) {
          implicit val kcqlContext = new KcqlContext(fields)
          val schema = from.getSchema.copy()
          kcql(schema)
        } else {
          implicit val f = fields
          val schema = from.getSchema.flatten(fields)
          kcqlFlatten(schema)
        }
      }.orNull
    }

    def kcql(newSchema: Schema)(implicit kcqlContext: KcqlContext): GenericContainer = {
      from match {
        case container: NonRecordContainer =>
          kcqlContext.fields match {
            case Seq(f) if f.getName == "*" => container
            case _ => throw new IllegalArgumentException(s"Can't select specific fields from primitive avro record:${from.getClass.getName}")
          }
        case record: IndexedRecord => fromRecord(record, record.getSchema, newSchema, Vector.empty[String]).asInstanceOf[IndexedRecord]
        case other => throw new IllegalArgumentException(s"${other.getClass.getName} is not handled")
      }
    }

    def kcqlFlatten(newSchema: Schema)(implicit fields: Seq[KcqlField]): GenericContainer = {
      from match {
        case container: NonRecordContainer => flattenPrimitive(container)
        case record: IndexedRecord => flattenIndexedRecord(record, newSchema)
        case other => throw new IllegalArgumentException(s"${other.getClass.getName} is not handled")
      }
    }

    def flattenPrimitive(value: NonRecordContainer)(implicit fields: Seq[KcqlField]): GenericContainer = {
      fields match {
        case Seq(f) if f.getName == "*" => value
        case _ => throw new IllegalArgumentException(s"Can't select multiple fields from ${value.getSchema}")
      }
    }

    def flattenIndexedRecord(record: IndexedRecord, newSchema: Schema)(implicit fields: Seq[KcqlField]): GenericContainer = {
      val fieldsParentMap = fields.foldLeft(Map.empty[String, ArrayBuffer[String]]) { case (map, f) =>
        val key = Option(f.getParentFields).map(_.mkString(".")).getOrElse("")
        val buffer = map.getOrElse(key, ArrayBuffer.empty[String])
        buffer += f.getName
        map + (key -> buffer)
      }

      val newRecord = new GenericData.Record(newSchema)
      fields.foldLeft(0) { case (index, field) =>
        if (field.getName == "*") {
          val sourceFields = record.getSchema.getFields(Option(field.getParentFields).map(_.asScala).getOrElse(Seq.empty))
          val key = Option(field.getParentFields).map(_.mkString(".")).getOrElse("")
          sourceFields
            .filter { f =>
              fieldsParentMap.get(key).forall(!_.contains(f.name()))
            }.foldLeft(index) { case (i, f) =>
            val extractedValue = get(record, record.getSchema, Option(field.getParentFields).map(_.asScala).getOrElse(Seq.empty[String]) :+ f.name())
            newRecord.put(i, extractedValue.orNull)
            i + 1
          }
        }
        else {
          val extractedValue = get(record, record.getSchema, Option(field.getParentFields).map(_.asScala).getOrElse(Seq.empty[String]) :+ field.getName)
          newRecord.put(index, extractedValue.orNull)
          index + 1
        }
      }
      newRecord
    }

    def fromUnion(value: Any,
                  fromSchema: Schema,
                  targetSchema: Schema,
                  parents: Seq[String])(implicit kcqlContext: KcqlContext): Any = {
      from(value, fromSchema.fromUnion(), targetSchema.fromUnion(), parents)
    }


    def fromArray(value: Any,
                  schema: Schema,
                  targetSchema:
                  Schema,
                  parents: Seq[String])(implicit kcqlContext: KcqlContext): Any = {
      value match {
        case c: java.util.Collection[_] =>
          c.foldLeft(new java.util.ArrayList[Any](c.size())) { (acc, e) =>
            acc.add(from(e, schema.getElementType, targetSchema.getElementType, parents))
            acc
          }
        case other => throw new IllegalArgumentException(s"${other.getClass.getName} is not handled")
      }
    }

    def fromRecord(value: Any,
                   schema: Schema,
                   targetSchema: Schema,
                   parents: Seq[String])(implicit kcqlContext: KcqlContext): Any = {
      val record = value.asInstanceOf[IndexedRecord]
      val fields = kcqlContext.getFieldsForPath(parents)
      //.get(parents.head)
      val fieldsTuple = fields.headOption.map { _ =>
        fields.flatMap {
          case Left(field) if field.getName == "*" =>
            val filteredFields = fields.collect { case Left(f) if f.getName != "*" => f.getName }.toSet

            schema.getFields
              .withFilter(f => !filteredFields.contains(f.name()))
              .map { f =>
                val sourceField = Option(schema.getField(f.name))
                  .getOrElse(throw new IllegalArgumentException(s"${f.name} was not found in $schema"))
                sourceField -> f
              }

          case Left(field) =>
            val sourceField = Option(schema.getField(field.getName))
              .getOrElse(throw new IllegalArgumentException(s"${field.getName} can't be found in $schema"))

            val targetField = Option(targetSchema.getField(field.getAlias))
              .getOrElse(throw new IllegalArgumentException(s"${field.getName} can't be found in $targetSchema"))

            List(sourceField -> targetField)

          case Right(field) =>
            val sourceField = Option(schema.getField(field))
              .getOrElse(throw new IllegalArgumentException(s"$field can't be found in $schema"))

            val targetField = Option(targetSchema.getField(field))
              .getOrElse(throw new IllegalArgumentException(s"$field can't be found in $targetSchema"))

            List(sourceField -> targetField)

        }
      }.getOrElse {
        targetSchema.getFields
          .map { f =>
            val sourceField = Option(schema.getField(f.name))
              .getOrElse(throw new IllegalArgumentException(s"Can't find the field ${f.name} in ${schema.getFields.map(_.name()).mkString(",")}"))
            sourceField -> f
          }
      }

      val newRecord = new GenericData.Record(targetSchema)
      fieldsTuple.foreach { case (sourceField, targetField) =>
        val v = from(record.get(sourceField.name()),
          sourceField.schema(),
          targetField.schema(),
          parents :+ sourceField.name)
        newRecord.put(targetField.name(), v)
      }
      newRecord
    }

    def fromMap(value: Any, fromSchema: Schema,
                targetSchema: Schema,
                parents: Seq[String])(implicit kcqlContext: KcqlContext): Any = {
      Option(value.asInstanceOf[java.util.Map[CharSequence, Any]]).map { map =>
        val newMap = new util.HashMap[CharSequence, Any]()
        //check if there are keys for this
        val fields = kcqlContext.getFieldsForPath(parents)
        val initialMap = {
          if (fields.exists(f => f.isLeft && f.left.get.getName == "*")) {
            map.keySet().map(k => k.toString -> k.toString).toMap
          } else {
            Map.empty[String, String]
          }
        }

        fields.headOption.map { _ =>
          fields.filterNot(f => f.isLeft && f.left.get.getName != "*")
            .foldLeft(initialMap) {
              case (m, Left(f)) => m + (f.getName -> f.getAlias)
              case (m, Right(f)) => m + (f -> f)
            }
        }
          .getOrElse(map.keySet().map(k => k.toString -> k.toString).toMap)
          .foreach { case (key, alias) =>
            Option(map.get(key)).foreach { v =>
              newMap.put(
                from(key, StringSchema, StringSchema, null).asInstanceOf[CharSequence],
                from(v, fromSchema.getValueType, targetSchema.getValueType, parents))
            }
          }
        newMap
      }.orNull
    }

    def from(from: Any,
             fromSchema: Schema,
             targetSchema: Schema,
             parents: Seq[String])(implicit kcqlContext: KcqlContext): Any = {
      Option(from).map { _ =>
        implicit val s = fromSchema
        fromSchema.getType match {
          case Schema.Type.BOOLEAN | Schema.Type.NULL |
               Schema.Type.DOUBLE | Schema.Type.FLOAT |
               Schema.Type.LONG | Schema.Type.INT |
               Schema.Type.ENUM | Schema.Type.BYTES | Schema.Type.FIXED => from

          case Schema.Type.STRING => new Utf8(from.toString).asInstanceOf[Any] //yes UTF8

          case Schema.Type.UNION => fromUnion(from, fromSchema, targetSchema, parents)

          case Schema.Type.ARRAY => fromArray(from, fromSchema, targetSchema, parents)

          case Schema.Type.MAP => fromMap(from, fromSchema, targetSchema, parents)

          case Schema.Type.RECORD => fromRecord(from, fromSchema, targetSchema, parents)

          case other => throw new IllegalArgumentException(s"Invalid Avro schema type:$other")
        }
      }.orNull
    }
  }

}
