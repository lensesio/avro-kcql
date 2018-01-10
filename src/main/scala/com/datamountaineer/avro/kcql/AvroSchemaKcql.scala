package com.datamountaineer.avro.kcql

import com.datamountaineer.kcql.{Kcql, Field => KcqlField}
import org.apache.avro.Schema
import org.apache.avro.Schema.Field

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object AvroSchemaKcql {

  implicit class AvroSchemaKcqlConverter(val schema: Schema) extends AnyVal {
    /**
      * This assumes a null, type union. probably better to look at the value and work out the schema
      */
    def fromUnion(): Schema = {
      schema.getTypes.asScala.toList match {
        case actualSchema +: Nil => actualSchema
        case List(n, actualSchema) if n.getType == Schema.Type.NULL => actualSchema
        case List(actualSchema, n) if n.getType == Schema.Type.NULL => actualSchema
        case _ => throw new IllegalArgumentException("Unions has one specific type and null")
      }
    }

    def getFields(path: Seq[String]): Seq[Field] = {
      def navigate(current: Schema, parents: Seq[String]): Seq[Field] = {
        if (Option(parents).isEmpty || parents.isEmpty) {
          current.getType match {
            case Schema.Type.RECORD => current.getFields.asScala
            case Schema.Type.UNION => navigate(current.fromUnion(), parents)
            case Schema.Type.MAP => throw new IllegalArgumentException(s"Can't select fields ${path.mkString(".")} since it resolved to a Map($current)")
            case _ => throw new IllegalArgumentException(s"Can't select fields ${path.mkString(".")} from schema:$current ")
          }
        } else {
          current.getType match {
            case Schema.Type.RECORD =>
              val field = Option(current.getField(parents.head))
                .getOrElse(throw new IllegalArgumentException(s"Can't find field ${parents.head} in schema:$current"))
              navigate(field.schema(), parents.tail)
            case Schema.Type.UNION => navigate(schema.fromUnion(), parents)
            case _ => throw new IllegalArgumentException(s"Can't select fields ${path.mkString(".")} from schema:$current ")
          }
        }
      }

      navigate(schema, path)
    }

    def fromPath(path: Seq[String]): Seq[Field] = {
      AvroSchemaExtension.fromPath(schema, path)
    }

    def copy(kcql: Kcql): Schema = {
      if (kcql.hasRetainStructure) {
        implicit val kcqlContext = new KcqlContext(kcql.getFields.asScala)
        copy
      }
      else {
        flatten(kcql.getFields.asScala)
      }
    }

    def copy()(implicit kcqlContext: KcqlContext): Schema = {
      AvroSchemaExtension.copy(schema, Vector.empty)
    }

    def flatten(fields: Seq[KcqlField]): Schema = {
      def allowOnlyStarSelection() = {
        fields match {
          case Seq(f) if f.getName == "*" => schema
          case _ => throw new IllegalArgumentException(s"You can't select fields from schema:$schema")
        }
      }

      schema.getType match {
        case Schema.Type.ARRAY | Schema.Type.MAP => throw new IllegalArgumentException(s"Can't flattent schema type:${schema.getType}")
        case Schema.Type.BOOLEAN | Schema.Type.BYTES |
             Schema.Type.DOUBLE | Schema.Type.ENUM |
             Schema.Type.FIXED | Schema.Type.FLOAT |
             Schema.Type.INT | Schema.Type.LONG |
             Schema.Type.NULL | Schema.Type.STRING => allowOnlyStarSelection()

        //case Schema.Type.MAP => allowOnlyStarSelection()
        case Schema.Type.UNION => schema.fromUnion().flatten(fields)
        case Schema.Type.RECORD =>
          fields match {
            case Seq(f) if f.getName == "*" => schema
            case _ => createRecordSchemaForFlatten(fields)
          }
      }
    }

    def copyProperties(from: Schema): Schema = {
      from.getType match {
        case Schema.Type.RECORD | Schema.Type.FIXED | Schema.Type.ENUM =>
          from.getAliases.asScala.foreach(schema.addAlias)
        case _ =>
      }
      from.getObjectProps.asScala.foreach { case (prop: String, value: Any) =>
        schema.addProp(prop, value)
      }
      schema
    }

    private def createRecordSchemaForFlatten(fields: Seq[KcqlField]): Schema = {
      val newSchema = Schema.createRecord(schema.getName, schema.getDoc, schema.getNamespace, false)
      val fieldParentsMap = fields.foldLeft(Map.empty[String, ArrayBuffer[String]]) { case (map, f) =>
        val key = Option(f.getParentFields.asScala).map(_.mkString(".")).getOrElse("")
        val buffer = map.getOrElse(key, ArrayBuffer.empty[String])
        if (buffer.contains(f.getName)) {
          throw new IllegalArgumentException(s"You have defined the field ${
            if (f.hasParents) {
              f.getParentFields.asScala.mkString(".") + "." + f.getName
            } else {
              f.getName
            }
          } more than once!")
        }
        buffer += f.getName
        map + (key -> buffer)
      }

      val colsMap = collection.mutable.Map.empty[String, Int]

      def getNextFieldName(fieldName: String): String = {
        colsMap.get(fieldName).map { v =>
          colsMap.put(fieldName, v + 1)
          s"${fieldName}_${v + 1}"
        }.getOrElse {
          colsMap.put(fieldName, 0)
          fieldName
        }
      }

      val newFields = fields.flatMap {

        case field if field.getName == "*" =>
          val siblings = fieldParentsMap.get(Option(field.getParentFields.asScala).map(_.mkString(".")).getOrElse(""))
          Option(field.getParentFields.asScala)
            .map { p =>
              val s = schema.fromPath(p)
                .headOption
                .getOrElse(throw new IllegalArgumentException(s"Can't find field ${p.mkString(".")} in schema:$schema"))
                .schema()

              s.getType match {
                case Schema.Type.UNION=> s.fromUnion()
                case Schema.Type.RECORD=> s
                case other=>
                  throw new IllegalArgumentException(s"Field selection ${p.mkString(".")} resolves to schema type:$other. Only RECORD type is allowed")
              }
            }.getOrElse(schema)
            .getFields.asScala
            .withFilter { f =>
              siblings.collect { case s if s.contains(f.name()) => false }.getOrElse(true)
            }
            .map { f =>
              AvroSchemaExtension.checkAllowedSchemas(f.schema(), field)
              new Field(getNextFieldName(f.name()), f.schema(), f.doc(), f.defaultVal())
            }

        case field if field.hasParents =>
          schema.fromPath(field.getParentFields.asScala.toVector :+ field.getName)
            .map { extracted =>
              require(extracted != null, s"Invalid field:${(field.getParentFields.asScala.toVector :+ field.getName).mkString(".")}")
              AvroSchemaExtension.checkAllowedSchemas(extracted.schema(), field)
              if (field.getAlias == "*") {
                new Field(getNextFieldName(extracted.name()), extracted.schema(), extracted.doc(), extracted.defaultVal())
              } else {
                new Field(getNextFieldName(field.getAlias), extracted.schema(), extracted.doc(), extracted.defaultVal())
              }
            }

        case field =>
          val originalField = Option(schema.getField(field.getName))
            .getOrElse(throw new IllegalArgumentException(s"Can't find field:${field.getName} in schema:$schema"))
          AvroSchemaExtension.checkAllowedSchemas(originalField.schema(), field)
          Seq(new Field(getNextFieldName(field.getAlias), originalField.schema(), originalField.doc(), originalField.defaultVal()))
      }


      newSchema.setFields(newFields.asJava)
      newSchema.copyProperties(schema)
    }
  }

  private object AvroSchemaExtension {
    def copy(from: Schema, parents: Vector[String])(implicit kcqlContext: KcqlContext): Schema = {
      from.getType match {
        case Schema.Type.RECORD => createRecordSchema(from, parents)
        case Schema.Type.ARRAY =>
          val newSchema = Schema.createArray(copy(from.getElementType, parents))
          newSchema.copyProperties(from)
        case Schema.Type.MAP =>
          val elementSchema = copy(from.getValueType, parents)
          val newSchema = Schema.createMap(elementSchema)
          newSchema.copyProperties(from)

        case Schema.Type.UNION =>
          val newSchema = Schema.createUnion((from.getTypes.asScala.map(copy(_, parents))).asJava)
          newSchema.copyProperties(from)

        case _ => from
      }
    }

    private def createRecordSchema(from: Schema, parents: Vector[String])(implicit kcqlContext: KcqlContext): Schema = {
      val newSchema = Schema.createRecord(from.getName, from.getDoc, from.getNamespace, false)

      val fields = kcqlContext.getFieldsForPath(parents)
      val newFields: Seq[Schema.Field] = fields match {
        case Seq() =>
          from.getFields.asScala
            .map { schemaField =>
              val newSchema = copy(schemaField.schema(), parents :+ schemaField.name)
              val newField = new org.apache.avro.Schema.Field(schemaField.name, newSchema, schemaField.doc(), schemaField.defaultVal())
              schemaField.aliases().asScala.foreach(newField.addAlias)
              newField
            }

        case Seq(Left(f)) if f.getName == "*" =>
          from.getFields.asScala.map { schemaField =>
            val newSchema = copy(schemaField.schema(), parents :+ schemaField.name)
            val newField = new org.apache.avro.Schema.Field(schemaField.name, newSchema, schemaField.doc(), schemaField.defaultVal())
            schemaField.aliases().asScala.foreach(newField.addAlias)
            newField
          }
        case other =>
          fields.flatMap {
            case Left(field) if field.getName == "*" =>
              from.getFields.asScala
                .withFilter(f => !fields.exists(e => e.isLeft && e.left.get.getName == f.name))
                .map { f =>
                  val newSchema = copy(f.schema(), parents :+ f.name)
                  newSchema.copyProperties(f.schema())
                  val newField = new org.apache.avro.Schema.Field(f.name(), newSchema, f.doc(), f.defaultVal())
                  newField
                }.toList

            case Left(field) =>
              val originalField = Option(from.getField(field.getName)).getOrElse(
                throw new IllegalArgumentException(s"Invalid selecting ${parents.mkString("", ".", ".")}${field.getName}. Schema doesn't contain it."))
              val newSchema = copy(originalField.schema(), parents :+ field.getName)
              newSchema.copyProperties(originalField.schema())
              val newField = new org.apache.avro.Schema.Field(field.getAlias, newSchema, originalField.doc(), originalField.defaultVal())
              Seq(newField)

            case Right(field) =>
              val originalField = Option(from.getField(field))
                .getOrElse(throw new IllegalArgumentException(s"Invalid selecting ${parents.mkString("", ".", ".")}$field. Schema doesn't contain it."))
              val newSchema = copy(originalField.schema(), parents :+ field)
              newSchema.copyProperties(originalField.schema())
              val newField = new org.apache.avro.Schema.Field(field, newSchema, originalField.doc(), originalField.defaultVal())
              Seq(newField)
          }
      }

      newSchema.setFields(newFields.asJava)
      newSchema.copyProperties(from)
    }

    def fromPath(from: Schema, path: Seq[String]): Seq[Field] = {
      path match {
        case Seq(field) if field == "*" =>
          from.getType match {
            case Schema.Type.UNION => fromPath(from.fromUnion(), path)
            case Schema.Type.RECORD => from.getFields.asScala
            case other => throw new IllegalArgumentException(s"Can't select field:$field from ${other.toString}")
          }
        case Seq(field) =>
          from.getType match {
            case Schema.Type.UNION => fromPath(from.fromUnion(), path)
            case Schema.Type.RECORD => Seq(from.getField(field))
            case other => throw new IllegalArgumentException(s"Can't select field:$field from ${other.toString}")
          }
        case head +: tail =>
          val next = Option(from.getField(head))
            .getOrElse(throw new IllegalArgumentException(s"Can't find the field '$head'"))
          fromPath(next.schema(), tail)
      }
    }

    @tailrec
    def checkAllowedSchemas(schema: Schema, field: KcqlField): Unit = {
      schema.getType match {
        case Schema.Type.ARRAY | Schema.Type.MAP => throw new IllegalArgumentException(s"Can't flatten from schema:$schema by selecting '${field.getName}'")
        case Schema.Type.UNION => checkAllowedSchemas(schema.fromUnion(), field)
        case _ =>
      }
    }
  }

}
