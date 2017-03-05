package com.datamountaineer.avro.kcql

import com.datamountaineer.avro.kcql.AvroKcql._
import com.datamountaineer.avro.kcql.AvroSchemaKcql._
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.util.Utf8

trait AvroFieldValueGetter {

  def get(value: Any, schema: Schema, path: Seq[String]): Option[Any] = {
    path.headOption.map { parent =>
      schema.getType match {
        case Schema.Type.RECORD => if (Option(value).isEmpty) None else fromRecord(value, schema, path)
        case Schema.Type.MAP => if (Option(value).isEmpty) None else fromMap(value, schema, path)
        case Schema.Type.UNION => get(value, schema.fromUnion(), path)
        case _ => throw new IllegalArgumentException(s"Can't select $parent field from schema:$schema")
      }
    }.getOrElse {
      schema.getType match {
        case Schema.Type.BOOLEAN | Schema.Type.NULL |
             Schema.Type.DOUBLE | Schema.Type.FLOAT |
             Schema.Type.LONG | Schema.Type.INT |
             Schema.Type.ENUM | Schema.Type.BYTES |
             Schema.Type.FIXED => Option(value)

        case Schema.Type.STRING => Option(new Utf8(value.toString).asInstanceOf[Any]) //yes UTF8

        case Schema.Type.UNION => get(value, schema.fromUnion(), path)

        case Schema.Type.ARRAY | Schema.Type.MAP | Schema.Type.RECORD =>
          throw new IllegalArgumentException(s"Can't select an element from an array(schema:$schema)")

        case other => throw new IllegalArgumentException(s"Invalid Avro schema type:$other")
      }
    }
  }


  private def fromRecord(value: Any, schema: Schema, path: Seq[String]) = {
    val field = Option(schema.getField(path.head))
      .getOrElse(throw new IllegalArgumentException(s"Can't find field:${path.head} in schema:$schema"))
    val v = value.asInstanceOf[IndexedRecord].get(path.head)
    get(v, field.schema(), path.tail)
  }


  private def fromMap(value: Any, schema: Schema, path: Seq[String]) = {
    val field = Option(schema.getField(path.head))
      .getOrElse(throw new IllegalArgumentException(s"Can't find field:${path.head} in schema:$schema"))
    val v = value.asInstanceOf[IndexedRecord].get(path.head)
    get(v, field.schema(), path.tail)
  }

}
