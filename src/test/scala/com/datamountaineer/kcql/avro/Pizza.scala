package com.datamountaineer.kcql.avro

case class Ingredient(name: String, sugar: Double, fat: Double)

case class Pizza(name: String, ingredients: Seq[Ingredient], vegetarian: Boolean, vegan: Boolean, calories: Int)

