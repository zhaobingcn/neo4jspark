package com.neo4j.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.neo4j.spark._

/**
  * Created by zhzy on 17-3-3.
  */
object connector{

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("mySpark").set("spark.neo4j.bolt.password","654321")
    val sc = new SparkContext(conf)
//    val neo = Neo4j(sc)
//    val neordd = neo.cypher("match (n:Person) return n.name").loadRowRdd
//    neordd.map(t => "Name:" + t(0)).foreach(n => println(n))
//    createGraph(sc)
  }

  def createGraph(sc: SparkContext): Unit ={

    val neo = Neo4j(sc)
    val cypher1 = "UNWIND range(1, 1000) AS row" +
      "CREATE (:Person {id: row, name: 'name' + row, age: row % 100})"
    val cypher2 = "UNWIND range(1, 1000) AS row" +
      "   MATCH (n)  WHERE id(n) = row MATCH (m)" +
      "  WHERE id(m) = toInt(rand() * 1000)" +
      " CREATE (n)-[:KNOWS]->(m)"

    val a = neo.cypher(cypher1).loadRowRdd
    val b = neo.cypher(cypher2).loadRowRdd
  }

  def someFuncation(sc:SparkContext):Unit={
    val rdd =
  }


}
