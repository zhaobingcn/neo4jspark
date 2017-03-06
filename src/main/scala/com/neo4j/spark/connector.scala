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
    val neo = Neo4j(sc)
    val neordd = neo.cypher("match (n:Person) return n.name").loadRowRdd
    val pairRDD = neordd.map(x => x(0))
    pairRDD.foreach(n => println(n))
  }
}
