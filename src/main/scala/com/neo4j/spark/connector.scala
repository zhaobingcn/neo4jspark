package com.neo4j.spark

import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.lib.PageRank
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
//      loadRdds(sc)
      loadDataFrames(sc)
//      loadGraphX(sc)
  }

  def createGraph(sc: SparkContext): Unit ={

    val neo = Neo4j(sc)
    val cypher2 = "UNWIND range(1,100) as id " +
    "CREATE (p:Person {id:id}) WITH collect(p) as people" +
    "UNWIND people as p1" +
     " UNWIND range(1,10) as friend" +
    "WITH p1, people[(p1.id + friend) % size(people)] as p2" +
     " CREATE (p1)-[:KNOWS {years: abs(p2.id - p2.id)}]->(p2)";
    neo.Query(cypher2)
  }

  def loadRDDs(sc:SparkContext):Unit={
    val neo = Neo4j(sc)
    val rdd = neo.cypher("MATCH (n:Person) RETURN id(n) as id ").loadRowRdd
    println(rdd.count())
    // inferred schema
    println(rdd.first.schema.fieldNames)
    println(rdd.first().schema("id"))

    //求平均值
    val mean =  neo.cypher("MATCH (n:Person) RETURN id(n)").loadRdd[Long].mean
    println(mean)

    val limitMean = neo.cypher("match (n:Person) where n.id < {maxId} return n.id").param("maxId", 10).loadRowRdd
    println(limitMean.count())

    // provide partitions and batch-size
    val batchOperation = neo.cypher("MATCH (n:Person) RETURN id(n) SKIP {_skip} LIMIT {_limit}").partitions(4).batch(25).loadRowRdd
    println(batchOperation.count())

    //load via pattern
    val load = neo.pattern("Person",Seq("KNOWS"),"Person").rows(80).batch(21).loadNodeRdds
    load.foreach(x => println(x))
    println(load.count())

    //load ralationships by pattern
    val loadRel = neo.pattern("Person",Seq("KNOWS"),"Person").partitions(12).batch(100).loadRelRdd
    loadRel.foreach(x => println(x))
    println(loadRel.count())

  }

  def loadDataFrames(sc:SparkContext): Unit = {
    val neo = Neo4j(sc)

    val dataFrame = neo.cypher("MATCH (n:Person) RETURN id(n) as id SKIP {_skip} LIMIT {_limit}").partitions(4).batch(25).loadDataFrame

    println(dataFrame.count())

    val df = neo.pattern("Person",Seq("KNOWS"),"Person").partitions(12).batch(100).loadDataFrame

    df.foreach(x => println(x))


    val graphFrame = neo.pattern(("Person","id"),("KNOWS",null), ("Person","id")).partitions(3).rows(1000).loadGraphFrame

    graphFrame.vertices.count
    //     => 100
    graphFrame.edges.count
    //     => 1000

    val pageRankFrame = graphFrame.pageRank.maxIter(5).run()
    val ranked = pageRankFrame.vertices
    ranked.printSchema()

    val top3 = ranked.orderBy(ranked.col("pagerank").desc).take(3)

  }

  def loadGraphX(sc:SparkContext): Unit = {
    val neo = Neo4j(sc)

    //load graph via cypher query
    val graphQuery = "MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN id(n) as source, id(m) as target, type(r) as value SKIP {_skip} LIMIT {_limit}"

    val graph: Graph[Long, String] = neo.rels(graphQuery).partitions(7).batch(200).loadGraph

    println(graph.vertices.count())
    println(graph.edges.count())

    //load graph via pattern
    val graph2 = neo.pattern(("Person","id"),("KNOWS", "since"), ("Person", "id")).partitions(7).batch(200).loadGraph[Long, Long]

    val graph3 = PageRank.run(graph2, 5)

    println(graph3.vertices.sortBy(_._2).take(3))
  }


}
