package ora.apache
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.hive.HiveContext
import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable
import scala.collection.mutable._
import scala.collection.immutable._

/**
  * Created by Liao Wenzhe on 2018/7/17 0017.
  */
  
object test {
  val hc = SparkSession.builder().config("spark.sql.shuffle.partitions",10).enableHiveSupport().appName("zcjl").getOrCreate()
  val cpuNum=12
  val cell_tac_2 = ArrayBuffer[ArrayBuffer[Double]]()
  val points = ArrayBuffer[ArrayBuffer[ArrayBuffer[Vector[Double]]]]()
  for (i <- 1 to cpuNum) cell_tac_2 += ArrayBuffer[Double]()
  for (i <- 1 to cpuNum) points += ArrayBuffer[ArrayBuffer[Vector[Double]]]()
  val system = ActorSystem("dbscan")
 
  def main(args: Array[String]): Unit = {
    //DBSCAN的参数设定
    val minPts = 10 //密度阈值
    val ePs = 0.05 //领域半径
    var cell_tac = new ArrayBuffer[Double]()
    var tac = hc.sql(
      s"""with tab_1 as (SELECT cell_tac ,count(*) count FROM  njyxnlc_hive_db.zcjl group by  cell_tac)
         |select cell_tac from  tab_1 where count <=4000 """.stripMargin)
    hc.sql(s"""SELECT *  from njyxnlc_hive_db.zcjl """.stripMargin).cache().createTempView("zcjl")
    tac.collect.foreach(row => {
        cell_tac.append(row.getAs("cell_tac").toString.toDouble)
      })
    var rowIndex = 0
    for (i <- 0 until cell_tac.length) {
      cell_tac_2(rowIndex) append cell_tac(i)
      rowIndex = (rowIndex + 1) % cpuNum
    }
    for (item <- 0 until cell_tac_2.length) {
      for (i <- 0 until cell_tac_2(item).length) {
        points(item) += ArrayBuffer[Vector[Double]]()
        var cell_tac_3 = cell_tac_2(item)(i).toDouble
        val data = hc.sql(
        s"""SELECT longitude,latitude,cell_tac FROM zcjl WHERE cell_tac = ${cell_tac_3} """.stripMargin)
        data.collect.foreach(row => {
            var vector = Vector[Double]()
            vector ++= Vector(row.getAs("longitude").toString.toDouble)
            vector ++= Vector(row.getAs("latitude").toString.toDouble)
            vector ++= Vector(row.getAs("cell_tac").toString.toDouble)
            points(item)(i).append(vector)
        })
      }
    }
    var points1 = points.toArray
   
    class dbscan_Actor(points1:Array[Vector[Double]],key:Double) extends Actor {
        def receive = {
          case "dbscan" => runDBSCAN(points1, ePs, minPts, key)
        }
   }
    for (item <- 0 until points1.length) {
       for (i <- 0 until points1(item).length) {
          val dbscan = system.actorOf(Props(new dbscan_Actor(points1(item)(i).toArray,points1(item)(i)(0)(2))))
          println("begin")
          dbscan  ! "dbscan"
       }
     }
    println("end")
    Thread.sleep(10000)
    system.shutdown()
   }
  
  // DBSCAN算法实现
  def runDBSCAN(data:Array[Vector[Double]],ePs:Double,minPts:Int, key:Double):Unit ={
    println("runDBSCAN:" + key)
    val types = (for(i <- 0 to data.length - 1) yield -1).toArray // 用来划分各点属于哪个种类型的点（核心点标记为1，边界点标记为0，噪音点标记为-1(即cluster中值为0的点)
    val visited = (for(i <- 0 to data.length - 1) yield 0).toArray // 用来判断点有没有处理过，处理过标记为1，没有处理过的标记为0
    var number = 1 // 用于标记划分的类
    var xTempPoint = Vector(0.0,0.0)
    var yTempPoint = Vector(0.0,0.0)
    var distance = new Array[(Double,Int)](1)
    var distanceTemp = new Array[(Double,Int)](1)
    val neighPoints = new ArrayBuffer[Vector[Double]]()
    var neighPointsTemp = new Array[Vector[Double]](1)
    val clusters = new Array[Int](data.length) // 用于标记每个数据点所属的类别
    var index = 0
    for(i <- 0 to data.length - 1){// 对每一个点的数据进行处理以及对其分类
      println("test_1:" + key)
      neighPoints.clear()
      if(visited(i) == 0){ // 表示该点未被处理
        visited(i) == 1 // 标记为处理过
        xTempPoint = data(i) // 取到该点的数据
        distance = data.map(x => (vectorDis(x,xTempPoint),data.indexOf(x)))// 取得该点到其他所有点的距离Array{(distance,index)}
        neighPoints ++= distance.filter(x => x._1 <= ePs).map(v => data(v._2)) // 找到半径ePs内的所有点(密度相连点集合)
        println("test_2:" + key)
        if(neighPoints.length > 1 && neighPoints.length < minPts){
          breakable{
            for(item <- 0 to neighPoints.length -1 ){// 此为非核心点，若其领域内有核心点，则该点为边界点
            var index = data.indexOf(neighPoints(item))
              if(types(index) == 1){
                types(i) = 0// 标记为边界点
                break
              }
            }
          }
        }
        if(neighPoints.length >= minPts){// 核心点,此时neighPoints表示以该核心点出发的密度相连点的集合
          types(i) = 1
          clusters(i) = number
          while(neighPoints.isEmpty == false){ // 对该核心点领域内的点迭代寻找核心点，直到所有核心点领域半径内的点组成的集合不再扩大（每次聚类 ）
            yTempPoint =neighPoints.head // 取集合中第一个点
            index = data.indexOf(yTempPoint)
            if(visited(index) == 1){
              if(types(index) == -1){
                clusters(index) = number
                types(index) = 0
              }
            }
            if(visited(index) == 0){// 若该点未被处理，则标记已处理
              visited(index) = 1
              if(clusters(index)==0) clusters(index) = number
              distanceTemp = data.map(x => (vectorDis(x,yTempPoint),data.indexOf(x)))  // 取得该点到其他所有点的距离Array{(distance,index)}
              neighPointsTemp = distanceTemp.filter(x => x._1 <= ePs).map(v => data(v._2)) // 找到半径ePs内的所有点
              if(neighPointsTemp.length >= minPts) {
                types(index) = 1 // 该点为核心点
                for (i <- 0 to neighPointsTemp.length - 1) {
                  // 将其领域内未分类的对象划分到簇中,然后放入neighPoints
                  if (clusters(data.indexOf(neighPointsTemp(i))) == 0) {
                    clusters(data.indexOf(neighPointsTemp(i))) = number // 只划分簇，没有访问到
                    neighPoints += neighPointsTemp(i)
                  }
                }
              }
              if(neighPointsTemp.length > 1 && neighPointsTemp.length < minPts){
                breakable{
                  for(i <- 0 to neighPointsTemp.length -1 ){// 此为非核心点，若其领域内有核心点，则该点为边界点
                  var index1 = data.indexOf(neighPointsTemp(i))
                    if(types(index1) == 1){
                      types(index) = 0// 边界点
                      break
                    }
                  }
                }
              }
            }
            neighPoints -= yTempPoint // 将该点剔除
          }
          number += 1 // 进行新的聚类
        }
      }
    }
    printResult(data, clusters, types,key)
  }
  
  // 结果保存,最好是把结果保存到HIVE中，最好就是把源始数据打上类别标志
  def printResult(data:Array[Vector[Double]],clusters:Array[Int],types:Array[Int],item:Double) = {
    println("printResult:" + item)
    val result = data.map(v => (clusters(data.indexOf(v)),v)).groupBy(v => v._1) //Map[int,Array[(int,Vector[Double])]]
    // key代表簇号，value代表属于这一簇的元素数组,转化为Clusters对象。
    var coenblist: ListBuffer[Clusters] = new ListBuffer[Clusters]()
    result.foreach(v =>{
      val data = v._2.map(v => Clusters(v._1,v._2(0),v._2(1),v._2(2)))
      data.foreach(line =>{
        val clusters = line.clusters
        val lng = line.lng
        val lat = line.lat
        val cell_tac = line.cell_tac
        coenblist.append(new Clusters(clusters,lng,lat,cell_tac))
      })
    })
    var clusters_tac="clusters" + item.toInt.toString
    var types_tac="types" + item.toInt.toString
    val rdd:RDD[Clusters] = hc.sparkContext.parallelize[Clusters](coenblist)
    hc.createDataFrame(rdd.map(p=>Row(p.clusters,p.lng,p.lat,p.cell_tac)), CoenblistSchema).createTempView(clusters_tac)
    val pointsTypes = data.map(v => (types(data.indexOf(v)),v)).groupBy(v => v._1) //Map[点类型int,Array[(点类型int,Vector[Double])]]
    var coenbtypes: ListBuffer[Types] = new ListBuffer[Types]()
    pointsTypes.foreach(v =>{
      val types = v._2.map(v => Types(v._1,v._2(0),v._2(1),v._2(2)))
      types.foreach(line =>{
        val types = line.types
        val lng = line.lng
        val lat = line.lat
        val cell_tac = line.cell_tac
        coenbtypes.append(new Types(types,lng,lat,cell_tac))
      })
    })
    val rdd_1:RDD[Types] = hc.sparkContext.parallelize[Types](coenbtypes)
    hc.createDataFrame(rdd_1.map(p=>Row(p.types,p.lng,p.lat,p.cell_tac)), CoenbtypesSchema).createTempView(types_tac)
    hc.sql(
      s"""
         |select
         |          a.clusters
         |          ,a.lng
         |          ,a.lat
         |          ,b.types
         |          ,b.cell_tac
         |          from ${clusters_tac} a left join ${types_tac} b on  a.lng=b.lng and a.lat=b.lat
      """.stripMargin).coalesce(1).write.option("header",true).csv(s"/jc_njyxnlc/lwz/${clusters_tac}.csv")
  }
  
  // --------------------------两个经纬度之间的距离-----------------------------
  def vectorDis(v1: Vector[Double], v2: Vector[Double]):Double = {
    var distance = 0.0
    distance = getDistance(v1(1),v1(0),v2(1),v2(0))
    distance
  }
  
  def getDistance(lat1:Double,lng1:Double,lat2:Double,lng2:Double):Double={
    val pi:Double = 3.141592625
    val earthPadius = 6378.137
    val radlat1= lat1 * pi / 180.0
    val radlat2 = lat2 * pi / 180.0
    val a = radlat1 - radlat2
    val b = lng1 * pi / 180.0 - lng2 * pi / 180.0
    var s:Double = 0.toDouble
    s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2) + Math.cos(radlat1) * Math.cos(radlat2) * Math.pow(Math.sin(b / 2), 2)))
    s = s * earthPadius
    s.formatted("%.4f")
  }
  
  def CoenblistSchema: StructType = {
    StructType(
      List(
        StructField("clusters", IntegerType),
        StructField("lng", DoubleType),
        StructField("lat", DoubleType),
        StructField("cell_tac", DoubleType)
      )
    )
  }
  
  def CoenbtypesSchema: StructType = {
    StructType(
      List(
        StructField("types", IntegerType),
        StructField("lng", DoubleType),
        StructField("lat", DoubleType) ,
        StructField("cell_tac", DoubleType)
      )
    )
  }
  
  case class Clusters(clusters:Int,lng:Double,lat:Double,cell_tac:Double)
  case class Types(types:Int,lng:Double,lat:Double,cell_tac:Double)
}

