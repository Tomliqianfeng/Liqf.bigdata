package mongoDB



import com.alibaba.fastjson.{JSON, JSONObject}
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.rdd.MongoRDD
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.bson.Document

import java.text.SimpleDateFormat
import java.util.Date



case class toHive_data(comments: String, datetime: String, userid: String,username: String,score: Int,dscore: Int,qscore: Int,lscore: Int,sscore: Int,salename: String,productName: String,orderTime: String,mobile: String,msgcategorycode: String,categoryName: String,product_id: String,categoryCode: String,ispic: String,picturecount: Int,isvalid: String,udate: String,commentID: String,order_number: String,createdate: String,isProcess: String,isAuto: String,channelId: String,tags: String,isReport: String,headLine: String,collection_udate: String, usercollection_num: Int,badReason: String)

object mongodbToHive {

    def main(args: Array[String]): Unit = {
      //实例化config对象
      val conf: SparkConf = new SparkConf()
        .setAppName("MongoToHiveTask")
        .set("spark.mongodb.input.uri", "mongodb://read_comment:read_comment0108@plmongodb1.iblidc.com:31037/bl_comment.plComment?readPreference=primaryPreferred")
        .set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.1.0")
        .set("spark.mongodb.input.partitioner", "MongoPaginateByCountPartitioner")
        .set("spark.mongodb.input.partitionerOptions.numberOfPartitions", "64")
        .set("spark.mongodb.input.partitionerOptions.partitionKey", "_id")

      //实例化session对象
      val ss: SparkSession = SparkSession
        .builder()
        .config(conf)
        .enableHiveSupport()
        .getOrCreate()

      //设置mongo配置并映射临时表
      val readConfig = ReadConfig(Map("database" -> "bl_comment", "collection" -> "plComment"), Some(ReadConfig(ss)))
      val mongoRDD: MongoRDD[Document] = MongoSpark.load(ss.sparkContext, readConfig)
      import ss.implicits._

      //mongo json 数据解析
      mongoRDD.mapPartitions(partition => {
        partition.map(row => {
          val json: JSONObject = JSON.parseObject(row.toJson())

          val comments: String = json.getString("comments")
          val datetime: String = json.getString("datetime")
          val userid:   String = json.getString("userid")
          val username: String = json.getString("username")

          var score:  Int = 0
          if (json.getInteger("score") != null) {
            score = json.getInteger("score")
          }
          var dscore: Int = 0
          if (json.getInteger("dscore") != null) {
            dscore = json.getInteger("dscore")
          }
          var qscore: Int = 0
          if (json.getInteger("qscore") != null) {
            qscore = json.getInteger("qscore")
          }
          var lscore: Int = 0
          if (json.getInteger("lscore") != null) {
            lscore = json.getInteger("lscore")
          }
          var sscore: Int = 0
          if (json.getInteger("sscore") != null) {
            sscore = json.getInteger("sscore")
          }

          val productName:  String = json.getString("productName")
          val orderTime:    String = json.getString("orderTime")
          val mobile:       String = json.getString("mobile")
          val categoryName: String = json.getString("categoryName")
          val product_id:   String = json.getString("product_id")
          val categoryCode: String = json.getString("categoryCode")
          val ispic:        String = json.getString("ispic")
          var picturecount: Int = 0
          if (json.getJSONArray("pictures") != null) {
            picturecount = json.getJSONArray("pictures").size()
          }

          val isvalid:      String = json.getString("isvalid")
          val udate:        String = json.getString("udate")
          val commentID:    String = json.getJSONObject("_id").getString("$oid")
          val order_number: String = json.getString("order_number")
          val createdate:   String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date().getTime)
          val isProcess:    String = json.getString("isProcess")
          val isAuto:       String = json.getString("isAuto")
          val channelId:    String = json.getString("channelId")
          var tags:         String = null
          if(json.getJSONArray("tags") != null){
            tags = json.getJSONArray("tags").getString(0)
            if(json.getJSONArray("tags").size() > 1){
              for(i <- 1 until json.getJSONArray("tags").size()){
                tags = tags+","+json.getJSONArray("tags").getString(i)
              }
            }
          }

          val isReport:         String = json.getString("isReport")
          val headLine:         String = json.getString("headLine")
          var collection_udate: String = null
          if (json.getJSONObject("collection") != null) {
            collection_udate = json.getJSONObject("collection").getString("udate")
          }

          var usercollection_num : Int = 0
          if (json.getJSONObject("collection") != null ) {
            if (json.getJSONObject("collection").getJSONArray("userCollection") != null) {
              usercollection_num = json.getJSONObject("collection").getJSONArray("userCollection").size()
            }
          }
          val badReason: String = json.getString("badReason")

          toHive_data(comments,datetime,userid,username,score,dscore,qscore,lscore,sscore,null,productName,orderTime,mobile,null,categoryName,product_id,categoryCode,ispic,picturecount,isvalid,udate,commentID,order_number,createdate,isProcess,isAuto,channelId,tags,isReport,headLine,collection_udate, usercollection_num,badReason)

        })
      }).toDS().createTempView("tmp")

      val sql = {
        """
          |select comments,datetime,userid,username,score,dscore,qscore,lscore,sscore,salename,productName,orderTime,mobile,msgcategorycode,categoryName,product_id,categoryCode,ispic,picturecount,isvalid,udate,commentID,order_number,createdate,isProcess,isAuto,channelId,tags,isReport,headLine,collection_udate, case when usercollection_num=0 then null else usercollection_num end as usercollection_num,badReason,'nodate' as dt
          |from tmp
          |""".stripMargin}

      //执行sql并将结果覆盖写到hive表
      ss.sql(sql)
        .repartition(20)
        .write
        .mode(SaveMode.Overwrite)
        .insertInto("sourcedata.s13_bl_comment_full")



      //释放资源
      ss.stop()
    }

}
