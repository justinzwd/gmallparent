package com.atguigu.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.common.constant.GmallConstant
import com.atguigu.gmall.common.util.MyEsUtil
import com.atguigu.gmall.realtime.StartUpLog
import com.atguigu.gmall.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis


object DauApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP,ssc)

//    inputDStream.foreachRDD{rdd => println(rdd.map(_.value()).collect().mkString("\n"))}
    // 转换处理
    val startupLogStream: DStream[StartUpLog] = inputDStream.map {
      record =>
        val jsonStr: String = record.value()
        val startupLog: StartUpLog = JSON.parseObject(jsonStr, classOf[StartUpLog])
        val date = new Date(startupLog.ts)
        val dateStr: String = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(date)
        val dateArr: Array[String] = dateStr.split(" ")
        startupLog.logDate = dateArr(0)
        startupLog.logHour = dateArr(1).split(":")(0)
        startupLog.logHourMinute = dateArr(1)

        startupLog
    }

    // 利用Redis去重
    val filteredDStream: DStream[StartUpLog] = startupLogStream.transform { rdd =>
      println("过滤前：" + rdd.count())
      // driver 周期性执行
      val curdate: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
      val jedis: Jedis = RedisUtil.getJedisClient
      val key = "dau:" + curdate
      val dauSet: util.Set[String] = jedis.smembers(key)
      val dauBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauSet)

      val filteredRDD: RDD[StartUpLog] = rdd.filter { startuplog => // executor
        val dauSet: util.Set[String] = dauBC.value
        !dauSet.contains(startuplog.mid)
      }
      println("过滤后：" + filteredRDD.count())
      filteredRDD
    }

    //去重思路：相同mid的数据分成以组，每组取一个
    val groupbyMidDStream: DStream[(String, Iterable[StartUpLog])] = filteredDStream.map(startuplog =>
      (startuplog.mid, startuplog)).groupByKey()
    val distinctDStream: DStream[StartUpLog] = groupbyMidDStream.flatMap { case (mid, startuplogItr) =>
      startuplogItr.take(1)
    }

    // 保存到Redis中
    startupLogStream.foreachRDD{ rdd=>
      rdd.foreachPartition{startuplogItr=>
        val jedis: Jedis = RedisUtil.getJedisClient
        val list: List[StartUpLog] = startuplogItr.toList
        for (startuplog<- startuplogItr) {
          val key = "dau:" + startuplog.logDate
          val value = startuplog.mid
          jedis.sadd(key,value)
          println(startuplog)
        }

        MyEsUtil.indexBulk(GmallConstant.ES_INDEX_DAU,list)
        jedis.close()
      }

    }

    ssc.start()
    ssc.awaitTermination()
  }

}
