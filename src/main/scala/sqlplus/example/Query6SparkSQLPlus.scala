package sqlplus.example

import sqlplus.helper.ImplicitConversions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object Query6SparkSQLPlus {
	val logger = LoggerFactory.getLogger("SparkSQLPlusExperiment")

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
		conf.setAppName("Query6SparkSQLPlus")
		val sc = new SparkContext(conf)
		sc.defaultParallelism
		val sparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()

		val intLessThan = (x: Int, y: Int) => x < y

		val v1 = sc.textFile(s"${args.head}/graph.dat").map(line => {
			val fields = line.split(",")
			Array[Any](fields(0).toInt, fields(1).toInt)
		})

		val v2 = v1.map(fields => ((fields(0)), 1)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2))
		val v3 = v1.map(fields => ((fields(0)), 1)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2))

		val v4 = v1.keyBy(x => (x(1).asInstanceOf[Int]))
		val v5 = v2.keyBy(x => (x(0).asInstanceOf[Int]))
		val v6 = v4.semiJoin(v5)
		val v7 = v6.reKeyBy(x => (x(0).asInstanceOf[Int]))
		val v8 = v7.groupBy()
		val v9 = v8.sortValuesWith(1, (x: Int, y: Int) => intLessThan(y, x))
		val v10 = v9.extractFieldInHeadElement(1)
		val v11 = v4.appendExtraColumn(v10)
		val v12 = v11.reKeyBy(x => (x(0).asInstanceOf[Int]))
		val v13 = v12.groupBy()
		val v14 = v13.sortValuesWith(2, (x: Int, y: Int) => intLessThan(y, x))
		val v15 = v14.extractFieldInHeadElement(2)
		val v16 = v4.appendExtraColumn(v15)
		val v17 = v16.reKeyBy(x => (x(0).asInstanceOf[Int]))
		val v18 = v17.groupBy()
		val v19 = v18.sortValuesWith(2, (x: Int, y: Int) => intLessThan(y, x))
		val v20 = v19.extractFieldInHeadElement(2)
		val v21 = v3.keyBy(x => (x(0).asInstanceOf[Int]))
		val v22 = v21.appendExtraColumn(v20)
		val v23 = v22.filter(x => intLessThan(x._2(1).asInstanceOf[Int], x._2(2).asInstanceOf[Int]))

		val v24 = v23.enumerate(v19, 1, 2, (x: Int, y: Int) => intLessThan(x, y), Array(0,1), Array(1,2))
		val v25 = v24.reKeyBy(x => (x(2).asInstanceOf[Int]))
		val v26 = v25.enumerate(v14, 1, 2, (x: Int, y: Int) => intLessThan(x, y), Array(0,1,2), Array(1,2))
		val v27 = v26.reKeyBy(x => (x(3).asInstanceOf[Int]))
		val v28 = v27.enumerate(v9, 1, 1, (x: Int, y: Int) => intLessThan(x, y), Array(0,1,2,3), Array(1))
		val v29 = v28.reKeyBy(x => (x(4).asInstanceOf[Int]))
		val v30 = v5.groupBy()
		val v31 = v29.enumerate(v30, Array(0,1,2,3,4), Array(1))
		val v32 = v31.map(t => Array(t._2(1), t._2(0), t._2(2), t._2(3), t._2(4), t._2(5)))

		val ts1 = System.currentTimeMillis()
		v32.count()
		val ts2 = System.currentTimeMillis()
		logger.info("Query6-SparkSQLPlus time: " + (ts2 - ts1) / 1000f)

		sparkSession.close()
	}
}
