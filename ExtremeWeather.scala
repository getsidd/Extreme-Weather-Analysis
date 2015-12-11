import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

object ExtremeWeather {

	def main(args: Array[String]) {

		val start: Long = System.currentTimeMillis/1000
		val sc = new SparkContext(new SparkConf().setAppName("Extreme Weather"))
		
		// Load the climate data and filter the headers by taking the required columns 
		val Initial_Data = sc.textFile("/gpfs/courses/cse603/students/suryasid/Actual_Data/").map(_.split(",")).filter(line => !(line.contains("EXTREME"))).map(line => Array(line(0),line(1),line(2),line(3),line(4),line(5),line(6),line(8)))
		
		// Taking the values of each feature prcp, snwd, snow, tmax, tmin according to the key stationName+yearMonth and finding the average, replace the values with missing values.		

		val Prcp = Initial_Data.map(x => (x(6)+x(7).slice(0,6),x(1).toDouble))
		val Inter_Prcp = Prcp.map(x => { 
		      var t = (x._1,x._2)
		      if(x._2 == -9999.0) t = (x._1,0.0)
		      t
		      })
		val  Prcp_Map = Inter_Prcp.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).map{ case (key, value) => (key, value._1/value._2.toFloat)}.collectAsMap()

		val Snwd = Initial_Data.map(x => (x(6)+x(7).slice(0,6),x(2).toDouble))
		val Inter_Snwd = Snwd.map(x => {
		      var t = (x._1,x._2)
		      if(x._2 == -9999.0) t = (x._1,0.0)
		      t
		      })
		val Snwd_Map = Inter_Snwd.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).map{ case (key, value) => (key, value._1/value._2.toFloat)}.collectAsMap()

		val Snow = Initial_Data.map(x => (x(6)+x(7).slice(0,6),x(3).toDouble))
		val Inter_Snow = Snow.map(x => {
		      var t = (x._1,x._2)
		      if(x._2 == -9999.0) t = (x._1,0.0)
		      t
		      })
		val Snow_Map = Inter_Snow.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).map{ case (key, value) => (key, value._1/value._2.toFloat)}.collectAsMap()

		val Tmax = Initial_Data.map(x => (x(6)+x(7).slice(0,6),x(4).toDouble))
		val Inter_Tmax = Tmax.map(x => {
		      var t = (x._1,x._2)
		      if(x._2 == -9999.0) t = (x._1,0.0)
		      t
		      })
		val Tmax_Map = Inter_Tmax.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).map{ case (key, value) => (key, value._1/value._2.toFloat)}.collectAsMap()

		val Tmin = Initial_Data.map(x => (x(6)+x(7).slice(0,6),x(5).toDouble))
		val Inter_Tmin = Tmin.map(x => {
		      var t = (x._1,x._2)
		      if(x._2 == -9999.0) t = (x._1,0.0)
		      t
		      })
		val Tmin_Map = Inter_Tmin.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).map{ case (key, value) => (key, value._1/value._2.toFloat)}.collectAsMap()

                // Replacing the average values with the missing values
		val data_parsed = Initial_Data.map(x => {
		      var key = x(6)+x(7).slice(0,6)
		      var a = Array(x(0),x(1),x(2),x(3),x(4),x(5))
		      if(x(1) == "-9999") a(1) = ""+Prcp_Map(key)
		      if(x(2) == "-9999") a(2) = ""+Snwd_Map(key)
		      if(x(3) == "-9999") a(3) = ""+Snow_Map(key)
		      if(x(4) == "-9999") a(4) = ""+Tmax_Map(key)
		      if(x(5) == "-9999") a(5) = ""+Tmin_Map(key)
		      a
		      })
		
		// Creating the labeled points 
		val parsedData = data_parsed.map { x =>
		      LabeledPoint(x(0).toDouble, Vectors.dense(x(1).toDouble,x(2).toDouble,x(3).toDouble,x(4).toDouble,x(5).toDouble))
		      }.cache()
                
		val numIterations = 100

		// Creating the Model using SVM
		val model = SVMWithSGD.train(parsedData, numIterations)

		// Finding the Labels based on the model
		val scoreAndLabels = parsedData.map { point =>
			val score = model.predict(point.features)
			(point.label,score)
		      }
		val Err = scoreAndLabels.filter(x => x._1 != x._2).count()/(data_parsed.count()).toDouble
		println("Error =" + Err)
		val end: Long = System.currentTimeMillis/1000
		println("Time: "+ (end-start))

	}
}
