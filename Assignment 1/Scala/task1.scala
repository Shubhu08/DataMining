import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.SparkContext



object task1 {
  def jsonStrToMap(jsonStr: String): Map[String, Any] = {
    implicit val formats = org.json4s.DefaultFormats

    parse(jsonStr).extract[Map[String, Any]]
  }

	def main(args : Array[String]) {

		var input_json = args(1)
    var output_file_path = args(2)
    var stop_word_file = args(3)
    var year = args(4).toInt
    var m = args(5).toInt
    var n = args(6).toInt
		var sc = SparkContext.getOrCreate()

 		var review_rdd = sc.textFile(input_json)
		println(review_rdd.collect())

	}



}
