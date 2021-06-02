import org.apache.spark.{SparkContext, SparkConf}

object DailyRevenue {
  def main(args: Array[String]) = {

    val conf = new SparkConf().setMaster(args(3)).setAppName("Daily Revenue")
    val sc = new SparkContext(conf)

    // Read orders and orderItems
    val ordersPath = args(0)
    val orders = sc.textFile(ordersPath + "orders/part-00000")
    val orderItems = sc.textFile(ordersPath + "order_items/part-00000")

    // Fiter for completed or closed orders
    val ordersFiltered = orders.
      filter(order => order.split(",")(3) == "COMPLETE" || order.split(",")(3) == "CLOSED")

    // Convert both ordersFiltered and orderItems to K-V pairs
    val ordersMap = ordersFiltered.
      map(order => (order.split(",")(0).toInt, order.split(",")(1)))
    val orderItemsMap = orderItems.
      map(oi => (oi.split(",")(1).toInt, (oi.split(",")(2).toInt, oi.split(",")(4).toFloat)))

    // Join the 2 datasets
    val ordersJoin = ordersMap.join(orderItemsMap)
    // Structure: (order_id, (order_date, (order_item_product_id, order_item_subtotal)))

    // Get daily revenue per product id
    val ordersJoinMap = ordersJoin.
      map(rec => ((rec._2._1, rec._2._2._1), rec._2._2._2))
    //Structure: ((order_date, order_item_product_id), order_item_subtotal)

    val dailyRevenuePerProductId = ordersJoinMap.
      reduceByKey((revenue, order_item_subtotal) => revenue + order_item_subtotal)
    //Structure: ((order_date, order_item_product_id), daily_revenue_per_product_id)

    // Load products from Local file system and convert into RDD /data/retail_db/products/part-00000
    import scala.io.Source
    val productsPath = args(1)
    val productsRaw = Source.
      fromFile(productsPath).
      getLines.
      toList

    val products = sc.parallelize(productsRaw)

    // Join dailyRevenuePerProductId with products to get daily revenue per product (by name)
    val productsMap = products.
      map(product => (product.split(",")(0).toInt, product.split(",")(2)))

    val dailyRevenuePerProductIdMap = dailyRevenuePerProductId.
      map(rec => (rec._1._2, (rec._1._1, rec._2)))
    //Structure From: ((order_date, order_item_product_id), daily_revenue_per_product_id)
    //Structure To: (order_product_id, (order_date, daily_revenue_per_product_id))

    val dailyRevenuePerProductJoin = dailyRevenuePerProductIdMap.join(productsMap)
    //Structure: (order_product_id, ((order_date, daily_revenue_per_product_id), product_name))

    // Sort the data by date in ascending order and by daily revenue per product in descending order
    val dailyRevenuePerProductSorted = dailyRevenuePerProductJoin.
      map(rec => ((rec._2._1._1, -rec._2._1._2), (rec._2._1._1, rec._2._1._2, rec._2._2))).
      sortByKey()
    //Structure: ((order_date_asc, daily_revenue_per_product_id_desc), (order_date, daily_revenue_per_product_id, product_name))

    // Get data to the desired format - order_date, daily_revenue_per_product, product_name
    val dailyRevenuePerProduct = dailyRevenuePerProductSorted.
      map(rec => rec._2._1 + "," + rec._2._2 + "," + rec._2._3)

    // Save final output into HDFS in avro file format as well as text file format
    // HDFS location - avro format /user/YOUR_USER_ID/daily_revenue_avro_scala 
    // HDFS location - text format /user/YOUR_USER_ID/daily_revenue_txt_scala 
    val outputPath = args(2)
    dailyRevenuePerProduct.saveAsTextFile(outputPath)
  }
}


