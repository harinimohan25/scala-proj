package com.customergraph.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.commons.Logging.Log
import org.apache.commons.logging.LogFactory

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, Filesystem, Path}

import scala.collection.mutable.{HashMap,ListBuffer}

object CustomerGraphApp extends App {
    case class Interest(mk_nm: String, sg_nm: String, amt: String, fi_score: Int)

    val LOG = LogFactory.getLog(this.getClass.getName)
    val conf = new SparkConf
    conf.setAppName("CustomerGraph_SparkApp")
        .set("Spark.kryoserializer.buffer.max","124m")
     val sc = new SparkContext(conf)
     LOG.info("SparkContext env: " + sc.getConfg.getAll.toString)
     val input_file = args(0)
     val output_tmp = args(1)
     val output_file = args(2)
     val metadata_file = args(3)
     val ddl_file = args(4)
     val configuration_file = args(5)

     val inputRDD = sc.textFile(input_file).map(_.split("\t")
     val cgs = inputRDD.map(x => (x(0), new Interest(x(2), x(3), x(6), x(7).toInt).groupByKey
     val mkt_seg_namesRdd = inputRDD.map( x => (x(2)).distinct
     val seg_nm_bc = sc.broadcast(mkt_seg_namesRdd.collect.sorted)

     val mkt_id_namesRdd = inputRDD.map( x => (x(1)+"|"+x(2))).distinct
     mkt_id_namesRdd.coalesce(1,true).saveAsTextFile(metadata_file)

     val ddl = genDDL(args(4), args(5), seg_nm_bc.value)
     val pivot = cgs.mapPartitionsWithIndex(aggregate(args(1), args(5), seg_nm_bc.value) )

     LOG.info("pivot parititons computed: "+ pivot.collect.length)
     getMergeInHdfs(output_tmp, output_file, sc.hadoopConfiguration)

     def aggregate(output_tmp_dir: String, config_dir: String, seg_names: Array[(String)](index: Int, ite: Iterator[(String, Iterable[Interest])]): Iterator[String] = {
        val mkt_seg_names = seg_nm_bc.value
        val results = new ListBuffer[String]

        while(ite.hasNext) {
        val record = ite.next
        val cust_id = records._1
        val interests = records._2

        val sb = new StringBuilder()

        sb.append(cust_id)
        mkt_seg_names.map{x =>
                val mkt_nm = x
                val idx = interests.find(y => y.mk_nm == mkt_nm)
                val result = idx_match {
                            case some(_) => val int = idx.get; sb.append(","+int.sg_nm+","+int.amt+","+int.fi_score)
                            case None => sb.append(",,,")
                            }
                         }
        results += sb.toString
     }
     val records = results.iterator
     val config = new Configuration

     config.addResource(config_dir)

     writePartition(config, output_tmp_dir, records, index)
     List(results.size.toString).iterator
   }

   def getMergeInHdfs(src: String, dst: String, config: Configuration) = {
    val fs = FileSystem.get(config)
    FileUtil.copyMerge(fs, new Path(src), fs, new Path(dst), false, config, null)
   }

   def writePartition(config: Configuration, dst: String, records: Iterator[String], index: Int) = {
    val fs = FileSystem.get(config)
    val os = fs.create(new Path(dst+"/customer_graph.part_" + index))
    while (records.hasNext) {
        val records = records.next + "\n"
        os.write(records.getBytes)
   }
   os.flush
   os.close
  }

  def genDDL(output_tmp_dir: String, config_dir: String, cols: Array[(String)]) = {
    val lb = new ListBuffer[String]
    val tbl_ddl = new StringBuilder()
    tbl_ddl.append("DROP EXTERNAL TABLE IF EXISTS @src_schema.@src_tbl_nm; \n\n")
    tbl_ddl.append("CREATE EXTERNAL TABLE @src_schema.@src_tbl_nm (\n cust_dim_nb \t\t\t\t NUMERIC(18,0), \n")
    val last:Int = cols.length
    var count :Int = 0
    var colnm :String = ""
    for (colnm <- cols )
    {
        tbl_ddl.append("seg_nm" + colnm + "     CHARACTER VARYING(50), \n")
        tbl_ddl.append("spnd_am_" + colnm + "       INTEGER, \n")
        count = count + 1
        if (count == last) {
        tbl_ddl.append("fnlz_sc_" + col_nm + "      INTEGER \n)")
        tbl_ddl.append(" LOCATION \n(\n")
        tbl_ddl.append("  '@cg_src_file' \n)\n")
        tbl_ddl.append("  FORMAT 'text' (delimiter ',' null '' escape 'OFF') ENCODING 'UTF8'; ")
        }
        else {
            tbl_ddl.append("fnlz_sc_" + colnm + "   INTEGER, \n")
        }
    }
    lb += tbl_ddl.toString
    val cgDDL = lb.iterator
    val config = new Configuration
    config.addResources(config_dir)
    writeFile(config, output_tmp_dir, cgDDL)
   }
}

