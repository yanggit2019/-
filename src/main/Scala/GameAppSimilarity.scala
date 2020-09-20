import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import java.{lang => jl}

import com.esotericsoftware.kryo.Kryo
import org.apache.hadoop.io.Text
import org.apache.spark.serializer.KryoRegistrator

import scala.collection.mutable.ListBuffer

case class UserPkg(var uptime: Long = 0, var pkgName: String = "") extends KryoRegistrator{
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(Class.forName(classOf[UserPkg].getName))
    kryo.register(Class.forName(classOf[Text].getName))
    kryo.register(classOf[Array[Long]])
    kryo.register(classOf[Array[String]])
    kryo.register(Class.forName("scala.collection.mutable.WrappedArray$ofInt"))
    kryo.register(Class.forName("scala.reflect.ManifestFactory$$anon$9"))
  }
}

class GameAppSimilarity

object GameAppSimilarity {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GameAppSimilarity")

    /**
      * 本地
      */
//        conf.setMaster("local[*]")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[UserPkg],classOf[GameAppSimilarity]))
    //local[20]就等于有20个executors，但是都是在driver上的
    //spark_sql的shuffle的partitions的数量
//        conf.set("spark.sql.shuffle.partitions", "1")
    //spark的executor执行内存
//        conf.set("spark.executor.memory", "10g")
//        conf.set("spark.submit.deployMode","cluster")

    val sc = new SparkContext(conf)
    //14天安装的数据源ORC 格式  "struct<pkgname1:string,pkgname2:string,values:double>";

    /**
      * 本地
      */
//        val orcPath = "H:\\orc数据"
        val orcPath = "hdfs://ns1/data/hainiu/sum_install_head"
//    val orcPath = args(0)
    val hivec = new HiveContext(sc)

    val df: DataFrame = hivec.read.orc(orcPath)
    df.createOrReplaceTempView("sum_install_daily_14d")
    //求出数据源中每个包名对应的最大安装量//求出数据源中每个包名对应的最大安装量

    /**
      * 本地
      */
//        val hql: String = "select pkgname,count(*) n from " + "(select a.aid,a.pkgname from (select _col0 as aid,_col1 as pkgname from sum_install_daily_14d where _col4 REGEXP \'^game_\') a " + "left join " + "(select c.aid from (select _col0 as aid,count(*) n from sum_install_daily_14d where _col4 REGEXP \'^game_\' group by _col0) c where c.n>50) b " + "on a.aid=b.aid where b.aid is null) d group by pkgname"
    val hql: String = "select pkgname,count(*) n from " + "(select a.aid,a.pkgname from (select aid,pkgname from sum_install_daily_14d where gp REGEXP \'^game_\') a " + "left join " + "(select c.aid from (select aid,count(*) n from sum_install_daily_14d where gp REGEXP \'^game_\' group by aid) c where c.n>50) b " + "on a.aid=b.aid where b.aid is null) d group by pkgname"
    //执行这个sql并转换成javaPairRDD<包名，包名对应的安装量>
    val sql: DataFrame = hivec.sql(hql)
    val mapPair: RDD[(String, Long)] = sql.rdd.map(r => (r.getString(0), r.getLong(1)))
    //因为是javaPairRDD所以能转成map类型，并设置成广播变量，用于公式中求两个包安装量的算术平均值使用
    val pkgCountMap: collection.Map[String, Long] = mapPair.collectAsMap()
    val broadcast: Broadcast[collection.Map[String, Long]] = sc.broadcast(pkgCountMap)
    //aid:string,pkgname:string,uptime:bigint,country:string,gp:string

    val filterRDD: RDD[(String, String)] = df.rdd.filter(r => {
      //过滤非法数据
      var aid, pkgName, uptime, gp: String = ""
      aid = r.getString(0)
      pkgName = r.getString(1)
      pkgName = r.getLong(2).toString
      gp = r.getString(4)
      if (aid == null || pkgName == null || uptime == null || gp == null || !gp.startsWith("game_")) {
        false
      }
      true
    }).map(r => {
      //将过滤之后的合法数据 spark-sql的row类型转换成自定义的UserPkg类型
      var pkgName, uptime, aid: String = ""
      aid = r.getString(0)
      pkgName = r.getString(1)
      uptime = r.getLong(2).toString
      val userPkg = new UserPkg() //程序里面new对象太多了
      userPkg.pkgName = pkgName
      userPkg.uptime = uptime.toLong
      (aid, userPkg)
    }).groupByKey().filter(f => {
      //先将pair的数据按aid  groupby之后，再过滤掉安装过大于50个应用的人，因为这种人的要么是BUG ID产生的，要么就是兴趣太广泛对于物品相似度来讲不具备什么参考价值
      val size: Int = f._2.toList.size
      if (size > 50) {
        false
      } else {
        true
      }
    }).flatMap(f => {
      /**
        * 根据每个人拥有的包也就是物品，找出物品之间的关系，并计算出每个人对每两个物品的相似度贡献值
        * 并考虑对拥有物品太多人的惩罚和对每两个物品的时间行为进行加减权(因为基于常理来讲只有人对物品产生的行为时间越相近，那这个人对这两个物品的兴趣也就越相近，比如想买一个商品会同时对比别一件相似的商品)
        * 按字典排序组成输出KEY值也就是被个人产生的两个相关的物品
        * 因为按规则定好才可以在下一步计算的时候把每个人贡献的两个相同物品的相似值进行累加
        * 如果没有规则的话,比如a和b两个物品的相似值就可能会输出a b或b a两种key值，这样的话a和b物品的每个人贡献的相似值可能就累加不到一起去了。
        */

      val tList = new ListBuffer[(String, String)]
      val list: List[UserPkg] = f._2.toList
      val size: Int = list.size
      val sd: Double = 1 / Math.log10(1 + size)
      for (i <- 0 until size - 1) {
        val userPkg: UserPkg = list(i)
        for (a <- i + 1 until size) {
          val userPkg1: UserPkg = list(a)
          val td: Double = 1.0 / (1 + Math.abs(userPkg.uptime - userPkg1.uptime))
          val key: String = if (userPkg.pkgName.compareTo(userPkg1.pkgName) < 0)
            s"${userPkg.pkgName}\t${userPkg1.pkgName}"
          else
            s"${userPkg1.pkgName}\t${userPkg.pkgName}"

          val value: String = (sd * td).toString
          tList += ((key, value))
        }
      }
      tList
    })


    val simiRDD: RDD[Row] = filterRDD.filter(f => {
      //过滤掉每两个物品有可能计算出错的相似度值，进一步保证下一步计算结果的准确性
      val pkg1Pkg2: String = f._1
      val temp: Array[String] = pkg1Pkg2.split("\t")
      val pkg1: String = temp(0)
      val pkg2: String = temp(1)
      val similarity: String = f._2
      if ("".equals(pkg1) || "".equals(pkg2) || "".equals(similarity)) {
        false
      } else {
        true
      }
    }
    ).groupByKey().flatMap(f => {
      /**
        * 把每个人贡献相关的两个物品相似度值进行累加求合，从而得到所有人对这两个物品贡献的相似度值总和
        * 然后再除以这两个物品所拥有人数的算法平均值，这一步主要是考虑把太热的物品也就是大多数人都可能拥有的物品的相似度值给平均化，避免产生所有物品都跟这个热门物品的相似度结果最大
        * 得到两个物品的最终相似度值，因为最终的结果值可能比较大，不好存储，所以用log10进行归一化（只要不影响排序用什么方法都可以）
        * 输出所有可能关系的相似度值，因为之前是按字典排序的最后的结果可能如下
        * a	b	similarity
        * b	c	similarity
        * 如果不转成
        * a	b	similarity
        * b	a	similarity
        * b	c	similarity
        * c	b	similarity
        * 那从结果中查询与b相关的物品那就需要同时查询第一列和第二列再把结果合并到一起再排序
        * 如果转换过那只要查询的时候查询第一列并按第一列进行排序就可以了
        * 基于itemCF原理这种转换也是必须的，同时也方便查询，见PPT的第一页
        */
      val list = new ListBuffer[Row]
      val pkgs: Array[String] = f._1.split("\t")
      val map: collection.Map[String, Long] = broadcast.value
      val pkg1: Long = map.getOrElse(pkgs(0), 0)
      val pkg2: Long = map.getOrElse(pkgs(1), 0)

      if (pkg1 != 0 && pkg2 != 0) {
        val iterator: List[String] = f._2.toList
        var td = 0.0
        for (i <- iterator) {
          td += i.toDouble
        }
        val similarity: Double = Math.log10(td / Math.sqrt(pkg1 * pkg2))
        val similarityStr: String = f"${similarity}%.5f"

        //此方法的参数需要java的object类型，所以不能用scala的double
        val row1: Row = RowFactory.create(pkgs(0), pkgs(1), new jl.Double(similarityStr))
        val row2: Row = RowFactory.create(pkgs(1), pkgs(0), new jl.Double(similarityStr))
        list += row1
        list += row2
      }
      list
    })

    val fields = new ListBuffer[StructField]
    //pkgname1:string,pkgname2:string,values:double
    fields += DataTypes.createStructField("pkgname1", DataTypes.StringType, true)
    fields += DataTypes.createStructField("pkgname2", DataTypes.StringType, true)
    fields += DataTypes.createStructField("values", DataTypes.DoubleType, true)
    val schema: StructType = DataTypes.createStructType(fields.toArray)
    val sqlContext = new SQLContext(sc)

    val df1: DataFrame = sqlContext.createDataFrame(simiRDD, schema)

    df1.createOrReplaceTempView("app_ins_similarity")

    //把计算结果按第一列进行分组并按相似度值进行倒序排序并计算出row_num用于数据的截断，因为相似度值不确定所以只能根据row_num值进行数据的截断
    //至于这个截断值应该取多少，这个需要通过离线试验给出并加上线上试验结果确定
    val pkgDisSql = "select distinct pkgname1 from (select pkgname1,pkgname2,values,row_number() OVER (DISTRIBUTE BY pkgname1 SORT BY values DESC) rn from app_ins_similarity) a where a.rn >= 100"
    val pkgDisDF: DataFrame = sqlContext.sql(pkgDisSql)
    pkgDisDF.createOrReplaceTempView("pkg_distinct")

    val pkgRankSql = "select pkgname1,pkgname2,values,row_number() OVER (DISTRIBUTE BY pkgname1 SORT BY values DESC) rn from app_ins_similarity"
    val pkgRankDF: DataFrame = sqlContext.sql(pkgRankSql)
    pkgRankDF.createOrReplaceTempView("pkg_rank")

    /**
      * 本地
      */
    //  全输出
        val outSql: String = "select concat(pkgname1,'\t',pkgname2,'\t',values,'\t',rn) from " + "(select pkgname1,pkgname2,values,row_number() OVER (DISTRIBUTE BY pkgname1 SORT BY values DESC) rn from app_ins_similarity) a"

    //  局部输出
//    val outSql = "select concat(d.pkgname1,'\t',d.pkgname2,'\t',d.values,'\t',d.rn) from pkg_rank d inner join pkg_distinct c on d.pkgname1 = c.pkgname1"
    val outDF: DataFrame = sqlContext.sql(outSql)

    /**
      * 本地
      */
//        outDF.write.mode(SaveMode.Overwrite).format("text").save("H:\\output_orc")
        outDF.write.mode(SaveMode.Overwrite).format("text").save("hdfs://ns1/user/liuyiyang21/suanfa")
//    outDF.write.mode(SaveMode.Overwrite).format("text").save(args(1))
    sc.stop()
  }
}
