package com.bcg.analytics1

import com.bcg.casestudy._
import org.apache.spark.sql.functions._
import org.apache.log4j.{ Logger, Level }
import org.apache.spark.sql.{ SaveMode, DataFrameReader, Dataset }
import org.apache.spark.sql.DataFrame
import scala.reflect.io.File
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window

object CarCrashSolutions extends App {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  //Analytics 1: Find the number of crashes (accidents) in which number of persons killed are male?
  def noOfCarCrashsInWhichMaleDies_Analytics1(df1: DataFrame, df2: DataFrame) {

    df1.as("a").join(df2.as("b"), col("a.CRASH_ID") === col("b.CRASH_ID"), "inner")
      .where((col("a.PRSN_INJRY_SEV_ID") === "KILLED") && (col("a.PRSN_GNDR_ID") === "MALE"))
      .agg(count(col("a.CRASH_ID")).as("No_Of_Crashes_In_Which_Males_Killed"))
      .show(false)

  }

  //Analysis 2: How many two wheelers are booked for crashes?

  def noOfTwoWheelearsBookedForCrash_Analytics2(df1: DataFrame) {
    df1.filter((col("VEH_BODY_STYL_ID").contains("MOTORCYCLE")))
      .agg(count(col("CRASH_ID")).as("no_Of_Two_Wheelears_Booked_For_Crash"))
      .show(false)
  }

  //Analysis 3: Which state has highest number of accidents in which females are involved?

  def higestNumberOfCrashesInvolvedByWomen_Analytics3(df1: DataFrame, df2: DataFrame): Row = {
    df1.as("a").join(df2.as("b"), col("a.CRASH_ID") === col("b.CRASH_ID"), "inner")
      .where((col("a.PRSN_GNDR_ID") === "FEMALE"))
      .groupBy(col("b.VEH_LIC_STATE_ID").as("state"))
      .agg(count(col("a.CRASH_ID")).as("count_of_crashes"))
      .sort(desc("count_of_crashes"))
      .head()
  }

  // Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death

  def top5To15VehicleTypes_Analytics4(df1: DataFrame) {
    df1.groupBy(col("VEH_MAKE_ID").as("vehicle_type"))
      .agg(sum(col("TOT_INJRY_CNT") + col("DEATH_CNT")).as("count_of_injury_and_death"))
      .withColumn("Rank", dense_rank() over (Window.orderBy(col("count_of_injury_and_death").desc)))
      .where(col("Rank") >= 5 && col("Rank") <= 15)
      .show(false)

  }
  
  
  //5.	Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
  
  def topEthinicityByVehicleBodyStyle_Analytics5(df1: DataFrame, df2: DataFrame) {
    
    df1.as("a").join(df2.as("b"), col("a.CRASH_ID") === col("b.CRASH_ID"), "inner")
                  .groupBy(col("b.VEH_BODY_STYL_ID").as("body_style"), col("a.PRSN_ETHNICITY_ID").as("ethinic_group"))
                  .agg(count("a.CRASH_ID").as("crash_count"))
                  .withColumn("rank", dense_rank() over (Window.partitionBy(col("body_style")).orderBy(col("crash_count").desc)))
                  .where(col("rank")===1)
                  .show(false)
    
  }
  
   //Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
  
  def top5ZipCodesofAlcoholCrashes(df1: DataFrame) {
    
      val f = Seq("NA","NONE")
      
      df1.select(col("PRSN_ALC_SPEC_TYPE_ID"), col("DRVR_ZIP"), col("CRASH_ID"))
              .where((!col("PRSN_ALC_SPEC_TYPE_ID").isin(f  : _*)) && (col("DRVR_ZIP").isNotNull) )
              .groupBy(col("DRVR_ZIP"))
              .agg(count(col("CRASH_ID")).as("crash_count"))
              .withColumn("rank", dense_rank() over (Window.orderBy(col("crash_count").desc)))
              .where(col("rank") <= 5)
              .show(false)
  
    
  }
  
  //Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
  
  def distinctCrashCountWhereDamangeLevelISAbove4(df1: DataFrame, df2: DataFrame):Long = {
    
   return df1.as("a").join(df2.as("b"), col("a.CRASH_ID")===col("b.CRASH_ID"), "inner")
                .select(col("a.CRASH_ID"),col("b.DAMAGED_PROPERTY"),
                    col("a.FIN_RESP_TYPE_ID"),
                    (split(col("a.VEH_DMAG_SCL_1_ID"), " ")(1)).as("DamagedLevel_veh_1"),
                    (split(col("a.VEH_DMAG_SCL_2_ID"), " ")(1)).as("damagedLevel_veh_2"))
                .distinct()
                .where((col("DAMAGED_PROPERTY") === "NONE") && 
                      ((col("DamagedLevel_veh_1") > 4) || (col("DamagedLevel_veh_2") > 4))
                      && (col("a.FIN_RESP_TYPE_ID").contains("INSURANCE")) )
                .select(col("CRASH_ID"),col("DAMAGED_PROPERTY"))
                .distinct()
                .count()
    
  }
  
   
  
  
  
  

}