package com.bcg.casestudy

import org.apache.log4j.{Logger,Level}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.bcg.analytics1.CarCrashSolutions
import org.apache.spark.sql.expressions.Window

object MainProgram {
  
  def main(args: Array[String]): Unit = {
    
Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)

val spark = SparkSession.builder()
                          .appName("CarCrash")
                          .master("local[*]")
                          .getOrCreate()
                        
                          
 val readCsv = spark.read.option("header", "true")
 var personDf  = broadcast(readCsv.csv(config.primaryPersonFileInputPath))
 var unitsDf =   readCsv.csv(config.unitsInputFilePath).persist()
 var damageDf =  readCsv.csv(config.damagePropertes).persist()
 
 import spark.implicits._
 
 //Analytics 1: Find the number of crashes (accidents) in which number of persons killed are male?
 
  println("Analytics 1: Find the number of crashes (accidents) in which number of persons killed are male?")
 
  println(CarCrashSolutions.noOfCarCrashsInWhichMaleDies_Analytics1(personDf,unitsDf))
  
  
 //Analysis 2: How many two wheelers are booked for crashes? 
  
  println("Analysis 2: How many two wheelers are booked for crashes? ")
 
  println(CarCrashSolutions.noOfTwoWheelearsBookedForCrash_Analytics2(unitsDf))
  
 
//Analysis 3: Which state has highest number of accidents in which females are involved? 
  
  println("Analysis 3: Which state has highest number of accidents in which females are involved?")   
  
  println(CarCrashSolutions.higestNumberOfCrashesInvolvedByWomen_Analytics3(personDf, unitsDf))
  
  //Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death 
  
  println("Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death")                                          
 
   println(CarCrashSolutions.top5To15VehicleTypes_Analytics4(unitsDf))
 
 
 //Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
   
   println("Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style")
 
   println(CarCrashSolutions.topEthinicityByVehicleBodyStyle_Analytics5(personDf, unitsDf))
 
 //Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
 
 println("Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)")
              
 println(CarCrashSolutions.top5ZipCodesofAlcoholCrashes(personDf))
 
 //Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
 
 println("Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance")
 
 println(CarCrashSolutions.distinctCrashCountWhereDamangeLevelISAbove4(unitsDf,damageDf))
  
//8.	Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)
               
  //I did not understand the question, Statement is confucing at Top 10 Vehicle colour line, could not find any info related to colours.          
                                                 
                                
    
  }
  
}