#importing Libraries

from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import HiveContext
from pyspark import SparkContext


import sys
import datetime
import smtplib
import os.path

#importing user defined email utility function
import email_utility_fun

sc = SparkContext()
HiveContext = HiveContext(sc)


#defining a function for calcuating the difficulty level
def difficulty_level(cookTime,prepTime):


     if 'H' in cookTime or 'H' in prepTime:
          return "Hard"

     CT=int(''.join(c for c in cookTime if c not in 'PTM'))
     PT=int(''.join(c for c in prepTime if c not in 'PTM'))

     if  CT+PT>60:
         return "Hard"
     elif CT+PT in range(30,61):
             return "Medium"
     elif CT+PT<30:
             return "Easy"
     else:
             return "Unknown"





class Executor(object):
    def __init__(self, tasks=[]):
        self.tasks = tasks

    def run(self):
                   method_name='task_'+str(self.tasks)
                   method=getattr(self,method_name,lambda :'Invalid Task')
                   return method()
    def task_1(self):
	#changing the default encoding of ascii to utf-8
	reload(sys)
	sys.setdefaultencoding('utf-8')
	
	exists=os.path.exists("/staging/skprccm/recipes.json")
	if not exists:
		msg="Task_1 got falied.  Plese check for the input file path"
		email_utility_fun.body_table('meera.laxman@searshc.com','meera.laxman@searshc.com',"Task Execution Status",msg) 
        	sys.exit()

	#Loading the json data to a data frame
	jsonDF = HiveContext.read.json("staging/skprccm/recipes.json")
	jsonDF.write.format("orc").saveAsTable("jsonTest")
	ret_msg="\n\n\n\n....................... Successfully read the data and created the table in hive...............................................\n\n\n\n"
        return ret_msg
    def task_2(self):
	dfNew=HiveContext.sql("select * from jsonTest")
	cnt=dfNew.count()
	if cnt<1:
          msg="Task_2 got failed. Please check the input table as it is empty"
          email_utility_fun.body_table('meera.laxman@searshc.com','meera.laxman@searshc.com',"Task Execution Status",msg)
          sys.exit()

	#extracing the reoords which has the beef as one of ingredient
	searchfor = ['beef']
	dfNew = dfNew[lower(dfNew.ingredients).rlike('|'.join(searchfor))]
	udf_difficulty_level=udf(difficulty_level, StringType())
	dfNew=dfNew.withColumn("difficulty",udf_difficulty_level(dfNew.cookTime,dfNew.prepTime)).withColumn("date_of_execution",date_format(current_date(), "y-MM-dd"))
	dfNew.registerTempTable("output")
        dfNew['datePublished','cookTime','prepTime','difficulty'].show(10,False)
     
	#setting the configuration for activating the dynamic partition mode
	HiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")

	#Creating a partitoned table based on date of execution
	#NOTE:Below statement should be executed only first time later it should be commeneted as it is one time creation

	#HiveContext.sql("create table receipes_test(cooktime string,datepublished string,description string,image string,ingredients string,name string,preptime string,recipeyield string,url string,difficulty string) PARTITIONED BY (date_of_execution string) STORED as ORC TBLPROPERTIES ('orc.compress'='SNAPPY')")

	HiveContext.sql("INSERT OVERWRITE TABLE receipes_test PARTITION(date_of_execution) select * from output")

	ret_msg="\n\n\n\n......................... Successfully calculated the difficulty level fnd stored the result in hive table.............................\n\n\n\n"
        return ret_msg
    def task_3(self):
       msg="Task_3 got executed succesffly and the mail has been sent to all the receipients" 
       subject  = "Task Execution Status"
       email_utility_fun.body_table('meera.laxman@searshc.com','meera.laxman@searshc.com',subject,msg)

       ret_msg="\n\n\n\n.......................... Mail Sent Successfully...................................................................\n\n\n\n.."

       return ret_msg




#below is the code to exceute either individual pipe line or the entire pipeline

if (len(sys.argv)>1):
 e = Executor(sys.argv[1])
 print(e.run())
else:
 for i in range(1,4):
    print("\n\n\n\n ..................Running the Task %d...................................\n\n\n\n "%i)
    e = Executor(i)
    print(e.run())

