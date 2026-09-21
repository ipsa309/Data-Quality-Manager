import pandas as pd
import numpy as np
import datetime
import time
from pyspark.sql.types import *
from pyspark.sql.functions import *
import string
import requests
from pyspark.sql.functions import lit, col, create_map
from itertools import chain
from pyspark.dbutils import DBUtils
import cryptohash,re
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from concurrent.futures import ThreadPoolExecutor
import threading 
from collections import defaultdict
import json

class DQM:
  '''
  
  Initiate DQM class with passing requied field
  
  layer : This is data layer like raw,cleans etc
  source : source of data like TMFF, more etc
  table_path : Table path which is use to run qc
  format : format of table like delta, orc etc
  qc_table : give location for storing qc result
  
  example :

  dqm= DQM(layer='lob'
         ,source='tmff'
         ,table_path='/mnt/cdw_maestro_proddl/lob/damcoffw/tmff/tmff_cdw_job'
         ,format='delta'
        ,qc_table='/mnt/cdw_maestrodl/lob/damcoffw/poc/QC_TABLE')
        
  '''
  
  def __init__(self,layer,source,table_path,format,qc_table,batch_id,run_id,read_type,alert_on,config_file):
    self.spark=SparkSession.getActiveSession()
    self.batch_id=batch_id
    #self.save_error_log=save_error_log
    self.alert_on=alert_on
    self.config_file=config_file
    #self.dmail=dmail
    self.run_id=run_id
    self.read_type=read_type
    #self.primary_key=primary_key
    self.layer=layer
    self.source=source
    self.table_name=self.fn_getTableName(table_path).split('.')[0]
    
    self.data= self.fn_readData(table_path,format,read_type)
    self.view_name="DQM_"+self.table_name+"_view"
    self.qc_table=qc_table
    self.error_list=[]
    print('This is DQM table :',self.qc_table)
    #self.dutils=DBUtils(SparkSession.getActiveSession())
    self.schema = StructType([StructField('batch_id',StringType(), True),
                      StructField('run_id',StringType(), True),
                      StructField('layer',StringType(), True),
                      StructField('source',StringType(), True),
                      StructField('table_name',StringType(), True),
                      StructField('check_type',StringType(), True),
                    StructField('DQM_Type',StringType(), True),
                    StructField('DQM_On',StringType(), True),
                    StructField('observed_value',StringType(), True),
                    StructField('total_value',StringType(), True),
                    StructField('difference',StringType(), True),
                    StructField('run_time',StringType(), True),
                    StructField('time_taken',StringType(), True),
                    StructField('qc_run',StringType(), True),
                    StructField('qc_run_error',StringType(), True),
                    StructField('qc_status',StringType(), True),
                    StructField('qc_error',StringType(), True),
                    StructField('threshold_status',StringType(), True),
                    StructField('criticality',StringType(), True),
                    StructField('alert_flag',StringType(), True)])
    self.keys=['batch_id',
               'run_id',
               'layer',
               'source',
               'table_name',
               'check_type',
               'DQM_Type',
               'DQM_On',
               'observed_value',
               'total_value',
               'difference',
               'run_time',
               'time_taken',
               'qc_run',
               'qc_run_error',
               'qc_status',
               'qc_error',
               'threshold_status',
               'criticality',
               'alert_flag']
    # try:
    #   #_=spark.read.format('delta').load(qc_table)
    #   self.qc_table_name=self.fn_getTableName(qc_table)
    #   _=spark.read.format('orc').load(qc_table)  #changes for parallelism
    #   #self.qc_data= self.fn_readData(qc_table,'orc','full')
    #   self.qc_view_name="DQM_"+self.qc_table_name+"_view"
    #   print({'qc_table_name':self.qc_table_name,'qc_table_view':self.qc_view_name})
    #   print("Qc table is already present ")
    #    #changes for parallelism
    # except:
    #   print("Qc table is not exits. Creating new one")
    #   #df = self.spark.createDataFrame(data=[],schema = self.schema)
    #   #df.write.format('orc').save(qc_table)
    #   # try:
    #   #   df.write.format('orc').save(qc_table)
    #   # except:
    #   #   df.write.mode('append').format('orc').save(qc_table)
    #   self.qc_table_name=self.fn_getTableName(qc_table)
    #   self.qc_data= self.fn_readData(qc_table,'orc','full')
    #   self.qc_view_name="DQM_"+self.qc_table_name+"_view"
    #   print({'qc_table_name':self.qc_table_name,'qc_table_view':self.qc_view_name})
    
  def get_db_utils(self,spark):
      dbutils = None
      if spark.conf.get("spark.databricks.service.client.enabled") == "true":
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
      else:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
      return dbutils

  def run_io_tasks_in_parallel(self,tasks):
    with ThreadPoolExecutor(max_workers=len(tasks)) as executor:
      running_tasks = [executor.submit(task) for task in tasks]
      print("\n Number Of Thread running :",threading.activeCount(),"\n")
      return running_tasks

  def fn_get_qc_status(self, value, condition,criticality):
    #value= eval(str(value)) if str(value).isdigit() else str(value)
    #print(" Value ==>",type(value)," value=",value)
    #print("criticality in fn_get_qc_status",criticality)
    status=''
    rgb_value=''
    alert_flag='N'
    #print("condition",condition)
    if condition!='':
      _locals = locals()
      exec(condition,globals(), _locals)

      status = _locals['status']
      print("status: ",status)

      if status!='GREEN':
        #expected=condition.replace('"Pass"','').replace('"fail"','').replace('if','').replace('else','')
        expected=condition[:condition.find(':')].replace('if','')

        error=f"Status is {status}. ('{value}') records are not pass for the condition ('{expected}')"

        rgb_value=status
        status='fail'
        if criticality.lower() == 'y':
              alert_flag='Y'
      else:
        alert_flag='N'
        rgb_value=status
        status='success'
        error=None
    else:
      status,rgb_value,error=None,None,None
      
    #print(status,'\n',rgb_value,'\n',error)
    return status,rgb_value,error,alert_flag
  
  def fn_getTableName(self,table_path):
    table_path=table_path
    return table_path.split('/')[-2] if table_path.split('/')[-1]=='' else table_path.split('/')[-1]
  
  def fn_write_error_log(self,batch_id,run_id,layer,table_name,column_name,Dqm_type,data):
      table_data = create_map(list(chain(*((lit(name), col(name).cast('string')) for name in data.columns)))).alias("table_data")
      
      data=data.withColumn('batch_id',lit(batch_id)).withColumn('run_id',lit(run_id)).withColumn('layer',lit(layer)).withColumn('table_name',lit(table_name)).withColumn('column_name',lit(column_name)).withColumn('Dqm_type',lit(Dqm_type))
      error_log_table='/'.join(self.qc_table.split('/')[:-1])
      error_log_table=error_log_table+'/dqm_error_log'
      print(error_log_table)
      try:
        data.select('batch_id','run_id','layer','table_name','column_name','Dqm_type',table_data).write.mode('append').save(error_log_table)
      except:
        data.select('batch_id','run_id','layer','table_name','column_name','Dqm_type',table_data).write.mode('append').save(error_log_table)
      return 1
  
  def get_partition(self,path,size=0,cnt=0):
    dutils=DBUtils(SparkSession.getActiveSession())
    if size>0:
      return path
    elif cnt>6:
      return path
    else:
      path=dutils.fs.ls(path)[-1].path
      size=dutils.fs.ls(path)[-1].size
      cnt+=1
      return self.get_partition(path=path,size=size,cnt=cnt)
  
  def fn_readData(self,table_path,format,read_type):
    table_name=self.fn_getTableName(table_path).split('.')[0]
    #print(table_name)
    if read_type =='current':
      file_path=self.get_partition(table_path)
    else:
      file_path=table_path
    data=self.spark.read.format(format).option("header",True).load(file_path)
    data.cache()
    data.createOrReplaceTempView("DQM_"+table_name+"_view")
    return data
  
  def fn_send_dq_alert(self,to_list:str,subject:str,mail_body:str):
    #print("This is alert method",type(self.alert_on),self.alert_on)
    if self.alert_on.lower()=='y':
      #print("Insider alert on")
      mailobj={"mail_body": mail_body,"mail_list": to_list,"mail_subject": subject}
      mailobj=eval(str(mailobj))
      #url=f'https://prod-65.westeurope.logic.azure.com:443/workflows/cd07722c6ef84aea99fd0aedba50a24e/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=XmhNmXXri-aCVj9Clcon3qTZNkci0mfsTg3mFuAQH-0'
      url=self.config_file
      x = requests.post(url, json = mailobj, headers={'Content-Type':'application/json'})
      #print(x.status_code)
    
  def fn_generate_mail_body(self,check_type,check_name,column,value,total_value,end_time,start_time,qfn_status,qfn_error,qc_status,qc_error,rgb_value,criticality,alert_flag):
    mail_body=f'''Hi Team, <br/> Below are the detail of alert summary <br/> <table border=1>
  <thead><tr><th>Indicators</th><th>Value</th></tr></thead>
  <tbody>
    <tr><td>batch_id</td><td>{self.batch_id}</td></tr>
    <tr><td>run_id</td><td>{self.run_id}</td></tr>
    <tr><td>layer</td><td>{self.layer}</td></tr>
    <tr><td>source</td><td>{self.source}</td></tr>
    <tr><td>table_name</td><td>{self.table_name}</td></tr>
    <tr><td>check_type</td><td>{check_type}</td></tr>
    <tr><td>Check_Name</td><td>{check_name}</td></tr>
    <tr><td>column</td><td>{column}</td></tr>
    <tr><td>value</td><td>{str(value)}</td></tr>
    <tr><td>Total_value</td><td>{str(total_value)}</td></tr>
    <tr><td>qc_run_time</td><td>{str(end_time)}</td></tr>
    <tr><td>qc_time_taken</td><td>{str(end_time-start_time)}</td></tr>
    <tr><td>qfn_status</td><td>{qfn_status}</td></tr>
    <tr><td>qfn_error</td><td>{qfn_error}</td></tr>
    <tr><td>qc_status</td><td>{qc_status}</td></tr>
    <tr><td>qc_error</td><td>{qc_error}</td></tr>
    <tr bgcolor="{rgb_value}"><td>Severity</td><td>{rgb_value}</td></tr>
    <tr><td>criticality</td><td>{criticality}</td></tr>
    <tr><td>alert_flag</td><td>{alert_flag}</td></tr>
  </tbody>
</table><br/><br/> Thanks''';
    return mail_body
  # def get_error_list(self):
  #   print(self.error_list)
  #   if self.error_list is not None:
  #    with open('error_mail_data.json','w+') as f:
  #       json.dump(self.error_list, f)
  #    self.fn_generate_buisness_mail()

  # def fn_generate_buisness_mail(self):
  #   with open('error_mail_data.json', 'r') as f:
  #     error_data = json.load(f)
  #   grouped_dict = defaultdict(list)
  #   for error_grp in self.error_data: 
  #        grouped_dict[error_grp["to_list"]].append(error_grp)
  #        for to_list_value, group in grouped_dict.items():
  #           rows=[]
  #           for error in group:
  #              row=f'''<tr><td>{error["Source"]}</td>
  #              <td>{error["layer"]}</td>
  #              <td>{error["Table_Name"]}</td>
  #              <td>{error["Column_Name"]}</td>
  #              <td>{error["Check_Name"]}</td>
  #              <td>{error["Observerd_Value"]}</td>
  #              <td>{error["Total_Value"]}</td>
  #              <td>{error["Severity"]}</td></tr>'''
  #              rows.append(row)

  #        mail_body=f'''Hi Team, <br/> Below are the detail of alert summary <br/> 
  #                    <table border=1, id="data-table">
  #                      <thead>
  #                          <tr><th>Source</th>
  #                              <th>layer</th>
  #                              <th>Table_Name</th>
  #                              <th>Column_Name</th>
  #                              <th>Check_Name</th>
  #                              <th>Observerd_Value</th>
  #                              <th>Total_Value</th>
  #                              <th>Severity</th>
  #                          </tr>
  #                      </thead>
  #                      <tbody>
  #                      {''.join(rows)}
  #                      </tbody>
  #                      </table>
  #                      <br/><br/> Thanks''';
  #        mail_list=to_list_value
  #        subject=f"DQ Summay for source {self.source} and layer {self.layer}"
  #        self.fn_send_dq_alert(mail_list,subject,mail_body)
  
  def fn_getMessage(self,message):
    self.message=f"INFO : {datetime.datetime.now()} : {message}"
    return self.message
  
  def fn_writeData(self,data):
    logdata=dict(zip(self.keys,data))
    try:
      self.spark.createDataFrame(data=[logdata],schema=self.schema).write.format('orc').mode('append').save(self.qc_table)
    except:
      self.spark.createDataFrame(data=[logdata],schema=self.schema).write.format('orc').mode('append').save(self.qc_table)
    #print('\r\n--------------\r\n',logdata,'\r\n-----------------\r\n')
    return self.fn_getMessage("Log Saved Successfully")
  
  def fn_checkCount(self,check_type,column=None):
    start_time=datetime.datetime.now()
    #table_name=self.fn_getTableName(table_path)
    #data= self.fn_readData(table_path,format)
    if column==None:
      count=self.data.count()
      column='*'
    else:
      count=self.spark.sql(f"select count({column}) from {self.view_name}").take(1)
      count=count[0][0]
    end_time=datetime.datetime.now()
    data=[self.batch_id,self.run_id,self.layer,self.source,self.table_name,check_type,"checkCount",column,str(count),'',str(end_time),str(end_time-start_time),'success','']
    info=self.fn_writeData(data)
    #print(info)
    return info
  
  def fn_checkDuplicate(self,column,filtercondition,rgb_condition,check_type,criticality):
    start_time=datetime.datetime.now()
    #table_name=self.fn_getTableName(table_path)
    #data= self.fn_readData(table_path,format)
    count=self.data.count()
    distinctcount=self.data.select(column).distinct().count()
    end_time=datetime.datetime.now()
    data=[self.batch_id,self.run_id,self.layer,self.source,self.table_name,"checkDuplicate",column,str(count-distinctcount),'',str(end_time),str(end_time-start_time),'success','']
    info=self.fn_writeData(data)
    #print(info)
    return info
  
  # def fn_checkCountDistinct(self,column,filtercondition,rgb_condition,check_type,criticality):
  #   start_time=datetime.datetime.now()
  #   if column.replace('`','') not in [scol for scol in self.primary_key.split(',')] and column != '`*`' and column.startswith('`'):
  #       select_list=[scol for scol in self.primary_key.split(',')].append(column.replace('`',''))
  #   else:
  #       select_list=[scol for scol in self.primary_key.split(',')]
        
        
  #   #table_name=self.fn_getTableName(table_path)
  #   #data= self.fn_readData(table_path,format)
  #   sql="select a.* from  "+self.view_name+" a inner join (select `"+column+"`,count(`"+column+"`) from "+self.view_name+" group by `"+column+"` having count(`"+column+"`)>1) b on a.`"+column+"`=b.`"+column+"`"
  #   distinct_query_result=spark.sql(sql).select(select_list)
  #   distinctcount=distinct_query_result.count()
  #   end_time=datetime.datetime.now()
  #   data=[self.batch_id,self.run_id,self.layer,self.source,self.table_name,"checkCountDistinct",column,str(distinctcount),'',str(end_time),str(end_time-start_time),'success','']
  #   info=self.fn_writeData(data)
  #   #print(info)
  #   return info
  

  def fn_checkCountDistinct(self,column,rgb_condition,check_type,criticality,parameter,save_error_log,dmail,bmail,primary_key,filter_Column):
    
    Expected_cnt=''
    missing=''
    total_cnt=''
    qc_status,rgb_value,qc_error,alert_flag,qfn_error,qfn_status,current,missing_data,missing,total_cnt='','','','',None,'success','','','',''
    start_time=datetime.datetime.now()
    try:
#       col_list=parameter.split(',')
#       print("col_list1",col_list)
#       col_list=['`'+col+'`' for col in col_list]
#       col_list.append(column)
      if parameter == '' or parameter is None:
        col_list=column.split(',')
        col_list=['`'+col+'`' for col in col_list]

        
      else:
        col_list=parameter.split(',')
        col_list=['`'+col+'`' for col in col_list]
        col_list.append(column)
        
      
        
      if(str(primary_key)!=""):
          for col in col_list:       
              if col.replace('`','') not in [scol for scol in primary_key.split(',')] and col != '`*`' and col.startswith('`'):
                  select_list=[scol for scol in primary_key.split(',')]
                  select_list.append(col)
                  #print(select_list)
              else:
                    select_list=[scol for scol in primary_key.split(',')]
      else:
          select_list=[col_list]
      #print(select_list)
      join_cond=''
      if len(col_list) > 1:
          #print(col_list)
          for col in col_list:
              join_cond+="a."+col+"=b."+col+","
          join_cond=join_cond[:-1].replace(","," and ")
          column=','.join(col_list)
      else:
          join_cond="a."+column+"=b."+column

      column=column.replace('``','`')   
      join_cond=join_cond.replace('``','`')   
      
      select_list=eval(str(select_list).replace('``','`'))[0]
   
      total_sql="select "+column+" from "+self.view_name+" "+filter_Column+""
      
     
      sql="select a.* from  "+self.view_name+" a inner join (select "+column+",count(*) from "+self.view_name+" "+filter_Column+" group by "+column+" having count(*)>1) b on " +join_cond
      

      
      missing_data=self.spark.sql(sql).select(select_list).dropDuplicates()
      missing=missing_data.dropDuplicates().count()
      total_cnt=self.spark.sql(total_sql).count()
      missing_percentage=(missing/total_cnt)*100
      
     
      Expected_cnt=total_cnt-missing
      
      
      
      
      
      #print("rgb_condition",rgb_condition)
      
      qc_status,rgb_value,qc_error,alert_flag=self.fn_get_qc_status(missing_percentage,rgb_condition,criticality)
      #print("alert_flag",alert_flag)

    except Exception as e:
      qc_status,rgb_value,qc_error,alert_flag,qfn_error,qfn_status=None,None,None,None,str(e)[:4000],'fail'
      #print(e)

    end_time=datetime.datetime.now()
    data=[self.batch_id,self.run_id,self.layer,self.source,self.table_name,check_type,"Duplicate records",column,str(missing),str(Expected_cnt),'',str(end_time),str(end_time-start_time),qfn_status,qfn_error,qc_status,qc_error,rgb_value,criticality,alert_flag]
    
    if alert_flag == 'Y':
      mail_data=self.fn_generate_mail_body(check_type,"Duplicate records",column,str(missing),str(Expected_cnt),end_time,start_time,qfn_status,qfn_error,qc_status,qc_error,rgb_value,criticality,alert_flag)
      to_list=dmail
      subject=f"{rgb_value} alert from DQ for {self.source} {self.layer} layer and {self.table_name} object"
      
      self.fn_send_dq_alert(to_list=to_list,subject=subject,mail_body=mail_data)
      if bmail is not None:
        self.error_list.append({"source":self.source,"layer":self.layer,"TableName":self.table_name,"Column":column,"checkName":"Duplicate records","ObservedValue":str(missing),"TotalValue":str(Expected_cnt),"Severity":rgb_value,"to_list":bmail,"to_list":bmail})
      if save_error_log.lower()=='y':
        print("Save the error logs")
        self.fn_write_error_log(self.batch_id,self.run_id,self.layer,self.table_name,column,"Duplicate records",missing_data)
      

    info=self.fn_writeData(data)
    
    #print(info)
    return info


  # def fn_checkMin(self,column,filtercondition,rgb_condition,check_type,criticality):
  #   start_time=datetime.datetime.now()
  #   #table_name=self.fn_getTableName(table_path)
  #   #data= self.fn_readData(table_path,format)
  #   min=self.data.agg({column:'min'}).take(1)[0][0]
  #   end_time=datetime.datetime.now()
  #   data=[self.batch_id,self.run_id,self.layer,self.source,self.table_name,"checkMinimum",column,str(min),'',str(end_time),str(end_time-start_time),'success','']
  #   info=self.fn_writeData(data)
  #   #print(info)
  #   return info

  def fn_checkMin(self,column,filtercondition,rgb_condition,check_type,criticality,save_error_log,dmail,bmail,primary_key):
    qc_status,rgb_value,qc_error,alert_flag,qfn_error,qfn_status,current,missing_data,missing='','','','',None,'success','','',''
    start_time=datetime.datetime.now()
    try:
      if(str(primary_key)!=""):
        if column.replace('`','') not in [scol for scol in primary_key.split(',')] and column != '`*`' and column.startswith('`'):
          select_list=[scol for scol in primary_key.split(',')]
          select_list.append(column.replace('`',''))
        else:
          select_list=[scol for scol in primary_key.split(',')]
      else:
        select_list=[column]
      
      missing_data=self.data.filter(filtercondition).select(select_list)
      missing=missing_data.count()
      total_cnt=self.data.count()
      qc_status,rgb_value,qc_error,alert_flag=self.fn_get_qc_status(int(missing),rgb_condition,criticality)
      #print(qc_status,qc_error)
    except Exception as e:
      qc_status,rgb_value,qc_error,alert_flag,qfn_error,qfn_status=None,None,None,None,str(e)[:100],'fail'
      

    end_time=datetime.datetime.now()
    data=[self.batch_id,self.run_id,self.layer,self.source,self.table_name,check_type,"fn_checkMin",column,str(missing),str(total_cnt),'',str(end_time),str(end_time-start_time),qfn_status,qfn_error,qc_status,qc_error,rgb_value,criticality,alert_flag]
    
    if alert_flag == 'Y':
      mail_data=self.fn_generate_mail_body(check_type,"fn_checkMin",column,str(missing),'',end_time,start_time,qfn_status,qfn_error,qc_status,qc_error,rgb_value,criticality,alert_flag)
      to_list=dmail
      subject=f"{rgb_value} alert from DQ for {self.source} {self.layer} layer and {self.table_name} object"
      if bmail is not None:
        self.error_list.append({"source":self.source,"layer":self.layer,"TableName":self.table_name,"Column":column,"checkName":"Check Minimum","ObservedValue":str(missing),"TotalValue":str(total_cnt),"Severity":rgb_value,"to_list":bmail,"to_list":bmail})
      self.fn_send_dq_alert(to_list=to_list,subject=subject,mail_body=mail_data)
      if save_error_log.lower()=='y':
        self.fn_write_error_log(self.batch_id,self.run_id,self.layer,self.table_name,column,"fn_checkMin",missing_data)
    
    info=self.fn_writeData(data)
    #print(info)
    return info

  def fn_checkMax(self,column,filtercondition,rgb_condition,check_type,criticality,save_error_log,dmail,bmail,primary_key):
    qc_status,rgb_value,qc_error,alert_flag,qfn_error,qfn_status,current,missing_data,missing='','','','',None,'success','','',''
    start_time=datetime.datetime.now()
    try:
      
      if(str(primary_key)!=""):
        if column.replace('`','') not in [scol for scol in primary_key.split(',')] and column != '`*`' and column.startswith('`'):
          select_list=[scol for scol in primary_key.split(',')]
          select_list.append(column.replace('`',''))
        else:
          select_list=[scol for scol in primary_key.split(',')]
      else:
        select_list=[column]
      
      missing_data=self.data.filter(filtercondition).select(select_list)
      missing=missing_data.count()
      total_cnt=self.data.count()
      qc_status,rgb_value,qc_error,alert_flag=self.fn_get_qc_status(int(missing),rgb_condition,criticality)
      #print(qc_status,qc_error)
    except Exception as e:
      qc_status,rgb_value,qc_error,alert_flag,qfn_error,qfn_status=None,None,None,None,str(e)[:100],'fail'
      

    end_time=datetime.datetime.now()
    data=[self.batch_id,self.run_id,self.layer,self.source,self.table_name,check_type,"fn_checkMax",column,str(missing),'','',str(end_time),str(end_time-start_time),qfn_status,qfn_error,qc_status,qc_error,rgb_value,criticality,alert_flag]
    
    if alert_flag == 'Y':
      mail_data=self.fn_generate_mail_body(check_type,"fn_checkMax",column,str(missing),'',end_time,start_time,qfn_status,qfn_error,qc_status,qc_error,rgb_value,criticality,alert_flag)
      to_list=dmail
      subject=f"{rgb_value} alert from DQ for {self.source} {self.layer} layer and {self.table_name} object"
      if bmail is not None:
        self.error_list.append({"source":self.source,"layer":self.layer,"TableName":self.table_name,"Column":column,"checkName":"Check Maximum","ObservedValue":str(missing),"TotalValue":str(total_cnt),"Severity":rgb_value,"to_list":bmail})
      self.fn_send_dq_alert(to_list=to_list,subject=subject,mail_body=mail_data)
      if save_error_log.lower()=='y':
        self.fn_write_error_log(self.batch_id,self.run_id,self.layer,self.table_name,column,"fn_checkMax",missing_data)
    
    info=self.fn_writeData(data)
    #print(info)
    return info
  
  
  # def fn_checkMax(self,column,filtercondition,rgb_condition,check_type,criticality):
  #   start_time=datetime.datetime.now()
  #   #table_name=self.fn_getTableName(table_path)
  #   #data= self.fn_readData(table_path,format)
  #   max=self.data.agg({column:'max'}).take(1)[0][0]
  #   end_time=datetime.datetime.now()
  #   data=[self.batch_id,self.run_id,self.layer,self.source,self.table_name,"checkMaximum",column,str(max),'',str(end_time),str(end_time-start_time),'success','']
  #   info=self.fn_writeData(data)
  #   #print(info)
  #   return info
  
  def fn_checkAverage(self,column,filtercondition,rgb_condition,check_type,criticality):
    start_time=datetime.datetime.now()
    #table_name=self.fn_getTableName(table_path)
    #data= self.fn_readData(table_path,format)
    avg=self.data.agg({column:'avg'}).take(1)[0][0]
    qc_status,rgb_value,qc_error,alert_flag=self.fn_get_qc_status(avg,rgb_condition,criticality)
    end_time=datetime.datetime.now()
    data=[self.batch_id,self.run_id,self.layer,self.source,self.table_name,"checkAverage",column,str(avg),'',str(end_time),str(end_time-start_time),'success','',qc_status,qc_error,rgb_value,criticality,alert_flag]
    info=self.fn_writeData(data)
    #print(info)
    return info
  
  def fn_checkDatatype(self,column,filtercondition,rgb_condition,check_type,criticality,save_error_log,dmail,bmail,primary_key,parameter):

    qc_status,rgb_value,qc_error,alert_flag,qfn_error,qfn_status,current,missing_data,missing,count,='','','','',None,'success','','','',''
    start_time=datetime.datetime.now()
    datatype=None
    current_datatype=None
    try:
      datatype=[dtype for name, dtype in self.data.dtypes if name.replace('`','').lower() == column.replace('`','').lower()][0]
      if str(datatype).startswith('decimal'):
        datatype='decimal'
      else:
        datatype=str(datatype)
      current_datatype=parameter
      
      qc_status,rgb_value,qc_error,alert_flag=self.fn_get_qc_status(datatype,rgb_condition,criticality)
    except Exception as e:
      qc_status,rgb_value,qc_error,alert_flag,qfn_error,qfn_status=None,None,None,None,str(e)[:500],'fail'
      
    end_time=datetime.datetime.now()
    data=[self.batch_id,self.run_id,self.layer,self.source,self.table_name,check_type,"checkDatatype",column,str(datatype),str(current_datatype),'',str(end_time),str(end_time-start_time),qfn_status,qfn_error,qc_status,qc_error,rgb_value,criticality,alert_flag]
    
    if alert_flag == 'Y':
     mail_data=self.fn_generate_mail_body(check_type,"checkDatatype",column,str(datatype),str(current_datatype),end_time,start_time,qfn_status,qfn_error,qc_status,qc_error,rgb_value,criticality,alert_flag)
     to_list=dmail
     subject=f"{rgb_value} alert from DQ for {self.source} {self.layer} layer and {self.table_name} object"
     self.fn_send_dq_alert(to_list=to_list,subject=subject,mail_body=mail_data)
      #self.fn_write_error_log(self.batch_id,self.run_id,self.layer,self.table_name,column,"checkDatatype",missing_data)
     if bmail is not None:
        self.error_list.append({"source":self.source,"layer":self.layer,"TableName":self.table_name,"Column":column,"checkName":"checkDatatype","ObservedValue":str(datatype),"TotalValue":str(current_datatype),"Severity":rgb_value,"to_list":bmail})
    info=self.fn_writeData(data)
    #print(info)
    return info
  
  def fn_checkLength(self,column,filtercondition,rgb_condition,check_type,criticality,save_error_log,dmail,bmail,primary_key,filter_column):
    #print("Inside length check")
    missing=''
    total_cnt=''
    missing_percentage=0
    qc_status,rgb_value,qc_error,alert_flag,qfn_error,qfn_status,current,missing_data,missing,count='','','','',None,'success','','','',''
    start_time=datetime.datetime.now()

    try:
      
      if (str(primary_key)!=""):
        
        if column.replace('`','') not in [scol for scol in primary_key.split(',')] and column != '`*`' and column.startswith('`'):
          
          select_list=[scol for scol in primary_key.split(',')]
          select_list.append(column.replace('`',''))
        else:
          select_list=[scol for scol in primary_key.split(',')]
      else:
        select_list=[column]

      if filter_column is None or filter_column=='':
        data=self.data
      else:
        data=self.data.filter(filter_column)

      missing_data=data.filter(filtercondition).select(select_list)
      missing=missing_data.count()
      total_cnt=data.count()
      missing_percentage=(missing/total_cnt)*100


      
      qc_status,rgb_value,qc_error,alert_flag=self.fn_get_qc_status(missing_percentage,rgb_condition,criticality)
      #print(qc_status,qc_error)
    except Exception as e:
      qc_status,rgb_value,qc_error,alert_flag,qfn_error,qfn_status=None,None,None,None,str(e)[:500],'fail'
      

    end_time=datetime.datetime.now()
    data=[self.batch_id,self.run_id,self.layer,self.source,self.table_name,check_type,"checkLength",column,str(missing),str(total_cnt),'',str(end_time),str(end_time-start_time),qfn_status,qfn_error,qc_status,qc_error,rgb_value,criticality,alert_flag]
    
    if alert_flag == 'Y':
      mail_data=self.fn_generate_mail_body(check_type,"checkLength",column,str(missing),str(total_cnt),end_time,start_time,qfn_status,qfn_error,qc_status,qc_error,rgb_value,criticality,alert_flag)
      to_list=dmail
      subject=f"{rgb_value} alert from DQ for {self.source} {self.layer} layer and {self.table_name} object"
      if bmail is not None:
        self.error_list.append({"source":self.source,"layer":self.layer,"TableName":self.table_name,"Column":column,"checkName":"checkLength","ObservedValue":str(missing),"TotalValue":str(total_cnt),"Severity":rgb_value,"to_list":bmail})
      self.fn_send_dq_alert(to_list=to_list,subject=subject,mail_body=mail_data)
      if save_error_log.lower()=='y':
        self.fn_write_error_log(self.batch_id,self.run_id,self.layer,self.table_name,column,"checkLength",missing_data)
      
    info=self.fn_writeData(data)
    #print(info)
    return info
  
  def fn_checkValueRange(self,column,filtercondition,rgb_condition,check_type,criticality,save_error_log,dmail,bmail,primary_key,filter_column):
    missing=''
    total_value=''
    missing_percentage=0
    qc_status,rgb_value,qc_error,alert_flag,qfn_error,qfn_status,current,missing_data,missing,count,total_value='','','','',None,'success','','','','',''
    start_time=datetime.datetime.now()
    
    try:
      if(str(primary_key)!=""):
        if column.replace('`','') not in [scol for scol in primary_key.split(',')] and column != '`*`' and column.startswith('`'):
          select_list=[scol for scol in primary_key.split(',')]
          select_list.append(column.replace('`',''))
        else:
          select_list=[scol for scol in primary_key.split(',')]
      else:
        select_list=[column]

      if filter_column is None or filter_column=='':
        data=self.data
      else:
        data=self.data.filter(filter_column)
        
      missing_data=data.filter(filtercondition).select(select_list)
      missing=missing_data.count()
      total_value=data.count()
      missing_percentage=(missing/total_value)*100
      qc_status,rgb_value,qc_error,alert_flag=self.fn_get_qc_status(missing_percentage,rgb_condition,criticality)
      print(qc_status,qc_error)
    except Exception as e:
      qc_status,rgb_value,qc_error,alert_flag,qfn_error,qfn_status=None,None,None,None,str(e)[:500],'fail'
      
    end_time=datetime.datetime.now()
    data=[self.batch_id,self.run_id,self.layer,self.source,self.table_name,check_type,"checkValueRange",column,str(missing),str(total_value),'',str(end_time),str(end_time-start_time),qfn_status,qfn_error,qc_status,qc_error,rgb_value,criticality,alert_flag]
    
    if alert_flag == 'Y':
      mail_data=self.fn_generate_mail_body(check_type,"checkValueRange",column,str(missing),str(total_value),end_time,start_time,qfn_status,qfn_error,qc_status,qc_error,rgb_value,criticality,alert_flag)
      to_list=dmail
      subject=f"{rgb_value} alert from DQ for {self.source} {self.layer} layer and {self.table_name} object"
      if bmail is not None:
        self.error_list.append({"source":self.source,"layer":self.layer,"TableName":self.table_name,"Column":column,"checkName":"checkValueRange","ObservedValue":str(missing),"TotalValue":str(total_value),"Severity":rgb_value,"to_list":bmail})
      self.fn_send_dq_alert(to_list=to_list,subject=subject,mail_body=mail_data)
      if save_error_log.lower()=='y':
        self.fn_write_error_log(self.batch_id,self.run_id,self.layer,self.table_name,column,"checkValueRange",missing_data)
      
    info=self.fn_writeData(data)
    #print(info)
    return info
  
  def fn_checkCompleteness(self,column,filtercondition,rgb_condition,check_type,criticality,save_error_log,dmail,bmail,primary_key,dqfilter):
    missing=''

    qc_status,rgb_value,qc_error,alert_flag,qfn_error,qfn_status,current,missing_data,missing,count='','','','',None,'success','','','',''
    start_time=datetime.datetime.now()
    try:
      
      if(str(primary_key)!=""):
        if column.replace('`','') not in [scol for scol in primary_key.split(',')] and column != '`*`' and column.startswith('`'):
          select_list=[scol for scol in primary_key.split(',')]
          select_list.append(column.replace('`',''))
        else:
          select_list=[scol for scol in primary_key.split(',')]
      else:
        select_list=[column]

      filter_Column=' where 1=1 ' if dqfilter=='' else ' where '+dqfilter.replace('and','')
      count=self.spark.sql(f"select count(1) from {self.view_name} {filter_Column}").take(1)
      count=count[0][0]
      #print(filtercondition)
      missing_data=self.data.filter(filtercondition).select(select_list)
      missing=missing_data.count()
      
      missing=(missing/count)*100
      
      
      qc_status,rgb_value,qc_error,alert_flag=self.fn_get_qc_status(missing,rgb_condition,criticality)
      print(qc_status,qc_error)
    except Exception as e:
      qc_status,rgb_value,qc_error,alert_flag,qfn_error,qfn_status=None,None,None,None,str(e)[:500],'fail'

    end_time=datetime.datetime.now()
    data=[self.batch_id,self.run_id,self.layer,self.source,self.table_name,check_type,"checkCompleteness",column,str(missing),str(count),'',str(end_time),str(end_time-start_time),qfn_status,qfn_error,qc_status,qc_error,rgb_value,criticality,alert_flag]
    if alert_flag == 'Y':
      mail_data=self.fn_generate_mail_body(check_type,"checkCompleteness",column,str(missing),str(count),end_time,start_time,qfn_status,qfn_error,qc_status,qc_error,rgb_value,criticality,alert_flag)
      to_list=dmail
      subject=f"{rgb_value} alert from DQ for {self.source} {self.layer} layer and {self.table_name} object"
      if bmail is not None:
        self.error_list.append({"source":self.source,"layer":self.layer,"TableName":self.table_name,"Column":column,"checkName":"checkCompleteness","ObservedValue":str(missing),"TotalValue":str(count),"Severity":rgb_value,"to_list":bmail})
      self.fn_send_dq_alert(to_list=to_list,subject=subject,mail_body=mail_data)
      if save_error_log.lower()=='y':
        self.fn_write_error_log(self.batch_id,self.run_id,self.layer,self.table_name,column,"checkCompleteness",missing_data)
    
    info=self.fn_writeData(data)
    #print(info)
    return info
  
  def fn_checkNegative(self,column,filtercondition,rgb_condition,check_type,criticality,save_error_log,dmail,bmail,primary_key):
    qc_status,rgb_value,qc_error,alert_flag,qfn_error,qfn_status,current,missing_data,missing='','','','','','','','',''
    start_time=datetime.datetime.now()
    try:
      
      if column.replace('`','') not in [scol for scol in primary_key.split(',')] and column != '`*`' and column.startswith('`'):
        select_list=[scol for scol in primary_key.split(',')]
        select_list.append(column.replace('`',''))
      else:
        select_list=[scol for scol in primary_key.split(',')]
        
        
      missing_data=self.data.filter(filtercondition).select(select_list)
      missing=missing_data.count()
      total_value=self.data.count()
      qc_status,rgb_value,qc_error,alert_flag=self.fn_get_qc_status(int(missing),rgb_condition,criticality)
      #print(qc_status,qc_error)
      qfn_status='Success'
      qfn_error=None
    except Exception as e:
      qc_status,rgb_value,qc_error,alert_flag,qfn_error,qfn_status=None,None,None,None,str(e)[:100],'fail'
    end_time=datetime.datetime.now()
    data=[self.batch_id,self.run_id,self.layer,self.source,self.table_name,check_type,"checkNegative",column,str(missing),'','',str(end_time),str(end_time-start_time),qfn_status,qfn_error,qc_status,qc_error,rgb_value,criticality,alert_flag]
    
    if alert_flag == 'Y':
      mail_data=self.fn_generate_mail_body(check_type,"checkNegative",column,str(missing),'',end_time,start_time,qfn_status,qfn_error,qc_status,qc_error,rgb_value,criticality,alert_flag)
      to_list=dmail
      subject=f"{rgb_value} alert from DQ for {self.source} {self.layer} layer and {self.table_name} object"
      
      if bmail is not None:
        self.error_list.append({"source":self.source,"layer":self.layer,"TableName":self.table_name,"Column":column,"checkName":"checkNegative","ObservedValue":str(missing),"TotalValue":str(total_value),"Severity":rgb_value,"to_list":bmail})
        
      
      self.fn_send_dq_alert(to_list=to_list,subject=subject,mail_body=mail_data)
      if save_error_log.lower()=='y':
        self.fn_write_error_log(self.batch_id,self.run_id,self.layer,self.table_name,column,"checkNegative",missing_data)
    
    info=self.fn_writeData(data)
    #print(info)
    return info
  
  def fn_checkMissingValue(self,column,filtercondition,rgb_condition,check_type,criticality,filter_column,save_error_log,dmail,bmail,primary_key):
    missing=''
    total_data=''
    missing_percentage=0
    qc_status,rgb_value,qc_error,alert_flag,qfn_error,qfn_status,current,missing_data,missing='','','','',None,'success','','',''
    start_time=datetime.datetime.now()
    try:
    
      if(str(primary_key)!=""):
        if column.replace('`','') not in [scol for scol in primary_key.split(',')] and column != '`*`' and column.startswith('`'):
          select_list=[scol for scol in primary_key.split(',')]
          select_list.append(column.replace('`',''))
        else:
          select_list=[scol for scol in primary_key.split(',')]
      else:
        select_list=[column]
      
      if filter_column is None or filter_column=='':
        data=self.data
      else:
        data=self.data.filter(filter_column)
      total_data=data.count()
      
      missing_data=data.filter(filtercondition).select(select_list)
      missing=missing_data.count()
      missing_percentage=(missing/total_data)*100
      qc_status,rgb_value,qc_error,alert_flag=self.fn_get_qc_status(missing_percentage,rgb_condition,criticality)
      
    except Exception as e:
      qc_status,rgb_value,qc_error,alert_flag,qfn_error,qfn_status=None,None,None,None,str(e)[:100],'fail'
      

    end_time=datetime.datetime.now()
    data=[self.batch_id,self.run_id,self.layer,self.source,self.table_name,check_type,"checkMissingValue",column,str(missing),str(total_data),'',str(end_time),str(end_time-start_time),qfn_status,qfn_error,qc_status,qc_error,rgb_value,criticality,alert_flag]
    
    if alert_flag == 'Y':
      
      mail_data=self.fn_generate_mail_body(check_type,"checkMissingValue",column,str(missing),str(total_data),end_time,start_time,qfn_status,qfn_error,qc_status,qc_error,rgb_value,criticality,alert_flag)
      to_list=dmail
      subject=f"{rgb_value} alert from DQ for {self.source} {self.layer} layer and {self.table_name} object"
      if bmail is not None:
        
        self.error_list.append({"source":self.source,"layer":self.layer,"TableName":self.table_name,"Column":column,"checkName":"checkMissingValue","ObservedValue":str(missing),"TotalValue":str(total_data),"Severity":rgb_value,"to_list":bmail})
      self.fn_send_dq_alert(to_list=to_list,subject=subject,mail_body=mail_data)
      if save_error_log.lower()=='y':
        self.fn_write_error_log(self.batch_id,self.run_id,self.layer,self.table_name,column,"checkMissingValue",missing_data)
    
    info=self.fn_writeData(data)
    #print(info)
    return info
  
  def fn_checkValueTrend(self,column,filtercondition,rgb_condition,window_parameter,check_type,criticality):
    qc_status,rgb_value,qc_error,alert_flag,qfn_error,qfn_status,current,avg,value='','','','','','','','',''
    start_time=datetime.datetime.now()
    print("===============================")
    print(column,filtercondition,rgb_condition,window_parameter,check_type,criticality)
    print("===============================")
    try:
      sql=filtercondition.replace('?',self.view_name)
      column='*' if '(*)' in sql else column
      current=self.spark.sql(sql).take(1)
      current=current[0][0]
      print("current====>",current)
      if self.read_type == 'full':
        sql2=filtercondition.replace('?',self.view_name)
      #qc_sql=f"select coalesce(avg(observed_value),0) avg from (select observed_value From {self.qc_view_name} where layer='{self.layer}' and source='{self.source}' and table_name='{self.table_name}' and DQM_On='{column}' and DQM_Type='checkValueTrend' and threshold_status='GREEN' order by run_time desc limit {window_parameter})a"
      qc_sql=f"select coalesce(avg(observed_value),0) avg from (select observed_value From {self.qc_view_name} where layer='{self.layer}' and source='{self.source}' and table_name='{self.table_name}' and DQM_On='{column}' and DQM_Type='checkValueTrend' and threshold_status='GREEN' order by run_time desc limit {10})a"

      print(qc_sql)
      avg=self.spark.sql(qc_sql).take(1)
      avg=avg[0][0]

      print("avg====>",avg)
      if avg>0:
        value=(current-avg)/avg
        qc_status,rgb_value,qc_error,alert_flag=self.fn_get_qc_status(float(value),rgb_condition,criticality)
        value=value*100
      else:
        value=None;
        qc_status,rgb_value,qc_error,alert_flag='success','GREEN',None,'N'


      #print(qc_status,qc_error)
      qfn_status='Success'
      qfn_error=None
    except Exception as e:
      qc_status,rgb_value,qc_error,alert_flag,qfn_error,qfn_status=None,None,None,None,str(e)[:100],'fail'
      
    end_time=datetime.datetime.now()
    
    data=[self.batch_id,self.run_id,self.layer,self.source,self.table_name,check_type,"checkValueTrend",column,str(current),str(value),'',str(end_time),str(end_time-start_time),qfn_status,qfn_error,qc_status,qc_error,rgb_value,criticality,alert_flag]
    
    if alert_flag == 'Y':
      mail_data=self.fn_generate_mail_body(check_type,"checkValueTrend",column,str(value),'',end_time,start_time,qfn_status,qfn_error,qc_status,qc_error,rgb_value,criticality,alert_flag)
      to_list=self.dmail
      subject=f"{rgb_value} alert from DQ for {self.source} {self.layer} layer and {self.table_name} object"
      
      self.fn_send_dq_alert(to_list=to_list,subject=subject,mail_body=mail_data)
      #self.fn_write_error_log(self.batch_id,self.run_id,self.layer,self.table_name,column,"checkValueTrend",missing_data)
      
    info=self.fn_writeData(data)
    #print(info)
    return info

  
  def fn_customQualityCheck(self,sql_query,column,custom_check_name,src_query,rgb_condition,criticality,save_error_log,dmail,bmail,error_log_query,check_type="CustomCheck"):
    '''
    sql_query : give sql query with table_name as '?' symbol
    example : select max(case when column>1 then 1 else 0 end) from ?
    qc_name : quality check label
    '''
    qc_status,rgb_value,qc_error,alert_flag,qfn_error,qfn_status,current,output_data,output='','','','',None,'success','','',''
    column1=column
    # if(str(primary_key)!=""):
    #     if column1.replace('`','') not in [scol for scol in primary_key.split(',')] and column1 != '`*`' and column1.startswith('`'):
    #       select_list=[scol for scol in primary_key.split(',')]
    #       select_list.append(column1.replace('`',''))
    #     else:
    #       select_list=[scol for scol in primary_key.split(',')]
    # else:
    #   select_list=[column1]
    
    # select_list=','.join(['a.'+x for x in select_list])
    start_time=datetime.datetime.now()
    
    sql=sql_query.replace('?',self.view_name)
    print("\n***********************\n")
    print("final_sql:"+sql)
    print("\n")
    src_sql=src_query.replace('?',self.view_name)
    print("src_sql:"+src_sql)
    print("\n***********************\n")

  
    #sql_total=f"select "+custom_agg+"(*) from "+self.view_name+""
    #print(sql)
    error_data=''
    output=''
    total=''
    error_percentage=''
    try:
      output=self.spark.sql(sql).take(1)
      total=self.spark.sql(src_sql).take(1)
      
      if len(output)==1:
        value=output[0][0]
        error_percentage=(output[0][0]/total[0][0])*100
        #print("rgb_condition: ",rgb_condition)
        print("value: ",value)
        qc_status,rgb_value,qc_error,alert_flag=self.fn_get_qc_status(error_percentage,rgb_condition,criticality)
       
        print(qc_status)


        if float(value)!=0 and qc_status.lower() == 'fail' and save_error_log.lower()=='y':
            
           
            err_sql=error_log_query.replace('?',self.view_name)
            
            print("error log query : "+err_sql)
            print("\n***********************\n")
            error_data= self.spark.sql(err_sql)
            self.fn_write_error_log(self.batch_id,self.run_id,self.layer,self.table_name,column,custom_check_name,error_data)
        output=output[0][0]
        total=total[0][0]
      else:
        output=None
        total=None
        qfn_status='fail'
        qfn_error='Error: Multiple value return from result'
    except Exception as e:
      print(e,output,total)
      output=None
      total=None
      qfn_status='fail'
      qfn_error='Error: Problem with SQL query : '+str(e)[:200]
      
      
    end_time=datetime.datetime.now()
    data=[self.batch_id,self.run_id,self.layer,self.source,self.table_name,check_type,custom_check_name,column,str(output),str(total),'',str(end_time),str(end_time-start_time),qfn_status,qfn_error,qc_status,qc_error,rgb_value,criticality,alert_flag]
    
    if alert_flag == 'Y':
      mail_data=self.fn_generate_mail_body(check_type,custom_check_name,column,str(output),str(total),end_time,start_time,qfn_status,qfn_error,qc_status,qc_error,rgb_value,criticality,alert_flag)
      to_list=dmail
      subject=f"{rgb_value} alert from DQ for {self.source} {self.layer} layer and {self.table_name} object"
      
      self.fn_send_dq_alert(to_list=to_list,subject=subject,mail_body=mail_data)
      if bmail is not None:
        self.error_list.append({"source":self.source,"layer":self.layer,"TableName":self.table_name,"Column":column,"checkName":custom_check_name,"ObservedValue":str(output),"TotalValue":str(total),"Severity":rgb_value,"to_list":bmail})
      

    info=self.fn_writeData(data)
    #print(info)
    return info
  