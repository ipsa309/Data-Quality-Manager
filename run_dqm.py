import pandas as pd
import numpy as np
import datetime
import time
from pyspark.sql.types import *
from pyspark.sql.functions import *
import string
import requests
from pyspark.sql.functions import lit, col, create_map,count,current_date,round,sum,expr
from itertools import chain
from pyspark.dbutils import DBUtils
import cryptohash
import re
import json
import urllib,io
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from multiprocessing.pool import ThreadPool
import multiprocessing as mp
from DQM.dqm import DQM
from collections import defaultdict
from jinja2 import Template
import htmlmin

#import DQM


class run_DQM:

    def fn_parse_operator_value(self, value):
        out = {}
        if value != '':
            value = value.split(',')
            if len(value) == 1:
                out['input_value'] = re.sub('[^0-9.-]', '', value[0])
                out['input_operator'] = re.sub('[0-9.-]', '', value[0])
                out['count'] = 1 if out['input_value'] != '' and out['input_operator'] != '' else 0
            elif len(value) == 2:
                out['input_value'] = [
                    re.sub('[^0-9.-]', '', value[0]), re.sub('[^0-9.-]', '', value[1])]
                out['input_operator'] = [
                    re.sub('[0-9.-]', '', value[0]), re.sub('[0-9.-]', '', value[1])]
                out['count'] = 2 if out['input_value'][0] != '' and out['input_operator'][0] != '' else 0
        # print(out)
        return out

    def fn_get_rgbcondition(self,first_operator, first_threshold, second_threshold, second_operator, msg):
        conditon = ''
        if first_threshold != '' and second_operator != '' and msg=='GREEN':
            conditon = f"if value {first_operator} {first_threshold} and value {second_operator} {second_threshold} :\r\n\tstatus='{msg}'\r\n"
        elif first_threshold != '' and second_operator == '' and  msg=='GREEN':
            conditon = f"if value {first_operator} {first_threshold} : \r\n\tstatus='{msg}'\r\n"
        elif first_threshold != '' and second_operator != '' and msg!='GREEN':
             conditon = f"elif value {first_operator} {first_threshold} and value {second_operator} {second_threshold} :\r\n\tstatus='{msg}'\r\n"
        elif first_threshold != '' and second_operator == '' and  msg!='GREEN':
            conditon = f"elif value {first_operator} {first_threshold} : \r\n\tstatus='{msg}'\r\n"     
        else:
            conditon=f"else : \r\n\tstatus='error'\r\n"    
        
        return conditon
    
    def read_error_data(self,spark,QC_table,run_table_id,batch_id,run_id):
            print("QC_table:",QC_table)
            sql="select * from orc.`"+QC_table+"_"+run_table_id+"` where batch_id='"+batch_id+"'"+" and run_id='"+run_id+"' and lower(criticality)!='n' "
            error_data=spark.sql(sql)
            error_list=error_data.rdd.map(lambda row: row.asDict())
            #print(error_list)
            return error_list.collect()
    
   


    def fn_generate_buisness_mail(self,error_list,to_list_dict):
            grouped_dict = defaultdict(list)
            for error_grp in error_list:
                key=f"{error_grp['source']}-{error_grp['layer']}"
                for tolist_key, value in to_list_dict.items():
                    if value == key:
                        grouped_dict[tolist_key].append(error_grp)
            final_dict = defaultdict(list)
            for key in to_list_dict.keys():
                final_dict[key] = grouped_dict[key]
            
            for tolist_key, group in final_dict.items():
                
                rows=[]
                for error in group:
                  if error["threshold_status"] != 'GREEN':
                        row=f'''<tr><td>{error["source"]}</td>
                        <td>{error["layer"]}</td>
                        <td>{error["table_name"]}</td>
                        <td>{error["DQM_On"]}</td>
                        <td>{error["DQM_Type"]}</td>
                        <td>{error["observed_value"]}</td>
                        <td>{error["total_value"]}</td>
                        <td>{error["threshold_status"]}</td></tr>'''
                        rows.append(row)
                if len(rows)!=0:
                    mail_body=f'''Hi Team, <br/> Below are the detail of alert summary <br/> 
                                    <table border=1, id="data-table">
                                      <thead>
                                          <tr><th>Source</th>
                                              <th>layer</th>
                                              <th>Table_Name</th>
                                              <th>Column_Name</th>
                                              <th>Check_Name</th>
                                              <th>Observerd_Value</th>
                                              <th>Expected_Value</th>
                                              <th>Severity</th>
                                          </tr>
                                      </thead>
                                      <tbody>
                                      {''.join(rows)}
                                      </tbody>
                                      </table>
                                      <br/><br/> Thanks''';
                else:
                    mail_body=''
                mail_list=tolist_key
                subject=f"DQ Summay for source {error_grp['source']} and layer {error_grp['layer']}"
                #self.fn_send_dq_alert(mail_list,subject,mail_body)
                return mail_body,mail_list,subject
            
    def fn_qc_all_details(self,spark,table,run_table_id):
        qc_details_query = "select layer, source, table_name, case when check_type='CustomCheck' then 'Custom_check' else DQM_Type end as check_Type, case when check_type='CustomCheck' then DQM_Type else DQM_On end as Column, case when DQM_Type='checkDatatype' then observed_value else cast((observed_value/total_value)*100 as string) end as error_pct, threshold_status as severity, '' as benchmark  from (select *, row_number() over(partition by batch_id, table_name, DQM_Type, DQM_On order by run_time desc) rn from orc.`"+table+"_"+run_table_id+"` where date(run_time)=current_date()) where rn=1"

        qc_details = spark.sql(qc_details_query)

        return qc_details.collect()
            
            
    def fn_error_aggregate(self,spark,table,run_table_id,days):

        if days==0 :

            query="select *,row_number() over(partition by batch_id, table_name, DQM_Type, DQM_On order by run_time desc) rn,case when DQM_Type in ('checkDatatype', 'checkCompleteness', 'checkLength', 'checkMissingValue', 'checkValueRange', 'Duplicate records') then DQM_Type else 'Custom_check' end as DQM_Type_modified from orc.`"+table+"_"+run_table_id+"` where date(run_time) = current_date()"
        
        else:
            query = f"""select *,row_number() over(partition by batch_id, table_name, DQM_Type, DQM_On order by run_time desc) rn,case when DQM_Type in ('checkDatatype', 'checkCompleteness', 'checkLength', 'checkMissingValue', 'checkValueRange', 'Duplicate records') then DQM_Type else 'Custom_check' end as DQM_Type_modified from delta.`{table}` where date(run_time) > current_date() - {days}"""
            
        
        df = spark.sql(query)

        df_missing_fail = df.filter(col('qc_status').isin('fail') & col('threshold_status').isin('RED') & col('criticality').isin('y') & col('rn').isin(1)).groupBy("DQM_Type_modified").agg(count('*').alias("error_Value")).select(col('DQM_Type_modified'), col('error_Value'))
        df_missing_total = df.filter(col('rn').isin(1)).groupBy("DQM_Type_modified").agg(count('*').alias("total_Value")).select(col('DQM_Type_modified'), col('total_Value'))

        df_total=df_missing_fail.join(df_missing_total, on='DQM_Type_modified',how="inner")

        df_perc_final=df_total.withColumn("percentage_error",  round(( col('error_Value') / col('total_Value')) * 100))

        check_list = {}

        check_list={row['DQM_Type_modified']:row['percentage_error'] for row in df_perc_final.select('DQM_Type_modified', 'percentage_error').collect()}

        return check_list
    
    def fn_qc_all_details_json(self,spark,table,run_table_id):
        qc_all_deatils=self.fn_qc_all_details(spark,table,run_table_id)

        data = [
            {       
                "check_type":row['check_Type'],
                "layer": row['layer'],
                "source": row['source'],
                "table": row['table_name'],
                "column":row['Column'],
                "value%": row['error_pct'],
                "severity": row['severity'],
                "Benchmark":row['benchmark']
            } for row in  qc_all_deatils  # Fixed variable name from qc_details to qc_all_deatils
        ]

        grouped_data = defaultdict(list)
        for row in data:
            grouped_data[row["check_type"]].append(row)

        def calculate_rowspan_info(rows):
            rowspan_info = {}
            last_layer_source_table = None
            count = 0
            last_index = 0  # Initialize last_index to avoid reference before assignment

            for index, row in enumerate(rows):
                current_layer_source_table = (row['layer'], row['source'], row['table'])
                if current_layer_source_table == last_layer_source_table:
                    count += 1
                else:
                    if last_layer_source_table is not None:
                        rowspan_info[last_index] = count
                    last_layer_source_table = current_layer_source_table
                    count = 1
                    last_index = index
            rowspan_info[last_index] = count
            return rowspan_info


        template = Template("""<h2>{{check_type.upper()}}</h2><table style="width:100%; border-collapse:collapse;   font-family:Arial, sans-serif; font-size:14px;">
        <thead>
        <tr style="background-color:#f2f2f2; text-align:left;">
        <th style="padding:8px; border:1px solid #ddd;">Layer</th>
        <th style="padding:8px; border:1px solid #ddd;">Source</th>
        <th style="padding:8px; border:1px solid #ddd;">Table</th>
        <th style="padding:8px; border:1px solid #ddd;">Column</th>
        <th style="padding:8px; border:1px solid #ddd;">Value %</th>
        <th style="padding:8px; border:1px solid #ddd;">Severity</th>
        <!--<th style="padding:8px; border:1px solid #ddd;">Benchmark</th>-->
        </tr>
        </thead>
        <tbody>
        {% set rowspan_info = calculate_rowspan_info(rows) %}
        {% for row in rows %}
        <tr style="background-color:{% if loop.index0 % 2 == 0 %}#f9f9f9{% else %}#ffffff{% endif %};">
        {% if loop.index0 in rowspan_info %}
        <td rowspan="{{ rowspan_info[loop.index0] }}" style="padding:8px; border:1px solid #ddd;">{{ row.layer }}</td>
        <td rowspan="{{ rowspan_info[loop.index0] }}" style="padding:8px; border:1px solid #ddd;">{{ row.source }}</td>
        <td rowspan="{{ rowspan_info[loop.index0] }}" style="padding:8px; border:1px solid #ddd;">{{ row.table }}</td>
        {% endif %}
        <td style="padding:8px; border:1px solid #ddd;">{{ row.column }}</td>
        <td style="padding:8px; border:1px solid #ddd;">{{ row["value%"] }}</td>
        <td style="padding:8px; border:1px solid #ddd;">{{ row.severity }}</td>
        <!--<td style="padding:8px; border:1px solid #ddd;">{{ row.benchmark }}</td>-->
        </tr>
        {% endfor %}
        </tbody>
        </table>""")


        json_output = []
        check_list_day0=self.fn_error_aggregate(spark,table,run_table_id,0)

        for i, (check_type, rows) in enumerate(grouped_data.items(), start=1):
            html_content = template.render(check_type=check_type,rows=rows, calculate_rowspan_info=calculate_rowspan_info)
            json_output.append({
                "id": f"verticalTab{i}",
                "title": check_type,
                "progress": check_list_day0[check_type] if check_type in check_list_day0 else 0.0,
                "content": html_content
            })


        json_result = json.dumps(json_output, indent=4)


        return json_result
    
    def qc_status_weekly(self,spark,table,run_table_id):
            date_series_df = spark.range(7) \
            .withColumn("run_date", expr("date_sub(current_date(), cast(id as int))"))

        # Register the DataFrame as a temporary view to use it in SQL
            date_series_df.createOrReplaceTempView("date_series")

            query_summary="SELECT date(run_time) AS run_date,SUM(CASE WHEN qc_status = 'success' THEN 1 ELSE 0 END) AS success_count,SUM(CASE WHEN qc_status = 'fail' THEN 1 ELSE 0 END) AS fail_count,SUM(CASE WHEN qc_status = 'fail' AND criticality = 'y' THEN 1 ELSE 0 END) AS critical_fail_count FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY batch_id, table_name, DQM_Type, DQM_On ORDER BY run_time DESC) AS rn FROM delta.`"+table+"` WHERE date(run_time) > current_date() - 7 union SELECT *, ROW_NUMBER() OVER (PARTITION BY batch_id, table_name, DQM_Type, DQM_On ORDER BY run_time DESC) AS rn FROM orc.`"+table+"_"+run_table_id+"` WHERE date(run_time) = current_date() ) sub WHERE rn = 1 GROUP BY date(run_time)"

            qc_summary_df=spark.sql(query_summary)

            qc_summary_df.createOrReplaceTempView("qc_summary")


            Query_final=f"""SELECT 
            CASE 
                WHEN ds.run_date = current_date() THEN 'day_1'
                WHEN ds.run_date = current_date() - 1 THEN 'day_2'
                WHEN ds.run_date = current_date() - 2 THEN 'day_3'
                WHEN ds.run_date = current_date() - 3 THEN 'day_4'
                WHEN ds.run_date = current_date() - 4 THEN 'day_5'
                WHEN ds.run_date = current_date() - 5 THEN 'day_6'
                WHEN ds.run_date = current_date() - 6 THEN 'day_7'
            END AS run_day,
            COALESCE(qs.success_count, 0) AS success_count,
            COALESCE(qs.fail_count, 0) AS fail_count,
            COALESCE(qs.critical_fail_count, 0) AS critical_fail_count,
            ds.run_date
        FROM date_series ds
        LEFT JOIN qc_summary qs ON ds.run_date = qs.run_date
        ORDER BY ds.run_date DESC"""
            success_Fail=spark.sql(Query_final)



        # Assuming success_Fail is a DataFrame obtained from a previous operation
            success_Fail_df = success_Fail.groupBy('run_day') \
            .agg(
                sum('success_count').alias('success_count'),
                sum('fail_count').alias('fail_count'),
                sum('critical_fail_count').alias('critical_fail_count')
            ).orderBy('run_day') 

            return success_Fail_df.collect()

    
    def fn_write_html_to_table(self,spark,html_string,product):
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        
        # Get the current date in the required format using PySpark
        current_date_value = spark.sql("SELECT current_date()").collect()[0][0]     

        # Prepare the HTML string by escaping it properly
        html_string_escaped = html_string.replace("'", "\\'")       

        # Use the variables in the query
        query = f'''
        SELECT 
            '{current_date_value}' as run_date,
            '{product}' as product,
            '{html_string_escaped}' as html_data
        '''     

        #replace_where_condition=f"product = '{product}'"

        df = spark.sql(query)
        df.write.mode("overwrite").partitionBy('product').saveAsTable('dq_dashboard_all_products')
    
    def fn_create_json(self,spark,QC_table,run_table_id):
            check_list_day0=self.fn_error_aggregate(spark,QC_table,run_table_id,0)
            check_list_day7=self.fn_error_aggregate(spark,QC_table,run_table_id,7)

            qc_summary_df=self.qc_status_weekly(spark,QC_table,run_table_id)
            qc_all_details=self.fn_qc_all_details_json(spark,QC_table,run_table_id)
            
            # Define the data dictionary for JSON conversion
            data = {
                "page_title": "DQ Dashboard",
                "sidebar_title": "DQ View",
                "sidebar_menu": [
                    {"id": "dashboard", "title": "Dashboard"},
                    {"id": "verticalTabs", "title": "Detailed Report"}
                ],
                "content_sections": [
                    {
                        "id": "dashboard",
                        "type": "tabs",
                        "tabs": [
                            {
                                "id": "Day1",
                                "title": "Current Day Trend",
                                "type": "cards",
                                "cards": [
                                    {"title": check, "progress": per} for check,per in check_list_day0.items()
                                ],
                                "graph_section": {
                                "title": "Day Wise Trend",
                                "content": "This graph shows number of QCs passed or failed(including crirtical failures) for last 7 days.",
                                "legend":["success","fail","critical"],
                                "color":["green","#FF5733","#C70039"],
                                "data":{       
                                    "level_"+row['run_day'].split("_")[1]:{"index_name": row['run_day'],
                                    "success": row['success_count'],
                                    "fail": row['fail_count'],
                                    "critical": row['critical_fail_count'],
                                    "cartx_total": row['success_count'] + row['fail_count'] + row['critical_fail_count']} for row in qc_summary_df
                                }
                              }
                            },
                            {
                                "id": "Day7",
                                "title": "Weekly Trend",
                                "type": "cards",
                                "cards": [
                                    {"title": check, "progress": per} for check,per in check_list_day7.items()
                                ],
                                "graph_section": {
                                "title": "Day Wise Trend",
                                "content": "This graph shows number of QCs passed or failed(including crirtical failures) for last 7 days.",
                                "legend":["success","fail","critical"],
                                "color":["green","#FF5733","#C70039"],
                                "data":{       
                                    "level_"+row['run_day'].split("_")[1]:{"index_name": row['run_day'],
                                    "success": row['success_count'],
                                    "fail": row['fail_count'],
                                    "critical": row['critical_fail_count'],
                                    "cartx_total": row['success_count'] + row['fail_count'] + row['critical_fail_count']} for row in qc_summary_df
                                }
                              }
                            }
                        ],
                        "active_tab": "tab1"
                    },
                    {
                        "id": "users",
                        "type": "content",
                        "content": "Manage your users here."
                    },
                    {
                        "id": "settings",
                        "type": "content",
                        "content": "Adjust your settings here."
                    },
                    {
                        "id": "reports",
                        "type": "content",
                        "content": "View your reports here."
                    },
                    {
                        "id": "verticalTabs",
                        "type": "vertical_tabs",
                        "tabs": eval(qc_all_details),
                        "active_vertical_tab": "verticalTab1"
                    },
                    {
                        "id": "logout",
                        "type": "content",
                        "content": "You have been logged out."
                    }
                ]
            }

            return data
    
    def fn_create_dashboard_html(self,template_data):
            
            # Jinja2 Template
            template_string = '''
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>{{ data.page_title }}</title>
                <link rel="stylesheet" href="
                    https://cdn.jsdelivr.net/npm/charts.css/dist/charts.min.css">
 
                <style>
                    body {
                        margin: 0;
                        font-family: Arial, sans-serif;
                        display: flex;
                        flex-direction: column;
                        height: 100vh;
                    }

                    .container {
                        display: flex;
                        flex: 1;
                    }

                    .sidebar {
                        width: 150px;
                        background-color: #2c3e50;
                        color: white;
                        display: flex;
                        flex-direction: column;
                        transition: transform 0.3s ease;
                    }

                    .sidebar-header {
                        padding: 5px;
                        background-color: #34495e;
                        text-align: center;
                    }

                    .sidebar-menu {
                        list-style-type: none;
                        padding: 0;
                        margin: 0;
                    }

                    .sidebar-menu li {
                        padding: 10px 15px;
                        font-size:12px;
                        cursor: pointer;
                    }

                    .sidebar-menu li a {
                        color: white;
                        text-decoration: none;
                    }

                    .sidebar-menu li a:hover {
                        background-color: #1abc9c;
                    }

                    .sidebar-menu .active {
                        background-color: #1abc9c;
                        color: white;
                    }

                    .main-content {
                        flex: 1;
                        display: flex;
                        flex-direction: column;
                    }

                    .top-bar {
                        background-color: #ecf0f1;
                        padding: 5px 10px;
                        border-bottom: 1px solid #bdc3c7;
                        display: flex;
                        justify-content: space-between;
                        align-items: center;
                        height: 50px;
                    }

                    .top-bar h1 {
                        font-size: 1.2em;
                        margin: 0;
                    }

                    .content {
                        flex: 1;
                        padding: 20px;
                        overflow-y: auto;
                    }

                    .footer {
                        background-color: #ecf0f1;
                        padding: 10px;
                        text-align: center;
                        border-top: 1px solid #bdc3c7;
                    }

                    .tabs {
                        display: flex;
                        border-bottom: 2px solid #bdc3c7;
                        margin-bottom: 20px;
                        overflow-x: auto;
                    }

                    .tab-buttons {
                        display: flex;
                        flex: 1;
                    }

                    .tab-buttons button {
                        background: none;
                        border: none;
                        padding: 10px 20px;
                        cursor: pointer;
                        font-size: 1em;
                        border-bottom: 2px solid transparent;
                        margin-right: 5px;
                        transition: background-color 0.3s, border-bottom 0.3s;
                    }

                    .tab-buttons button.active {
                        border-bottom: 2px solid #1abc9c;
                        background-color: #ecf0f1;
                    }

                    .tab-buttons button:hover {
                        background-color: #ecf0f1;
                    }

                    .tab-content-container {
                        display: flex;
                        flex-direction: column;
                    }

                    .tab-content {
                        display: none;
                    }

                    .tab-content.active {
                        display: block;
                    }

                    .cards {
                        display: flex;
                        flex-wrap: wrap;
                        gap: 20px;
                    }

                    .card {
                        background-color: #ecf0f1;
                        padding: 15px;
                        border: 1px solid #bdc3c7;
                        border-radius: 8px;
                        text-align: center;
                        position: relative;
                        flex: 1 1 calc(20% - 20px);
                        min-width: 180px;
                        box-sizing: border-box;
                    }

                    .progress-circle {
                        width: 80px;
                        height: 80px;
                        background: conic-gradient(#1abc9c var(--progress), #ecf0f1 0);
                        border-radius: 50%;
                        display: flex;
                        align-items: center;
                        justify-content: center;
                        margin: 0 auto;
                        position: relative;
                    }

                    .progress-circle::before {
                        content: '';
                        width: 50px;
                        height: 50px;
                        background: white;
                        border-radius: 50%;
                        position: absolute;
                    }

                    .progress-circle span {
                        font-size: 1.2em;
                        font-weight: bold;
                        position: absolute;
                    }

                    .graph-section {
                        margin-top: 30px;
                        padding: 20px;
                        background-color: #ecf0f1;
                        border: 1px solid #bdc3c7;
                        border-radius: 8px;
                    }

                    .graph-section h2 {
                        margin: 0 0 20px;
                    }

                    .flexible-section {
                        display: flex;
                        flex-wrap: wrap;
                        gap: 20px;
                        margin-top: 20px;
                    }

                    .flexible-section.two-columns .flex-item {
                        flex: 1 1 calc(50% - 20px);
                    }

                    .flexible-section.four-columns .flex-item {
                        flex: 1 1 calc(25% - 20px);
                    }

                    .flex-item {
                        background-color: #ecf0f1;
                        padding: 15px;
                        border: 1px solid #bdc3c7;
                        border-radius: 8px;
                        box-sizing: border-box;
                    }

                    @media (max-width: 768px) {
                        .sidebar {
                            transform: translateX(-100%);
                            position: fixed;
                            height: 100%;
                            z-index: 1000;
                        }

                        .sidebar.open {
                            transform: translateX(0);
                        }

                        .main-content {
                            margin-left: 0;
                        }

                        .top-bar button {
                            display: block;
                        }
                    }

                    @media (min-width: 769px) {
                        .top-bar button {
                            display: none;
                        }
                    }

                    .menu-toggle {
                        background-color: #1abc9c;
                        border: none;
                        color: white;
                        padding: 8px 12px;
                        cursor: pointer;
                        font-size: 14px;
                        border-radius: 5px;
                    }

                    .overlay {
                        display: none;
                        position: fixed;
                        top: 0;
                        left: 0;
                        width: 100%;
                        height: 100%;
                        background-color: rgba(0, 0, 0, 0.5);
                        z-index: 500;
                    }

                    .overlay.open {
                        display: block;
                    }

                    .vertical-tabs {
                        display: flex;
                    }

                    .vertical-tabs-buttons {
                        display: flex;
                        flex-direction: column;
                        width: 200px;
                        background-color: #f8f9fa;
                        border-right: 2px solid #bdc3c7;
                    }

                    .vertical-tabs-buttons button {
                        background: none;
                        border: none;
                        padding: 10px;
                        cursor: pointer;
                        font-size: 1em;
                        border-left: 4px solid transparent;
                        text-align: left;
                        transition: background-color 0.3s, border-left 0.3s;
                    }

                    .vertical-tabs-buttons button.active {
                        border-left: 4px solid #1abc9c;
                        background-color: #ecf0f1;
                    }

                    .vertical-tabs-buttons button:hover {
                        background-color: #ecf0f1;
                    }

                    .vertical-tab-content {
                        flex: 1;
                        padding: 20px;
                    }

                    .card-title {
                        padding: 5px;
                        font-weight: bold;
                    }
                </style>
            </head>
            <body>
                <div class="overlay" id="overlay" onclick="toggleSidebar()"></div>
                <div class="container">
                    <aside class="sidebar" id="sidebar">
                        <div class="sidebar-header">
                            <h2>{{ data.sidebar_title }}</h2>
                        </div>
                        <ul class="sidebar-menu">
                            {% for item in data.sidebar_menu %}
                            <li onclick="showContent('{{ item.id }}')" id="{{ item.id }}Menu"><a href="#">{{ item.title }}</a></li>
                            {% endfor %}
                        </ul>
                    </aside>
                    <main class="main-content">
                        <header class="top-bar">
                            <h1 id="pageTitle">{{ data.page_title }}</h1>
                            <button class="menu-toggle" onclick="toggleSidebar()">Menu</button>
                        </header>
                        <section class="content">
                            {% for section in data.content_sections %}
                            <div id="{{ section.id }}" class="tab-content {% if section.id == 'dashboard' %}active{% endif %}">
                                {% if section.type == 'tabs' %}
                                <div class="tabs">
                                    <div class="tab-buttons">
                                        {% for tab in section.tabs %}
                                        <button class="tab-button {% if tab.id == section.active_tab %}active{% endif %}" onclick="openTab('{{ tab.id }}')">{{ tab.title }}</button>
                                        {% endfor %}
                                    </div>
                                </div>
                                <div class="tab-content-container">
                                    {% for tab in section.tabs %}
                                    <div id="{{ tab.id }}" class="tab-content {% if tab.id == section.active_tab %}active{% endif %}">
                                        {% if tab.type == 'cards' %}
                                        <div class="cards">
                                            {% for card in tab.cards %}
                                            <div class="card">
                                                <div class="progress-circle" style="--progress: {{ card.progress }}%;">
                                                    <span>{{ card.progress }}%</span>
                                                </div>
                                                <div class="card-title">{{ card.title }}</div>
                                            </div>
                                            {% endfor %}
                                        </div>
                                        <div class="graph-section">
                                            <h2>{{ tab.graph_section.title }}</h2>
                                            <p>{{ tab.graph_section.content }}</p>
                                            <table class="charts-css column multiple show-labels data-spacing-10 datasets-spacing-1">
                                            <caption> Front End Developer Salary </caption>
                                            <tbody>
                                        {% for cdata in tab.graph_section.data %}
            
            
                                            <tr>
                                                    <th scope="row"> {{tab.graph_section.data[cdata]['index_name']}} </th>
                                                    <td style="--size: calc({{tab.graph_section.data[cdata]['success']}}/{{tab.graph_section.data[cdata]['cartx_total']}}); --color: green;"><span class="data"> {{tab.graph_section.data[cdata]['success']}}  </span></td>
                                                    <td style="--size: calc({{tab.graph_section.data[cdata]['fail']}}/{{tab.graph_section.data[cdata]['cartx_total']}}); --color: #FF5733;"><span class="data"> {{tab.graph_section.data[cdata]['fail']}}  </span></td>
                                                    <td style="--size: calc({{tab.graph_section.data[cdata]['critical']}}/{{tab.graph_section.data[cdata]['cartx_total']}}); --color: #C70039;"><span class="data"> {{tab.graph_section.data[cdata]['critical']}}  </span></td>
                                            </tr>
            
                                                
                                        {% endfor %}
            
                                    
                </tbody>
            </table>
            <br/>
            <ul class="charts-css legend legend-inline" style="justify-content: center;">
            {% for legend in tab.graph_section.legend%}
                <li style="color:  {{tab.graph_section.color[loop.index-1]}};"> {{legend}} </li>
            {% endfor %}
            </ul>

                                        </div>    
                                        {% elif tab.type == 'flexible' %}
                                        <div class="flexible-section {{ tab.columns }}">
                                            {% for item in tab['items'] %}
                                            <div class="flex-item">{{ item }}</div>
                                            {% endfor %}
                                        </div>
                                        {% endif %}
                                    </div>
                                    {% endfor %}
                                </div>
                                {% elif section.type == 'content' %}
                                <p>{{ section.content }}</p>
                                {% elif section.type == 'vertical_tabs' %}
                                <div class="vertical-tabs">
                                    <div class="vertical-tabs-buttons">
                                        {% for tab in section.tabs %}
                                        <button class="tab-button {% if tab.id == section.active_vertical_tab %}active{% endif %}" onclick="openVerticalTab('{{ tab.id }}')">
                                            <div class="card">
                                                <div class="progress-circle" style="--progress: {{ tab.progress }}%;">
                                                    <span>{{ tab.progress }}%</span>
                                                </div>
                                                <div class="card-title">{{ tab.title }}</div>
                                            </div>
                                        </button>
                                        {% endfor %}
                                    </div>
                                    {% for tab in section.tabs %}
                                    <div class="vertical-tab-content" id="{{ tab.id }}" style="display: {% if tab.id == section.active_vertical_tab %}block{% else %}none{% endif %};">
                                        <p>{{ tab.content|safe }}</p>
                                    </div>
                                    {% endfor %}
                                </div>
                                {% endif %}
                            </div>
                            {% endfor %}
                        </section>
                    </main>
                </div>
                <script>
                    function toggleSidebar() {
                        const sidebar = document.getElementById('sidebar');
                        const overlay = document.getElementById('overlay');
                        sidebar.classList.toggle('open');
                        overlay.classList.toggle('open');
                    }

                    function showContent(contentId) {
                        const contents = document.querySelectorAll('.tab-content');
                        const items = document.querySelectorAll('.sidebar-menu li');
                        const pageTitle = document.getElementById('pageTitle');

                        contents.forEach(content => {
                            content.classList.remove('active');
                        });

                        items.forEach(item => {
                            item.classList.remove('active');
                        });

                        document.getElementById(contentId).classList.add('active');
                        document.querySelector(`#${contentId}Menu`).classList.add('active');
                        pageTitle.textContent = document.querySelector(`#${contentId}Menu a`).textContent;

                        // Reset tab state for Dashboard when it is shown
                        if (contentId === 'dashboard') {
                            const activeTab = localStorage.getItem('activeTab') || 'tab1';
                            openTab(activeTab);
                        }
                    }

                    function openTab(tabId) {
                        const tabs = document.querySelectorAll('#dashboard .tab-content');
                        const buttons = document.querySelectorAll('.tab-button');

                        tabs.forEach(tab => {
                            tab.classList.remove('active');
                        });

                        buttons.forEach(button => {
                            button.classList.remove('active');
                        });

                        document.getElementById(tabId).classList.add('active');
                        document.querySelector(`.tab-button[onclick="openTab('${tabId}')"]`).classList.add('active');
                        
                        // Save the active tab to localStorage
                        localStorage.setItem('activeTab', tabId);
                    }

                    function openVerticalTab(tabId) {
                        const tabs = document.querySelectorAll('.vertical-tab-content');
                        const buttons = document.querySelectorAll('.vertical-tabs-buttons .tab-button');

                        tabs.forEach(tab => {
                            tab.style.display = 'none';
                        });

                        buttons.forEach(button => {
                            button.classList.remove('active');
                        });

                        document.getElementById(tabId).style.display = 'block';
                        document.querySelector(`.vertical-tabs-buttons .tab-button[onclick="openVerticalTab('${tabId}')"]`).classList.add('active');
                    }
                </script>
            </body>
            </html>
            '''
            # Create a Jinja2 Template object
            template = Template(template_string)

            # Render the template with the data
            try:
                rendered_html = htmlmin.minify(template.render(data=template_data), remove_empty_space=True)
                html_string=f'''{rendered_html}'''
                return html_string
                
            except Exception as e:
                print(f"Error rendering template: {e}")
            
            
    def __init__(self,file_path,github_user,github_pao,QC_table,table_base_path,format,src,layer1,read_type='full',alert_on='F',config_file=None,product='DQ'):
      
      # if alert_on and config_file == None and mail_list ==None:
      # if alert_on == True and config_file == None and mail_list == None:
      #   assert 1==0,"Required config_file and mail_list when you set alert_on=True"
    

        spark=SparkSession.getActiveSession()
  
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
      #'/mnt/cdw_maestrodl/lob/damcoffw/poc/QC_TABLE'
      #/mnt/cdw_maestro_proddl/lob/damcoffw/tmff/
        try:
            _=spark.read.format('delta').load(QC_table)
            print("Qc table is already present ")
        except:
            print("Qc table is not exits. Creating new one")
            df = spark.createDataFrame(data=[],schema = self.schema)
            df.write.format('delta').save(QC_table)

      

        #read config file
        # Headers for authentication
        headers = {'Authorization': f'token {github_pao}'}
        # github_session = requests.Session()
        # github_session.auth = (github_user, github_pao)
        # providing raw url to download csv from github
        json_url = file_path
        print(json_url)
        response = requests.get(json_url, headers=headers)
        response.raise_for_status()  # Ensure we notice bad responses


        download = response.content
        # download = github_session.get(json_url).content
        data = pd.read_json(io.StringIO(download.decode('utf-8')))
        
        #data = pd.read_json(file_path)
        #data = pd.read_json("C:/Users/IPA069/Downloads/dq_config.json")
        if src is not None and layer1 is not None:
            data=data.loc[:,data.columns.isin([src])]
            data=data.reset_index()
            data=data[data['index']==layer1]
            data=data.set_index('index')

        list1 = []
        list2 = {}
        cryptodate=datetime.datetime.now()
        batch_id=''
        run_id=''
        run_table_id=''        
        to_list_dict={}
        for x in data.columns:


            data_dict = data[x]
            data_dict = data_dict.dropna(axis=0)
            data_dict = data_dict.to_dict()
            # if list(data_dict.keys())==[layer1]:
            #     for j in list([layer1]):
            #         for k in data_dict[j].keys():
                        

            #             result = {}
            #             result['source'] = x
            #             result['layer'] = j
            #             result['table'] = k
            #             batch_id=cryptohash.md5(str(str(str(cryptodate)[:10])+j+x))#.encode('utf-8'))
            #             run_id=cryptohash.md5(str(str(str(cryptodate)[:19])+j+x))#.encode('utf-8'))
            #             run_table_id=cryptohash.md5(str(str(str(cryptodate)[:21])+j+x))#.encode('utf-8'))
            #             # list1.append(f'"{result}"')
            #             key = f'{x}_{j}_{k}'
            #             list1.append(key)
            #             list2[key] = data_dict[j][k]
            # else:
            for j in list(data_dict.keys()):

                for k in data_dict[j].keys():

                    result = {}
                    result['source'] = x
                    result['layer'] = j
                    result['table'] = k
                    batch_id=cryptohash.md5(str(str(str(cryptodate)[:10])+j+x))#.encode('utf-8'))
                    run_id=cryptohash.md5(str(str(str(cryptodate)[:19])+j+x))#.encode('utf-8'))
                    run_table_id=cryptohash.md5(str(str(str(cryptodate)[:21])+j+x))#.encode('utf-8'))
                    # list1.append(f'"{result}"')
                    key = f'{x}-{j}-{k}'
                    list1.append(key)
                    list2[key] = data_dict[j][k]


        list_4 = {}
        for x in set(list1):
            # print(eval(x))
            tmp = ''
            source = x.split("-")[0]
            layer = x.split("-")[1]
            tables = x.split("-")[2]
            #print(tables)

            obj = f"DQM(layer=\'{layer}'\
              ,source=\'{source}'\
              ,table_path=\'{table_base_path}{tables}'\
              ,format=\'{format}\'\
              ,qc_table=\'{QC_table}_{run_table_id}\'\
              ,batch_id=\'{batch_id}\'\
              ,run_id=\'{run_id}\'\
              ,read_type=\'{read_type}\'\
              ,alert_on=\'{alert_on}\'\
              ,config_file=\'{config_file}\')"
            
            
            condition = ''
            col_data = list2[x]
            # print(check_prop)
            for column in col_data:
                for checktype in col_data[column]:
                    for check in col_data[column][checktype]:
                        check_prop = col_data[column][checktype][check]
                        if 'Conformity' in checktype:
                            
                            if check_prop['active_flag'].lower() == 'y':
                                # condition=''
                                #print("all checks",check)
                                
                                if check.lower() == 'data_type':
                                    condition = f"\r\nif value == '{check_prop['parameter'].lower()}':\r\n\tstatus='GREEN' \r\nelse: \r\n\tstatus='RED'\r\n"
                                    
                                    tmp = obj+"." + \
                                        f"fn_checkDatatype(column='{column}',filtercondition=None,rgb_condition='''{condition}''',check_type='conformity',criticality='{check_prop['creticality']}',save_error_log='{check_prop['save_error_log']}',dmail='{check_prop['dmail']}',bmail='{check_prop['bmail']}',primary_key='{check_prop['log_columns']}',parameter='{check_prop['parameter']}')"
                                    value=f"{source}-{layer}"
                                    to_list_dict[check_prop['bmail']]=value
                                elif check.lower() == 'null':
                                    #print("Inside NULL")
                                    filter_Column=check_prop['filter_condition']
                                    condition = self.fn_get_rgbcondition(
                                        check_prop['pfoperator'], check_prop['pfvalue'], check_prop['psvalue'], check_prop['psoperator'], msg='GREEN')
                                    condition += self.fn_get_rgbcondition(
                                        check_prop['yfoperator'], check_prop['yfvalue'], check_prop['ysvalue'], check_prop['ysoperator'], msg='YELLOW')
                                    condition += self.fn_get_rgbcondition(
                                        check_prop['rfoperator'], check_prop['rfvalue'], check_prop['rsvalue'], check_prop['rsoperator'], msg='RED')
                                    condition += f"else : \r\n\tstatus='error'\r\n" 
                                    if check_prop['parameter'] == '-1':
                                        filtercondition = f"`{column}` == {check_prop['parameter']}"
                                    else:
                                        filtercondition = f"`{column}` is {check_prop['parameter'].lower()}"
                                    tmp = obj+"." + \
                                            f"fn_checkMissingValue(column='{column}',filtercondition='{filtercondition}',rgb_condition='''{condition}''',check_type='conformity',criticality='{check_prop['creticality']}',filter_column='{filter_Column}',save_error_log='{check_prop['save_error_log']}',dmail='{check_prop['dmail']}',bmail='{check_prop['bmail']}',primary_key='{check_prop['log_columns']}')"
                                    # key=x+'-'+column+'checkMissingValue'
                                    # to_list_dict[key]=check_prop['bmail']
                                    value=f"{source}-{layer}"
                                    to_list_dict[check_prop['bmail']]=value
                                    #print(tmp)
                                elif check.lower() == 'unique':
                                    #print("Inside Unique")
                                   

                                    filter_Column=' where 1=1 ' if check_prop['filter_condition']=='' else ' where '+check_prop['filter_condition']
                                    print(filter_Column)
                                    condition = self.fn_get_rgbcondition(check_prop['pfoperator'],check_prop['pfvalue'],check_prop['psvalue'],check_prop['psoperator'],'GREEN')
                                    condition += self.fn_get_rgbcondition(check_prop['yfoperator'],check_prop['yfvalue'],check_prop['ysvalue'],check_prop['ysoperator'],'YELLOW')
                                    condition += self.fn_get_rgbcondition(check_prop['rfoperator'],check_prop['rfvalue'],check_prop['rsvalue'],check_prop['rsoperator'],'RED')
                                    condition+= f"else : \r\n\tstatus='error'\r\n" 
                                    tmp = obj+"." + \
                                        f"fn_checkCountDistinct(column='{column}',rgb_condition='''{condition}''',check_type='conformity',criticality='{check_prop['creticality']}',parameter='{check_prop['parameter']}',save_error_log='{check_prop['save_error_log']}',dmail='{check_prop['dmail']}',bmail='{check_prop['bmail']}',primary_key='{check_prop['log_columns']}',filter_Column='{filter_Column}')"
                                    #print(tmp)
                                    # key=x+'-'+column+'Duplicate records'
                                    # to_list_dict[key]=check_prop['bmail']
                                    value=f"{source}-{layer}"
                                    to_list_dict[check_prop['bmail']]=value

                                elif check.lower() == 'length':
                                    filter_Column=check_prop['filter_condition']
                                    
                                    if check_prop['parameter'].lower() == 'equal':
                                            filtercondition = f"length({column}) = {check_prop['pv1']}"
                                    elif check_prop['parameter'].lower() == 'between':
                                            filtercondition = f"length({column}) between {check_prop['pv1']} and {check_prop['pv2']}"
                                    else:
                                            filtercondition = f"length({column}) != {check_prop['pv1']}"
                                    
                                    
#                                     if out['count'] == 1:
#                                         filtercondition = f"length({column}) {out['input_operator']} {out['input_value']}"
#                                     elif out['count'] == 2:
#                                         filtercondition = f"length({column}) between {eval(out['input_value'][0])} and {eval(out['input_value'][1])}"

                                    condition = self.fn_get_rgbcondition(
                                        check_prop['pfoperator'], check_prop['pfvalue'], check_prop['psvalue'], check_prop['psoperator'], msg='GREEN')
                                    condition += self.fn_get_rgbcondition(
                                        check_prop['yfoperator'], check_prop['yfvalue'], check_prop['ysvalue'], check_prop['ysoperator'], msg='YELLOW')
                                    condition += self.fn_get_rgbcondition(
                                        check_prop['rfoperator'], check_prop['rfvalue'], check_prop['rsvalue'], check_prop['rsoperator'], msg='RED')
                                    condition += f"else : \r\n\tstatus='error'\r\n" 
                                    tmp = obj+"." + \
                                        f"fn_checkLength(column='{column}',filtercondition='{filtercondition}',rgb_condition='''{condition}''',check_type='conformity',criticality='{check_prop['creticality']}',save_error_log='{check_prop['save_error_log']}',dmail='{check_prop['dmail']}',bmail='{check_prop['bmail']}',primary_key='{check_prop['log_columns']}',filter_column='{filter_Column}')"
                                    #print(tmp)
                                    # key=x+'-'+column+'Duplicate records'
                                    # to_list_dict[key]=check_prop['bmail']
                                    value=f"{source}-{layer}"
                                    to_list_dict[check_prop['bmail']]=value

                                elif check.lower() == 'discrete_range':
                                    filter_Column=check_prop['filter_condition']
#                                     out = str(check_prop['parameter'].split(',')).replace(
#                                         '[', '(').replace(']', ')')
                                    out=str("("+check_prop['parameter']+")")
#                                     print("out",out)
#                                     print("parameter",check_prop['parameter'])
                                    filtercondition = f'"lower(`{column}`) not in {out.lower()}"'
                                    condition = self.fn_get_rgbcondition(
                                        check_prop['pfoperator'], check_prop['pfvalue'], check_prop['psvalue'], check_prop['psoperator'], msg='GREEN')
                                    condition += self.fn_get_rgbcondition(
                                        check_prop['yfoperator'], check_prop['yfvalue'], check_prop['ysvalue'], check_prop['ysoperator'], msg='YELLOW')
                                    condition += self.fn_get_rgbcondition(
                                        check_prop['rfoperator'], check_prop['rfvalue'], check_prop['rsvalue'], check_prop['rsoperator'], msg='RED')
                                    condition += f"else : \r\n\tstatus='error'\r\n"
                                    tmp = obj+"." + \
                                        f"fn_checkValueRange(column='{column}',filtercondition={filtercondition},rgb_condition='''{condition}''',check_type='conformity',criticality='{check_prop['creticality']}',save_error_log='{check_prop['save_error_log']}',dmail='{check_prop['dmail']}',bmail='{check_prop['bmail']}',primary_key='{check_prop['log_columns']}',filter_column='{filter_Column}')"
                                    value=f"{source}-{layer}"
                                    to_list_dict[check_prop['bmail']]=value
                                elif check.lower() == 'continuous_range':
                                    filter_Column=check_prop['filter_condition']
                                    #out=str(lower(`{column}`) < '{check_prop['pv1']}')+"and"+str(lower(`{column}`) > '{check_prop['pv2']}')
                                    filtercondition = f"lower(`{column}`) < {check_prop['pv1']} or lower(`{column}`) > {check_prop['pv2']}"
                                    
                                    condition = self.fn_get_rgbcondition(
                                        check_prop['pfoperator'], check_prop['pfvalue'], check_prop['psvalue'], check_prop['psoperator'], msg='GREEN')
                                    condition += self.fn_get_rgbcondition(
                                        check_prop['yfoperator'], check_prop['yfvalue'], check_prop['ysvalue'], check_prop['ysoperator'], msg='YELLOW')
                                    condition += self.fn_get_rgbcondition(
                                        check_prop['rfoperator'], check_prop['rfvalue'], check_prop['rsvalue'], check_prop['rsoperator'], msg='RED')
                                    condition += f"else : \r\n\tstatus='error'\r\n" 
                                    
                                    tmp = obj+"." + \
                                        f"fn_checkValueRange(column='{column}',filtercondition='{filtercondition}',rgb_condition='''{condition}''',check_type='conformity',criticality='{check_prop['creticality']}',save_error_log='{check_prop['save_error_log']}',dmail='{check_prop['dmail']}',bmail='{check_prop['bmail']}',primary_key='{check_prop['log_columns']}',filter_column='{filter_Column}')"
                                    value=f"{source}-{layer}"
                                    to_list_dict[check_prop['bmail']]=value
          
                                elif check.lower() == 'completeness':
                                    #print("Inside completeness")
                                    
                                    
                                    filter_Column=' and 1=1 ' if check_prop['filter_condition']=='' else ' and '+check_prop['filter_condition']
                                    single_quote_filter=filter_Column.replace("\"", "'")
                                    
                                    filtercondition = f"(`{column}` is not {check_prop['parameter'].lower()} or replace({column},\' \',\'\') != \'\') {single_quote_filter} "
                                    
                                    condition = self.fn_get_rgbcondition(
                                        check_prop['pfoperator'], check_prop['pfvalue'], check_prop['psvalue'], check_prop['psoperator'], msg='GREEN')
                                    condition += self.fn_get_rgbcondition(
                                        check_prop['yfoperator'], check_prop['yfvalue'], check_prop['ysvalue'], check_prop['ysoperator'], msg='YELLOW')
                                    condition += self.fn_get_rgbcondition(
                                        check_prop['rfoperator'], check_prop['rfvalue'], check_prop['rsvalue'], check_prop['rsoperator'], msg='RED')
                                    condition += f"else : \r\n\tstatus='error'\r\n" 
                                    tmp = obj+"." + \
                                        f"fn_checkCompleteness(column='{column}',filtercondition=\"{filtercondition}\",rgb_condition='''{condition}''',check_type='conformity',criticality='{check_prop['creticality']}',save_error_log='{check_prop['save_error_log']}',dmail='{check_prop['dmail']}',bmail='{check_prop['bmail']}',primary_key='{check_prop['log_columns']}',dqfilter='{filter_Column}')"
                                    #print(tmp)
                                    value=f"{source}-{layer}"
                                    to_list_dict[check_prop['bmail']]=value

                                elif check.lower() == 'min_value':
                                    filtercondition = f"`{column}` < {check_prop['parameter']}"
                                    condition = self.fn_get_rgbcondition(
                                        check_prop['pfoperator'], check_prop['pfvalue'], check_prop['psvalue'], check_prop['psoperator'], msg='GREEN')
                                    condition += self.fn_get_rgbcondition(
                                        check_prop['yfoperator'], check_prop['yfvalue'], check_prop['ysvalue'], check_prop['ysoperator'], msg='YELLOW')
                                    condition += self.fn_get_rgbcondition(
                                        check_prop['rfoperator'], check_prop['rfvalue'], check_prop['rsvalue'], check_prop['rsoperator'], msg='RED')
                                    condition+=f"else : \r\n\tstatus='error'\r\n"
                                    tmp = obj+"." + \
                                        f"fn_checkMin(column='{column}',filtercondition=\"{filtercondition.lower()}\",rgb_condition='''{condition}''',check_type='conformity',criticality='{check_prop['creticality']}',save_error_log='{check_prop['save_error_log']}',dmail='{check_prop['dmail']}',bmail='{check_prop['bmail']}',primary_key='{check_prop['log_columns']}')"
                                    value=f"{source}-{layer}"
                                    to_list_dict[check_prop['bmail']]=value
                                elif check.lower() == 'max_value':
                                    filtercondition = f"`{column}` > {check_prop['parameter']}"
                                    condition = self.fn_get_rgbcondition(
                                        check_prop['pfoperator'], check_prop['pfvalue'], check_prop['psvalue'], check_prop['psoperator'], msg='GREEN')
                                    condition += self.fn_get_rgbcondition(
                                        check_prop['yfoperator'], check_prop['yfvalue'], check_prop['ysvalue'], check_prop['ysoperator'], msg='YELLOW')
                                    condition += self.fn_get_rgbcondition(
                                        check_prop['rfoperator'], check_prop['rfvalue'], check_prop['rsvalue'], check_prop['rsoperator'], msg='RED')
                                    condition += f"else : \r\n\tstatus='error'\r\n"
                                    tmp = obj+"." + \
                                        f"fn_checkMax(column='{column}',filtercondition=\"{filtercondition.lower()}\",rgb_condition='''{condition}''',check_type='conformity',criticality='{check_prop['creticality']}',save_error_log='{check_prop['save_error_log']}',dmail='{check_prop['dmail']}',bmail='{check_prop['bmail']}',primary_key='{check_prop['log_columns']}')"
                                    value=f"{source}-{layer}"
                                    to_list_dict[check_prop['bmail']]=value
                                elif check == 'CUSTOM_CHECK':
                                    #print("Inside Custom query")
                                    condition = self.fn_get_rgbcondition(
                                        check_prop['pfoperator'], check_prop['pfvalue'], check_prop['psvalue'], check_prop['psoperator'], msg='GREEN')
                                    condition += self.fn_get_rgbcondition(
                                        check_prop['yfoperator'], check_prop['yfvalue'], check_prop['ysvalue'], check_prop['ysoperator'], msg='YELLOW')
                                    condition += self.fn_get_rgbcondition(
                                        check_prop['rfoperator'], check_prop['rfvalue'], check_prop['rsvalue'], check_prop['rsoperator'], msg='RED')
                                    condition += f"else : \r\n\tstatus='error'\r\n"
                                    tmp = obj+"." + \
                                        f"fn_customQualityCheck(sql_query='''{check_prop['parameter']}''',column='{column}',custom_check_name='{check_prop['clable']}',src_query='''{check_prop['basequery']}''',rgb_condition='''{condition}''',criticality='{check_prop['creticality']}',save_error_log='{check_prop['save_error_log']}',dmail='{check_prop['dmail']}',bmail='{check_prop['bmail']}',error_log_query='''{check_prop['error_log_query']}''')"
                                    print(x,column,check_prop['clable'])
                                    value=f"{source}-{layer}"
                                    to_list_dict[check_prop['bmail']]=value

                        if 'Consistency' in checktype:
                            pass
                            # if check_prop['active_flag']=='y':
                            #     column=column.split(':')[0].strip()
                            #     column= column if column=='*' else '`'+column+'`'
                            #     filter_column=check_prop['filter_column'] if check_prop['filter_column']=='*' else '`'+check_prop['filter_column']+'`'
                            #     column=filter_column if filter_column=='*' else column
                            #     if check_prop['check_name'].endswith('table_level'):
                            #         if '_count_' in check_prop['check_name']:
                            #             tmp=obj+"."+f"fn_checkCount(check_type='consistency')"
                            #         elif '_trend_' in check_prop['check_name']:
                            #             condition=fn_get_rgbcondition(check_prop['pfoperator'],check_prop['pfvalue'],check_prop['psvalue'],check_prop['psoperator'],msg='GREEN')
                            #             condition+=fn_get_rgbcondition(check_prop['yfoperator'],check_prop['yfvalue'],check_prop['ysvalue'],check_prop['ysoperator'],msg='YELLOW')
                            #             condition+=fn_get_rgbcondition(check_prop['rfoperator'],check_prop['rfvalue'],check_prop['rsvalue'],check_prop['rsoperator'],msg='RED')
                            #             filtercondition=f"select {check_prop['agg_parameter']}({filter_column}) from ? "
                            #             tmp=obj+"."+f"fn_checkValueTrend(column='{column}',filtercondition='''\r\n{filtercondition}\r\n''',rgb_condition='{condition}',window_parameter='{check_prop['window_parameter']}',check_type='consistency',criticality='{check_prop['creticality']}')"
                            #         elif '_average_' in check_prop['check_name']:
                            #             tmp=''
                            #         elif '_outlier_' in check_prop['check_name']:
                            #             tmp=''
                            #     elif check_prop['check_name'].endswith('column_level'):
                            #         if '_count_' in check_prop['check_name']:
                            #             tmp=f"fn_checkCount(column='{column}',condition='{check_prop['condition']}',check_type='consistency')"
                            #         elif '_trend_' in check_prop['check_name']:
                            #             filtercondition=f"select {check_prop['agg_parameter']}({filter_column}) from ? "
                            #             tmp=f"fn_checkValueTrend(column='{column}',filtercondition='''\r\n{filtercondition}\r\n''',rgb_condition='{condition}',window_parameter='{check_prop['window_parameter']}',check_type='consistency',criticality='{check_prop['creticality']}')"
                            #         elif '_average_' in check_prop['check_name']:
                            #             tmp=f"fn_checkAverage(column='{column}',condition='{check_prop['condition']}',check_type='consistency')"
                            # elif '_outlier_' in check_prop['check_name']:
                            #     tmp=f"fn_checkCount(column='{column}',condition='{check_prop['condition']}',check_type='consistency')"

                        if (tmp != ''):
                            if x in list_4:

                                old = list_4[x]
                                old.append(tmp)
                                list_4[x] = old
                            else:
                                list_4[x] = [str(tmp)]
    #print(list_4)

            #list_4['buisness_mail']=f'"{obj}.get_error_list()"'
        def run(x):
            exec(x)


        list_6 = []
        for key, val in list_4.items():
        

            for v in list(set(val)):
                    run(v)
            # pool=ThreadPool(mp.cpu_count())
            # pool.map(run,val)

        self.table_info=QC_table+'_'+run_table_id,QC_table
        
        def fn_send_dq_alert(to_list:str,subject:str,mail_body:str):
        
    #print("This is alert method",type(self.alert_on),self.alert_on)
          if alert_on.lower()=='y' and mail_body!='':
            
            mailobj={"mail_body": mail_body,"mail_list": to_list,"mail_subject": subject}
            mailobj=eval(str(mailobj))
            #url=f'https://prod-65.westeurope.logic.azure.com:443/workflows/cd07722c6ef84aea99fd0aedba50a24e/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=XmhNmXXri-aCVj9Clcon3qTZNkci0mfsTg3mFuAQH-0'
            url=config_file
            x = requests.post(url, json = mailobj, headers={'Content-Type':'application/json'})

        err_list=self.read_error_data(spark,QC_table,run_table_id,batch_id,run_id)    
        if err_list is not None:
            
            mail_body,mail_list,subject=self.fn_generate_buisness_mail(err_list,to_list_dict)
            fn_send_dq_alert(mail_list,subject,mail_body)
        
        #dashboard_path=QC_table+'_'+"dashboard"
        json_var=self.fn_create_json(spark,QC_table,run_table_id)
        html_var=self.fn_create_dashboard_html(json_var)

        self.fn_write_html_to_table(spark,html_var,product)


        
        
        
        
        
    
       
    def __str__(self):
        return str(self.table_info)
        #print(list_6)