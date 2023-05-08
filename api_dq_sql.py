import streamlit
import pandas as pd
import snowflake.connector
import os
import openai
import json

#######################################Functions##############################
def get_structure_list():
  my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
  with my_cnx.cursor() as my_cur:
    my_cur.execute("SELECT STRUCTURE_ID || '|' || STRUCTURE_NAME FROM DMDQFMRWK.METADATA.STRUCTURES")
    f_return=my_cur.fetchall() 
    my_cnx.close()
    df = pd.DataFrame(f_return,columns=['Name'])
    return df   
 
def get_attributes_list(p_catalog,p_schema,p_table):
  my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
  with my_cnx.cursor() as my_cur:
    v_query='SELECT COLUMN_NAME FROM '+p_catalog+'.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME ='+ "'" + p_table + "'" +' AND TABLE_CATALOG = '+ "'" + p_catalog + "'" + ' AND TABLE_SCHEMA = ' + "'" + p_schema + "'"
    my_cur.execute(v_query)
    f_return=my_cur.fetchall() 
    my_cnx.close()                   
    df = pd.DataFrame(f_return,columns=['Name'])
    return df 
  
def get_dimensions_list():
  my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
  with my_cnx.cursor() as my_cur:
    v_query="SELECT DIMENSION_ID || '|' || DIMENSION_NAME FROM DMDQFMRWK.METADATA.DIMENSIONS"
    my_cur.execute(v_query)
    f_return=my_cur.fetchall() 
    my_cnx.close()                   
    df = pd.DataFrame(f_return,columns=['Name'])
    return df 

def insert_rule(p_dimension_id,p_structure_id,p_attribute_name,p_rule_name,p_busines_rule,p_tech_rule):
  my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
  with my_cnx.cursor() as my_cur:
    p_tech_rule=p_tech_rule.strip()
    p_tech_rule = p_tech_rule.replace("'","''")
    v_query="insert into DMDQFMRWK.METADATA.RULES values(DEFAULT," + p_dimension_id + "," + p_structure_id + ",'" + p_attribute_name + "','" + p_rule_name + "'," + '"' + p_busines_rule + '",' + '"' + p_tech_rule + '")'
    my_cur.execute(v_query)
    my_cnx.close()
    return 'The Rule was added ' + p_tech_rule
  
def call_openai(b_rule):
  
  #Call API to write the SQL
  openai.api_key = streamlit.secrets['pass']

  response = openai.Completion.create(
    model="text-davinci-003",
    prompt=b_rule,
    temperature=0,
    max_tokens=150,
    top_p=1.0,
    frequency_penalty=0.0,
    presence_penalty=0.0,
    stop=["#", ";"]
  )
  
  y = json.loads(str(response))
  p_technical_rule=str(y["choices"][0]["text"])
  return p_technical_rule
##############################################################################
streamlit.header("Rules Definition!")

#Retrieve the Tables
my_data_rows = get_structure_list()
p_structure = streamlit.selectbox('Tables',my_data_rows)
#Retrieve ID
p_structure_split_id = p_structure.split('|')
p_structure_id = p_structure_split_id[0]
#Retrive Name
p_structure = p_structure_split_id[1]

#parses structure
p_structure_split= p_structure.split('.')

p_catalog=p_structure_split[0]
p_schema=p_structure_split[1]
p_table=p_structure_split[2]

p_catalog=str(p_catalog)
p_schema=str(p_schema)
p_table=str(p_table)
                 
my_data_rows = get_attributes_list(p_catalog,p_schema,p_table)
p_column = streamlit.selectbox('Columns',my_data_rows)

my_data_rows = get_dimensions_list()
p_dim = streamlit.selectbox('Dimensions',my_data_rows)

#Retrieve ID
p_dim_split_id = p_dim.split('|')
p_dim_id = p_dim_split_id[0]
#Retrive Name
p_dim = p_dim_split_id[1]

if p_dim=='COMPLETENESS':
  p_rule_dim = 'Select quantity of records where is null'

if p_dim=='ACCURACY':
  p_rule_dim = 'Select quantity of records where value is less than <value_min> or value is greater than <value_max>'

if p_dim=='CONSISTENCY':
  p_rule_dim = 'Select quantity of records where value format is different of <AAAA 999-999-99>'
  
if p_dim=='VALIDITY':
  p_rule_dim = 'Select quantity of records where value is not in (1,3,5,7)'

if p_dim=='UNIQUENESS':
  p_rule_dim = 'Select the quantity  where the value exists in more than 1 record'

if p_dim=='INTEGRITY':
  ###############Retrieve the Refference Tables
  my_data_rows = get_structure_list()
  p_structure2 = streamlit.selectbox('Refference Tables',my_data_rows)
  
  #Retrieve ID
  p_structure_split_id2 = p_structure2.split('|')
  p_structure_id2 = p_structure_split_id2[0]
  #Retrive Name
  p_structure2 = p_structure_split_id2[1]

  #parses structure
  p_structure_split2= p_structure2.split('.')

  p_catalog2=p_structure_split2[0]
  p_schema2=p_structure_split2[1]
  p_table2=p_structure_split2[2]

  p_catalog2=str(p_catalog2)
  p_schema2=str(p_schema2)
  p_table2=str(p_table2)

  my_data_rows = get_attributes_list(p_catalog2,p_schema2,p_table2)
  p_column2 = streamlit.selectbox('Refference Columns',my_data_rows)
  ########################################################
  p_rule_dim = 'Select quantity of records where value not exists on table ' + p_structure2 + '(' +p_column2 +')'


b_rule = streamlit.text_area('Busines rule', value='#Snowflake \n'+p_structure+'('+p_column+') \n' + p_rule_dim,height=300)

if streamlit.button('Preview SQL'):
  #Call API to write the SQL
  p_technical_rule=call_openai(b_rule)
  streamlit.write(p_technical_rule)

if streamlit.button('Add Rule'):
  #Call API to write the SQL
  p_technical_rule=call_openai(b_rule)
  message_insert=insert_rule(p_dim_id,p_structure_id,p_column,'RULE_XXX',b_rule,p_technical_rule)  
  streamlit.text(message_insert)
 
