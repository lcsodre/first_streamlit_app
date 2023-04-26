import streamlit
import pandas as pd
import snowflake.connector
import os
import openai

#######################################Functions##############################
def get_structure_list():
  my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
  with my_cnx.cursor() as my_cur:
    my_cur.execute("SELECT STRUCTURE_NAME FROM DMDQFMRWK.METADATA.STRUCTURES")
    f_return=my_cur.fetchall() 
    my_cnx.close()
    df = pd.DataFrame(f_return,columns=['Name'])
    return df   
 
def get_attributes_list(p_catalog,p_schema,p_table):
  my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
  with my_cnx.cursor() as my_cur:
    v_query='SELECT COLUMN_NAME FROM DMDQFMRWK.PROCESSING.TMP_COLS WHERE TABLE_NAME ='+ "'" + p_table + "'" +' AND TABLE_CATALOG = '+ "'" + p_catalog + "'" + ' AND TABLE_SCHEMA = ' + "'" + p_schema + "'"
    my_cur.execute(v_query)
    f_return=my_cur.fetchall() 
    my_cnx.close()                   
    df = pd.DataFrame(f_return,columns=['Name'])
    return df 
##############################################################################
streamlit.header("Rules Definition!")

#Retrieve the Tables
my_data_rows = get_structure_list()
p_structure = streamlit.selectbox('Tables',my_data_rows)

#parses structure
p_structure_split= p_structure.split('.')

p_catalog=p_structure_split[0]
p_schema=p_structure_split[1]
p_table=p_structure_split[2]

p_catalog=str(p_catalog)
p_schema=str(p_schema)
p_table=str(p_table)
                 
#Retrive the Columns
if streamlit.button('Get Columns'):
  my_data_rows = get_attributes_list(p_catalog,p_schema,p_table)
  p_column = streamlit.selectbox('Tables',my_data_rows)


streamlit.text('Busines rule')

#Call API to write the SQL
openai.api_key = os.getenv("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")

response = openai.Completion.create(
  model="text-davinci-003",
  prompt="### Postgres SQL tables, with their properties:\n#\n# Employee(id, name, department_id)\n# Department(id, name, address)\n# Salary_Payments(id, employee_id, amount, date)\n#\n### A query to list the names of the departments which employed more than 10 employees in the last 3 months\nSELECT",
  temperature=0,
  max_tokens=150,
  top_p=1.0,
  frequency_penalty=0.0,
  presence_penalty=0.0,
  stop=["#", ";"]
)

streamlit.text('SQL rule',response)


  
 
