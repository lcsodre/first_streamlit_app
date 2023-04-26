import streamlit
import pandas as pd
import requests
import snowflake.connector
from urllib.error import URLError

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

streamlit.stop()

#Button to retrieve from Snowflake
if streamlit.button('Get Fruit List'):
  my_data_rows = get_fruit_load_list()
  streamlit.dataframe(my_data_rows)

add_myfruit = streamlit.text_input('What fruit would you like to add?','Jackfruit')
#Button to insert into Snowflake
if streamlit.button('Add Fruit to The List'):
  message_insert = insert_row_snoeflake(add_myfruit)
  streamlit.text(message_insert)
