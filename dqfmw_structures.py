import streamlit
import pandas as pd
import requests
import snowflake.connector
from urllib.error import URLError

#######################################Functions##############################
def get_databases_load_list():
  my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
  with my_cnx.cursor() as my_cur:
    my_cur.execute("select DATABASE_NAME FROM SNOWFLAKE.INFORMATION_SCHEMA.DATABASES")
    f_return=my_cur.fetchall() 
    my_cnx.close()
    df = pd.DataFrame(f_return,columns=['Name'])
    return df 

def get_tables_load_list(p_catalog):
  my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
  with my_cnx.cursor() as my_cur:
    v_query="SELECT TABLE_SCHEMA || '.' || TABLE_NAME AS NAME FROM " +p_catalog+ ".INFORMATION_SCHEMA.TABLES"
    my_cur.execute(v_query)
    f_return=my_cur.fetchall() 
    my_cnx.close()
    df = pd.DataFrame(f_return,columns=['Name'])
    return df

def get_domains_load_list():
  my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
  with my_cnx.cursor() as my_cur:
    v_query='SELECT DOMAIN_ID || '|' || DOMAIN_NAME FROM DMDQFMRWK.METADATA.DOMAINS'
    my_cur.execute(v_query)
    f_return=my_cur.fetchall() 
    my_cnx.close()
    df = pd.DataFrame(f_return,columns=['Name'])
    return df
  
def get_responsibles_load_list():
  my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
  with my_cnx.cursor() as my_cur:
    v_query="SELECT RESPONSIBLE_ID || '|' || RESPONSIBLE_NAME FROM DMDQFMRWK.METADATA.RESPONSIBLES"
    my_cur.execute(v_query)
    f_return=my_cur.fetchall() 
    my_cnx.close()
    df = pd.DataFrame(f_return,columns=['Name'])
    return df
  
def insert_row_snowflake(p_custodian_id,p_steward_id,p_owner_id,p_domain_id,p_structure_name,p_structure_desc):
  my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
  with my_cnx.cursor() as my_cur:
    my_cur.execute("insert into DMDQFMRWK.METADATA.STRUCTURES values(DEFAULT," + p_custodian_id + "," +p_steward_id + "," + p_owner_id + "," + p_domain_id + ",'" + p_structure_name + "','" + p_structure_desc + "')")
    my_cnx.close()
    return 'The structure was added ' + p_structure_name

def get_structures_load_list():
  my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
  with my_cnx.cursor() as my_cur:
    v_query="SELECT STRUCTURE_ID || '|' || STRUCTURE_NAME FROM DMDQFMRWK.METADATA.STRUCTURES"
    my_cur.execute(v_query)
    f_return=my_cur.fetchall() 
    my_cnx.close()
    df = pd.DataFrame(f_return,columns=['Name'])
    return df
##############################################################################

streamlit.title('DQ - Structures')

streamlit.sidebar.title('Menu')
page_domains = streamlit.sidebar.selectbox('Subject',['Domains','Responsible','Dimensions','Structures','Rules'])

with streamlit.form(key="struct_ins"):
  my_data_rows = get_databases_load_list()
  p_database = streamlit.selectbox('Database',my_data_rows)
  
  my_data_rows = get_tables_load_list(p_database)
  p_table = streamlit.selectbox('Table',my_data_rows)
  
  input_button = streamlit.form_submit_button('Add Structure')
  
  #Button to insert into Snowflake
  if input_button:
    message_insert = insert_row_snowflake(p_cust_id,p_stew_id,p_owner_id,p_dom_id,p_structure,p_structure_desc)
    streamlit.text(message_insert)
 
