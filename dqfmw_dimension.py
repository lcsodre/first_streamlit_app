import streamlit
import pandas as pd
import requests
import snowflake.connector
from urllib.error import URLError

#######################################Functions##############################
def get_dim_load_list():
  my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
  with my_cnx.cursor() as my_cur:
    my_cur.execute("select DIMENSION_ID|| '|' || DIMENSION_NAME from DMDQFMRWK.METADATA.DIMENSIONS")
    f_return=my_cur.fetchall() 
    my_cnx.close()
    df = pd.DataFrame(f_return,columns=['Name'])
    return df 
 
def insert_row_snowflake(new_dim):
  my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
  with my_cnx.cursor() as my_cur:
    my_cur.execute("insert into DMDQFMRWK.METADATA.DIMENSIONS values(DEFAULT,'" + new_dim + "')")
    my_cnx.close()
    return 'The dimension was added ' + new_dim

def delete_row_snowflake(p_dim_id):
  my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
  with my_cnx.cursor() as my_cur:
    my_cur.execute("DELETE FROM DMDQFMRWK.METADATA.DIMENSIONS WHERE DIMENSION_ID = " + p_dim_id )
    my_cnx.close()
    return 'The dimension was removed ID=' + p_dim_id
##############################################################################

streamlit.title('DQ - Dimension')

streamlit.sidebar.title('Menu')
page_domains = streamlit.sidebar.selectbox('Subject',['Domains','Responsible','Dimensions','Structures','Rules'])

with streamlit.form(key="dim_ins"):
  input_name= streamlit.text_input(label="Dimension Name")  
  input_button = streamlit.form_submit_button('Add Dimension')

  #Button to insert into Snowflake
  if input_button:
    message_insert = insert_row_snowflake(input_name)
    streamlit.text(message_insert)
  
  my_data_rows = get_dim_load_list()
  p_dimension = streamlit.selectbox('Dimensions',my_data_rows)

  #Retrieve ID
  p_dim_split_id = p_dimension.split('|')
  p_dim_id = p_dim_split_id[0]
  #Retrive Name
  p_dim = p_dim_split_id[1]
  
  exclude_button = streamlit.form_submit_button('Delete Dimension')
  
  #Button to insert into Snowflake
  if exclude_button:
    message_delete = delete_row_snowflake(p_dim_id)
    streamlit.text(message_delete)
  
  my_data_rows = get_dim_load_list()
  df = pd.DataFrame(my_data_rows,columns=['Name'])
  streamlit.table(df)
