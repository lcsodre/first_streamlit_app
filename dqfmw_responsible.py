import streamlit
import pandas as pd
import requests
import snowflake.connector
from urllib.error import URLError

#######################################Functions##############################
def get_responsible_load_list():
  my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
  with my_cnx.cursor() as my_cur:
    my_cur.execute("select RESPONSIBLE_ID|| '|' || RESPONSIBLE_NAME from DMDQFMRWK.METADATA.RESPONSIBLES")
    f_return=my_cur.fetchall() 
    my_cnx.close()
    df = pd.DataFrame(f_return,columns=['Name'])
    return df 
 
def insert_row_snowflake(new_resp):
  my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
  with my_cnx.cursor() as my_cur:
    my_cur.execute("insert into DMDQFMRWK.METADATA.RESPONSIBLES values(DEFAULT,'" + new_resp + "')")
    my_cnx.close()
    return 'The responsible was added ' + new_dim

def delete_row_snowflake(p_resp_id):
  my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
  with my_cnx.cursor() as my_cur:
    my_cur.execute("DELETE FROM DMDQFMRWK.METADATA.RESPONSIBLES WHERE RESPONSIBLE_ID = " + p_resp_id )
    my_cnx.close()
    return 'The responsible was removed ID=' + p_resp_id
##############################################################################

streamlit.title('DQ - Responsible')

streamlit.sidebar.title('Menu')
page_domains = streamlit.sidebar.selectbox('Subject',['Domains','Responsible','Dimensions','Structures','Rules'])

with streamlit.form(key="resp_ins"):
  input_name= streamlit.text_input(label="Responsible Name")  
  input_button = streamlit.form_submit_button('Add Responsible')

  #Button to insert into Snowflake
  if input_button:
    message_insert = insert_row_snowflake(input_name)
    streamlit.text(message_insert)
  
  my_data_rows = get_responsible_load_list()
  p_responsible = streamlit.selectbox('Responsibles',my_data_rows)

  #Retrieve ID
  p_resp_split_id = p_responsible.split('|')
  p_resp_id = p_resp_split_id[0]
  #Retrive Name
  p_resp = p_resp_split_id[1]
  
  exclude_button = streamlit.form_submit_button('Delete Responsible')
  
  #Button to insert into Snowflake
  if exclude_button:
    message_delete = delete_row_snowflake(p_resp_id)
    streamlit.text(message_delete)
  
  my_data_rows = get_responsible_load_list()
  df = pd.DataFrame(my_data_rows,columns=['Name'])
  streamlit.table(df)
