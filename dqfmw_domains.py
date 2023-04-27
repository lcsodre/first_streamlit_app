import streamlit
import pandas as pd
import requests
import snowflake.connector
from urllib.error import URLError

#######################################Functions##############################
def get_domain_load_list():
  my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
  with my_cnx.cursor() as my_cur:
    my_cur.execute("select DOMAIN_ID|| '|' || DOMAIN_NAME from DMDQFMRWK.METADATA.DOMAINS")
    f_return=my_cur.fetchall() 
    my_cnx.close()
    df = pd.DataFrame(f_return,columns=['Name'])
    return df 
 
def insert_row_snowflake(new_domain):
  my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
  with my_cnx.cursor() as my_cur:
    my_cur.execute("insert into DMDQFMRWK.METADATA.DOMAINS values(DEFAULT,'" + new_domain + "')")
    my_cnx.close()
    return 'The domain was added ' + new_domain

def delete_row_snowflake(p_domain_id):
  my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
  with my_cnx.cursor() as my_cur:
    my_cur.execute("DELETE FROM DMDQFMRWK.METADATA.DOMAINS WHERE DOMAIN_ID = " + p_domain_id + ")")
    my_cnx.close()
    return 'The domain was added ' + new_domain
##############################################################################

streamlit.title('DQ - Domains')

streamlit.sidebar.title('Menu')
page_domains = streamlit.sidebar.selectbox('Subject',['Domains','Responsible','Dimensions','Structures','Rules'])

with streamlit.form(key="domain_ins"):
  input_name= streamlit.text_input(label="Domain Name")  
  input_button = streamlit.form_submit_button('Add Domain')

  #Button to insert into Snowflake
  if input_button:
    message_insert = insert_row_snowflake(input_name)
    streamlit.text(message_insert)
  
  my_data_rows = get_domain_load_list()
  p_domain = streamlit.selectbox('Domains',my_data_rows)

  #Retrieve ID
  p_dom_split_id = p_domain.split('|')
  p_dom_id = p_dom_split_id[0]
  #Retrive Name
  p_dom = p_dom_split_id[1]
  
  exclude_button = streamlit.form_submit_button('Delete Domain')
  
  #Button to insert into Snowflake
  if exclude_button:
    message_delete = delete_row_snowflake(p_dom_id)
    streamlit.text(message_delete)
  
  my_data_rows = get_domain_load_list()
  df = pd.DataFrame(my_data_rows,columns=['Name'])
  streamlit.write(df)


#if page_domains=='Create':
  ##code here
  
#if page_domains=='Read':
  #code here
