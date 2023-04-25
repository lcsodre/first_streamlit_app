import streamlit
import pandas
import requests
import snowflake.connector
from urllib.error import URLError

streamlit.title('Data Quality Framework - Business Domains')

#######################################Functions##############################
def get_domain_load_list():
  my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
  with my_cnx.cursor() as my_cur:
    my_cur.execute("select * from DMDQFMRWK.METADATA.DOMAINS")
    f_return=my_cur.fetchall() 
    my_cnx.close()
    return f_return 
 
def insert_row_snowflake(new_domain):
  my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
  with my_cnx.cursor() as my_cur:
    my_cur.execute("insert into DMDQFMRWK.METADATA.DOMAINS values(DEFAULT,'" + new_domain + "')")
    my_cnx.close()
    return 'The domain was added ' + new_domain
##############################################################################

with streamlit.form(key="domain"):
  input_name= streamlit.text_input(label="Domain Name")  
  input_button = streamlit.form_submit_button('Add Domain')
  
#Button to insert into Snowflake
if input_button:
  message_insert = insert_row_snowflake(input_name)
  streamlit.text(message_insert)

#Button to retrieve from Snowflake
retrieve_button = streamlit.form_button('Refresh Domain´s List')
if retrieve_button:
  my_data_rows = get_domain_load_list()
  streamlit.dataframe(my_data_rows)
