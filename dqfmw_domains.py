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
    my_cur.execute("insert into pc_rivery_db.public.fruit_load_list values(DEFAULT,'" + new_domain + "')")
    my_cnx.close()
    return 'The domain was added ' + new_domain
##############################################################################


#Button to retrieve from Snowflake
if streamlit.button('Get Domain´s List'):
  my_data_rows = get_domain_load_list()
  streamlit.dataframe(my_data_rows)

add_domain = streamlit.text_input('Enter a New Domain Name')
#Button to insert into Snowflake
if streamlit.button('Add Domain to The List'):
  message_insert = insert_row_snowflake(add_domain)
  streamlit.text(message_insert)
