import streamlit
import pandas as pd
import requests
import snowflake.connector
from urllib.error import URLError

#######################################Functions##############################
def get_domain_load_list():
  my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
  with my_cnx.cursor() as my_cur:
    my_cur.execute("select DOMAIN_ID,DOMAIN_NAME from DMDQFMRWK.METADATA.DOMAINS")
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

streamlit.title('DQ - Domains')

streamlit.sidebar.title('Menu')
page_domains = streamlit.sidebar.selectbox('Domains',['Create','Update','Delete','Read'])

if page_domains=='Create':

  with streamlit.form(key="domain_ins"):
    input_name= streamlit.text_input(label="Domain Name")  
    input_button = streamlit.form_submit_button('Add Domain')

  #Button to insert into Snowflake
  if input_button:
    message_insert = insert_row_snowflake(input_name)
    streamlit.text(message_insert)

if page_domains=='Read':
  cols = streamlit.columns((1,2,1))
  fields=['ID','Name','Delete']
  
  for col,field_name in zip(cols,fields):
    col.write(field_name)
  
  my_data_rows = get_domain_load_list()
  
  streamlit.write(my_data_rows)
  
  for item in my_data_rows:
    col1,col2,col3 = streamlit.columns((1,2,1))
    col1.write(item.DOMAIN_ID)
    col2.write(item.DOMAIN_NAME)
    button_space = col3.empty()
    on_click = button_space.button('Delete','btn_delete' + str(item.DOMAIN_ID))
    
  #my_data_rows = get_domain_load_list()
  #df = pd.DataFrame(my_data_rows,columns=['Id','Name'])
  #streamlit.table(df)
 
