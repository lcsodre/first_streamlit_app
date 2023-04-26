import streamlit
import pandas
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
 
def insert_row_snoeflake(new_fruit):
  my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
  with my_cnx.cursor() as my_cur:
    my_cur.execute("insert into pc_rivery_db.public.fruit_load_list values('" + new_fruit + "')")
    my_cnx.close()
    return 'Thanks for adding ' + new_fruit
##############################################################################
my_data_rows = get_structure_list()

page_domains = streamlit.sidebar.selectbox('Domains',['Create','Update','Delete','Read'])

# Let's put a pick list here so they can pick the fruit they want to include 
fruits_selected = streamlit.multiselect("Select the Table:", list(my_data_rows.index))
fruits_to_show = my_fruit_list.loc[fruits_selected]

streamlit.dataframe(fruits_to_show)

streamlit.header("Fruityvice Fruit Advice!")

try:
  fruit_choice = streamlit.text_input('What fruit would you like information about?')
  
  if not fruit_choice:
    streamlit.error("Please select a fruit to get information.")
  else:
    return_of_function=get_fruityvice_data(fruit_choice)
    streamlit.dataframe(return_of_function)

except URLError as e:
  streamlit.error()

streamlit.header("View Our Fruit List - Add Your Favorites!")

#Button to retrieve from Snowflake
if streamlit.button('Get Fruit List'):
  my_data_rows = get_fruit_load_list()
  streamlit.dataframe(my_data_rows)

add_myfruit = streamlit.text_input('What fruit would you like to add?','Jackfruit')
#Button to insert into Snowflake
if streamlit.button('Add Fruit to The List'):
  message_insert = insert_row_snoeflake(add_myfruit)
  streamlit.text(message_insert)
