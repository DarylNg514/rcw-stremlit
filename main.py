import streamlit as st
import snowflake.connector as sc
import pandas as pd

con = sc.connect(
user="Daryl",
password="Daryl514",
account="hyoulpz-bc97192"
)
cursor= con.cursor()

def main():
    st.write("# Welcome to Snowflake demo") 
    res=con.cursor().execute("select * from RCW.PERSONNE.PERSONNNES ;").fetchall()
    st.write(f'Value of table in snowflake is : {res}')
    # ou 
    st.write(personnes())
    
def personnes():
    sql="select * from RCW.PERSONNE.PERSONNNES ;"
    df= cursor.execute(sql).fetchall()
    return pd.DataFrame(df) 
if __name__ == '__main__':
    main()