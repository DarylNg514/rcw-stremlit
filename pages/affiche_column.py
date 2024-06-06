import streamlit as st
import snowflake.connector as sc
import pandas as pd

def connection():
    try:
        con = sc.connect(
        user="Daryl",
        password="Daryl514",
        account="hyoulpz-bc97192"
        )
        if con is None:
            st.warning("Vos information de connection a snowflake sont incorrect")
    except:
        st.warning("Vos information de connection a snowflake sont incorrect")
    return con

def main():
    try:
        con = sc.connect(
        user="Daryl",
        password="Daryl514",
        account="hyoulpz-bc97192"
        )
        if con is None:
            st.warning("Vos information de connection a snowflake sont incorrect")

        cursor= con.cursor()
        def datapersonnes():
            sql="select * from RCW.PERSONNE.PERSONNNES ;"
            cursor.execute(sql)
            colonne_names=[]
            for column in cursor.description:
                colonne_names.append(column[0])
            data= cursor.fetchall()
            #column_names= [desc[0] for desc in cursor.description]
            df= pd.DataFrame(data=data,columns=colonne_names) 
            return df
        st.write("# Affichage le nom des colonne") 
        st.write(datapersonnes())
    except:
        st.warning("Vos information de connection a snowflake sont incorrect")

if __name__ == '__main__':
    main()