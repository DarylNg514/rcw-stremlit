import streamlit as st
import snowflake.connector as sc
import pandas as pd
from methode import connection,gestion_datawarehouses,gestion_bases_de_donnees,gestion_schemas,gestion_tables,crud,virsualisation

def main():
    auth_done = st.session_state.get('auth_done', False)
    st.title('Dashboard Snowflake') 
    if not auth_done:
        st.subheader('Connexion à Snowflake')
        user = st.text_input('Nom d\'utilisateur')
        password = st.text_input('Mot de passe', type='password')
        account = st.text_input('Compte')

        if st.button('Connexion'):
            con = connection(user, password, account)
            if con:
                st.success('Connecté à Snowflake!')
                st.session_state['auth_done'] = True
                st.session_state['con'] = con  # Stocker la connexion dans la session state
                st.experimental_rerun()
            else:
                st.error("Vos informations de connexion à Snowflake sont incorrectes")
        return

    con = st.session_state['con']  # Récupérer la connexion depuis la session state

    st.write("## Veuillez choisir une option sur le side bar")
    gestion=st.sidebar.radio("",{"Gestion des Datawarehouses","Gestion des Bases de Données","Gestion des Schémas","Gestion des Tables","CRUD (Créer, Lire, Mettre à jour, Supprimer)","virsualisation"},None)

    if gestion=="Gestion des Datawarehouses":
        # Gestion des Datawarehouses
        gestion_datawarehouses(con)
    elif gestion=="Gestion des Bases de Données":
        # Gestion des Bases de Données
        gestion_bases_de_donnees(con)
    elif gestion=="Gestion des Schémas":
        # Gestion des Schémas
        gestion_schemas(con)
    elif gestion=="Gestion des Tables":
        # Gestion des Tables
        gestion_tables(con)
    elif gestion=="CRUD (Créer, Lire, Mettre à jour, Supprimer)":
        # CRUD
        crud(con)
    elif gestion=="virsualisation":
        # CRUD
        virsualisation(con)



if __name__ == '__main__':
    main()