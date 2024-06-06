import streamlit as st
import snowflake.connector as sc
import pandas as pd
import matplotlib.pyplot as plt
def connection(user, password, account):
    con = sc.connect(
    user=user,
    password=password,
    account=account
    )

    return con
def gestion_datawarehouses(con):
    st.subheader('Gestion des Datawarehouses')
    if st.button('Lister les Datawarehouses'):
        cur = con.cursor()
        cur.execute('SHOW WAREHOUSES')
        df = cur.fetchall()
        st.dataframe(df)

    warehouse_name = st.text_input('Nom du Datawarehouse pour création')
    if st.button('Créer un nouveau Datawarehouse'):
        cur = con.cursor()
        cur.execute(f'CREATE WAREHOUSE IF NOT EXISTS {warehouse_name}')
        st.success(f'Datawarehouse {warehouse_name} créé avec succès!')

def gestion_bases_de_donnees(con):
    st.subheader('Gestion des Bases de Données')
    if st.button('Lister les Bases de Données'):
        cur = con.cursor()
        cur.execute('SHOW DATABASES')
        df = cur.fetchall()
        st.dataframe(df)

    db_name = st.text_input('Nom de la Base de Données pour création')
    if st.button('Créer une nouvelle Base de Données'):
        cur = con.cursor()
        cur.execute(f'CREATE DATABASE IF NOT EXISTS {db_name}')
        st.success(f'Base de Données {db_name} créée avec succès!')

def gestion_schemas(con):
    st.subheader('Gestion des Schémas')
    selected_db = st.text_input('Base de Données pour lister les Schémas',key=1)
    if st.button('Lister les Schémas'):
        cur = con.cursor()
        cur.execute(f'USE DATABASE {selected_db}')
        cur.execute('SHOW SCHEMAS')
        df = cur.fetchall()
        st.dataframe(df)

    schema_name = st.text_input('Nom du Schéma pour création')
    if st.button('Créer un Nouveau Schéma'):
        cur = con.cursor()
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS {selected_db}.{schema_name}')
        st.success(f'Schéma {schema_name} créé avec succès dans la base de données {selected_db}!')

def gestion_tables(con):
    st.subheader('Gestion des Tables')
    selected_db = st.text_input('Base de Données pour lister les Schémas',key=2)
    selected_schema = st.text_input('Schéma pour lister les Tables')
    if st.button('Lister les Tables'):
        cur = con.cursor()
        cur.execute(f'USE SCHEMA {selected_db}.{selected_schema}')
        cur.execute('SHOW TABLES')
        df = cur.fetchall()
        st.dataframe(df)

    table_name = st.text_input('Nom de la Table pour création')
    columns = st.text_input('Colonnes pour la nouvelle table (format: nom TYPE, nom TYPE)')
    if st.button('Créer une Nouvelle Table'):
        cur = con.cursor()
        cur.execute(f'CREATE TABLE IF NOT EXISTS {table_name} ({columns})')
        st.success(f'Table {table_name} créée avec succès dans le schéma {selected_schema}!')

def crud(con):
    cur = con.cursor()

    # Récupération des options pour les Datawarehouses
    cur.execute('SHOW WAREHOUSES')
    df = cur.fetchall()
    warehouses = [row[0] for row in df]
    warehouse = st.selectbox('Choisir un Datawarehouse', warehouses,None)

    # Récupération des options pour les Bases de Données
    cur.execute('SHOW DATABASES')
    df = cur.fetchall()
    databases = [row[1] for row in df]
    database = st.selectbox('Choisir une Base de Données', databases,None)

    # Configuration du contexte Snowflake pour le datawarehouse et la base de données
    if warehouse is not None:
        cur.execute(f'USE WAREHOUSE {warehouse}')
    if database is not None:
        cur.execute(f'USE DATABASE {database}')

    # Récupération des options pour les Schémas
    cur.execute('SHOW SCHEMAS')
    df = cur.fetchall()
    schemas = [row[1] for row in df]
    schema = st.selectbox('Choisir un Schéma', schemas,None)

    # Configuration du contexte Snowflake pour le schéma
    if schema is not None:
        cur.execute(f'USE SCHEMA {schema}')

    # Récupération des options pour les Tables
    cur.execute('SHOW TABLES')
    df = cur.fetchall()
    tables = [row[1] for row in df]
    table = st.selectbox('Choisir une Table', tables,None)

    # Interface pour les opérations CRUD
    if table is not None:
    
        st.subheader(f'Opérations CRUD sur la table {table}')
        operation = st.selectbox('Choisir l\'opération', ['Créer', 'Lire', 'Mettre à jour', 'Supprimer'],None)

        if operation == 'Créer':
            create_query = st.text_area('SQL pour créer une entrée', f'INSERT INTO {table} VALUES (value1, value2)')
            if st.button('Exécuter Création'):
                cur.execute(create_query)
                st.success('Entrée créée avec succès!')

        elif operation == 'Lire':
            read_query = st.text_area('SQL pour lire des données', f'SELECT * FROM {table}')
            if st.button('Exécuter Lecture'):
                cur.execute(read_query)
                df = cur.fetch_pandas_all()
                st.dataframe(df)

        elif operation == 'Mettre à jour':
            update_query = st.text_area('SQL pour mettre à jour une entrée', f'UPDATE {table} SET column1 = value1 WHERE condition')
            if st.button('Exécuter Mise à jour'):
                cur.execute(update_query)
                st.success('Entrée mise à jour avec succès!')

        elif operation == 'Supprimer':
            delete_query = st.text_area('SQL pour supprimer une entrée', f'DELETE FROM {table} WHERE condition')
            if st.button('Exécuter Suppression'):
                cur.execute(delete_query)
                st.success('Entrée supprimée avec succès!')

def virsualisation(con):
    cur = con.cursor()
    # Obtention des données pour les visualisations
    st.subheader('Visualisation des Données')

    # Requête pour le nombre de datawarehouses
    cur.execute('SHOW WAREHOUSES')
    df_warehouses = cur.fetchall()
    warehouses = [row[0] for row in df_warehouses]
    warehouse_count = len(warehouses)

    # Requête pour le nombre de bases de données
    cur.execute('SHOW DATABASES')
    df_databases = cur.fetchall()
    databases = [row[1] for row in df_databases]
    database_count = len(databases)

    # Requête pour le nombre de schémas
    cur.execute('SHOW SCHEMAS')
    df_schemas = cur.fetchall()
    schemas = [row[1] for row in df_schemas]
    schema_count = len(schemas)

    # Création d'un DataFrame pour le graphique
    data = {
        'Catégorie': ['Datawarehouses', 'Bases de données', 'Schémas'],
        'Nombre': [warehouse_count, database_count, schema_count]
    }
    df_graph = pd.DataFrame(data)

    # Création du graphique
    fig, ax = plt.subplots()
    ax.bar(df_graph['Catégorie'], df_graph['Nombre'], color=['blue', 'green', 'red'])
    ax.set_xlabel('Catégorie')
    ax.set_ylabel('Nombre')
    ax.set_title('Nombre de Datawarehouses, Bases de Données, et Schémas')
    st.pyplot(fig)