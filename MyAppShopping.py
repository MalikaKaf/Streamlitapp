import streamlit as st
import duckdb
import pandas as pd
#import plotly.express as px
#import plotly.graph_objects as go
import tempfile
import os


# Configuration de la page
st.set_page_config(page_title="Analyse des données client avec DuckDB", layout="wide")

# Titre de l'application
st.title("Analyse des données client avec StreamLit")
st.write("Cette application analyse les habitudes d achat client.")


# Fonction pour charger les données de démonstration du Titanic
def charger_donnees_titanic_demo():
    # URL des données Titanic de démonstration
    url = "https://github.com/atifrani/mgt_opl_env_dev/blob/main/data/titanic.csv?raw=true"
    return pd.read_csv(url)

# Sidebar pour le chargement des données
st.sidebar.title("Source de données")
source_option = st.sidebar.radio(
    "Choisir la source de données:",
    ["Données client", "Télécharger un fichier CSV"]
)



