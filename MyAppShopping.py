import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import tempfile
import os


# Configuration de la page
st.set_page_config(page_title="Analyse des données client avec DuckDB", layout="wide")

# Titre de l'application
st.title("Analyse des données client avec StreamLit")
st.write("Cette application analyse les habitudes d achat client.")


# Fonction pour charger les données de démonstration du shopping
def charger_donnees_shopping_demo():
    # URL des données shopping de démonstration
    url = "https://github.com/MalikaKaf/Streamlitapp/blob/main/Data/shopping_behavior_updated.csv?raw=true"
    return pd.read_csv(url)

# Sidebar pour le chargement des données
st.sidebar.title("Source de données")
source_option = st.sidebar.radio(
    "Choisir la source de données:",
    ["Données client", "Télécharger un fichier CSV"]
)

# Initialiser la connexion DuckDB
conn = duckdb.connect(database=':memory:', read_only=False)

# Obtenir les données
if source_option == "Données client":
    df = charger_donnees_shopping_demo()
    st.sidebar.success("Données shopping de démonstration chargées!")
    
    # Enregistrer les données dans DuckDB
    conn.execute("CREATE TABLE IF NOT EXISTS shopping AS SELECT * FROM df")
    
else:
    uploaded_file = st.sidebar.file_uploader("Télécharger un fichier CSV", type=["csv"])
    if uploaded_file is not None:
        # Sauvegarder temporairement le fichier
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            tmp_path = tmp_file.name
        
        # Créer une table à partir du CSV avec DuckDB
        conn.execute(f"CREATE TABLE IF NOT EXISTS shopping AS SELECT * FROM read_csv_auto('{tmp_path}')")
        
        # Charger les données pour affichage
        df = conn.execute("SELECT * FROM Shopping").fetchdf()
        st.sidebar.success(f"{len(df)} Shopping Chargé!")
        
        # Supprimer le fichier temporaire
        os.unlink(tmp_path)
    else:
        st.info("Veuillez télécharger un fichier CSV ou utiliser les données de démonstration.")
        st.stop()

# Afficher un aperçu des données
st.subheader("Aperçu des données")
st.dataframe(df.head(10))

# Statistiques générales
st.header("Statistiques générales")

# Utiliser DuckDB pour les statistiques de survie
stats_generales = conn.execute("""
    SELECT 
        COUNT(*) as total_client,
        count(distinct Location) as ville,
        avg(Age) as moyenne_age
    FROM shopping
""").fetchdf()

col1, col2, col3 = st.columns(3)
col1.metric("Total Client", stats_generales['total_client'][0])
col2.metric("Ville", stats_generales['ville'][0])
col3.metric("Moyenne Age", f"{stats_generales['moyenne_age'][0]}%")

# Créer les deux graphiques demandés
st.header("Analyse des clients")

# 1. Graphique du nombre de survivants par sexe
client_par_sexe = conn.execute("""
    SELECT 
        Gender,
        count(*) Total   
    FROM shopping
    GROUP BY Gender
    ORDER BY Gender
""").fetchdf()

# 2. Graphique du nombre de clients par âge
# D'abord, créer des groupes d'âge pour une meilleure visualisation
client_par_age = conn.execute("""
    SELECT 
        CASE 
            WHEN Age < 10 THEN '0-9'
            WHEN Age < 20 THEN '10-19'
            WHEN Age < 30 THEN '20-29'
            WHEN Age < 40 THEN '30-39'
            WHEN Age < 50 THEN '40-49'
            WHEN Age < 60 THEN '50-59'
            WHEN Age < 70 THEN '60-69'
            WHEN Age < 80 THEN '70-79'
            WHEN Age IS NULL THEN 'Inconnu'
            ELSE '80+'
        END as groupe_age,
       COUNT(*) as total
    FROM shopping
    GROUP BY groupe_age
    ORDER BY groupe_age
""").fetchdf()

# Afficher les deux graphiques côte à côte
# col1, col2 = st.columns(2)

with col1:
    st.subheader("client par sexe")
    
    # Créer un graphique à barres groupées pour les survivants par sexe
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=client_par_sexe['Gender'],
        y=client_par_sexe['Total'],
        name='Client',
        marker_color='green'
    ))
    
    fig.update_layout(
        barmode='group',
        xaxis_title='Sexe',
        yaxis_title='Nombre de clients',
        legend_title='Statut'
    )
    
    st.plotly_chart(fig, use_container_width=True)


with col2:
    st.subheader("client par age")
    
    # Créer un graphique à barres groupées pour les survivants par sexe
    fig1 = go.Figure()
    
    fig1.add_trace(go.Bar(
        x=client_par_age['groupe_age'],
        y=client_par_age['total'],
        name='Client',
        marker_color='red'
    ))
    
    fig1.update_layout(
        barmode='group',
        xaxis_title='Age',
        yaxis_title='Nombre de clients',
        legend_title='Statut'
    )
    
    st.plotly_chart(fig1, use_container_width=True)
