# streamlit_app.py

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import sqlite3
import joblib
from datetime import datetime
import base64



# --- Fonction utilitaire pour convertir une image en base64 ---
def load_image_base64(path):
    with open(path, "rb") as img:
        return base64.b64encode(img.read()).decode()

# --- Chargement des icônes locales ---
icons = {
    "Dashboard": load_image_base64("../images/bar-chart.png"),
    "Prédiction": load_image_base64("../images/innovation.png"),
    "Recherche": load_image_base64("../images/glass.png"),
    "logo": load_image_base64("../images/office-building.png"),
    "vente": load_image_base64("../images/sale.png"),
    "location": load_image_base64("../images/rent.png"),
    "type": load_image_base64("../images/office-building.png"),
    "ville": load_image_base64("../images/city.png"),
    "performance": load_image_base64("../images/bar-chart.png"),
    "prediction": load_image_base64("../images/innovation.png"),
    "filtre": load_image_base64("../images/filter.png"),
    "resultat": load_image_base64("../images/result.png"),
    "download": load_image_base64("../images/download.png"),
}

# Configuration de la page
st.set_page_config(
    page_title="ImmoSenegal - Dashboard",
    page_icon="../images/house.png",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================
# FONCTIONS UTILITAIRES
# ============================
# URL de l'API
API_URL = "http://127.0.0.1:8000"


@st.cache_data
def load_data():
    """Charger les données depuis SQLite"""
    try:
        conn = sqlite3.connect('../data/immobilier.db')
        df = pd.read_sql("SELECT * FROM realestate", conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f" Erreur chargement données: {e}")
        return pd.DataFrame()

def format_price(price):
    """Formater le prix"""
    return f"{price:,.0f} FCFA"


with st.sidebar:
    col1, col2 = st.columns([1,4])
    with col1:
        st.image("../images/house.png", width=80)
    with col2:
        st.markdown("<h2 style='margin:0px; padding-top:5px;'>ImmoSenegal</h2>", unsafe_allow_html=True)
    st.markdown("<hr>", unsafe_allow_html=True)

    
    page = st.radio(
        "Navigation",
        ["Dashboard", "Prédiction", "Recherche"]
    )

# CHARGEMENT DES DONNÉES
df = load_data()

if df.empty:
    st.error(" Aucune donnée disponible. Veuillez lancer le pipeline ETL.")
    st.stop()


# ============================
# PAGE 1 : DASHBOARD
# ============================

if page == "Dashboard":
    col1, col2 = st.columns([1,12])
    with col1:
        st.image("../images/result.png", width = 80)
    with col2:
        st.title("Dashboard Immobilier")
    st.markdown("Vue d'ensemble du marché immobilier sénégalais")
    
    # Métriques principales
    col1, col2, col3 = st.columns(3)
    
    with col1:
        total_properties = len(df)
        st.metric("Total Propriétés", f"{total_properties:,}")
    
    with col2:
        col1, col2 = st.columns([1,7])
        with col1:
            st.image("../images/sale.png", width = 30)
        with col2:
            total_ventes0 = len(df[df['category'] == 'Vente'])
            total_ventes1 = len(df[df["category"] == 'vente'])
            total_ventes = total_ventes0 + total_ventes1
            st.metric("Ventes", f"{total_ventes:,}")
    
    with col3:
        col1, col2 = st.columns([1,7])
        with col1:
            st.image("../images/rent.png")
        with col2:
            total_locations = len(df[df['category'] == 'Location'])
            total_locations1 = len(df[df['category'] == 'location'])
            total_loc = total_locations + total_locations1
            st.metric("Locations", f"{total_loc:,}")
    
    st.markdown("---")
    
    # Graphiques par catégorie
    col1, col2 = st.columns([1,16])
    with col1:
        st.image("../images/wallet.png", width = 40)
    with col2:
        st.subheader("Prix moyen par quartier (Top 10)")
    
    # Séparer les données par catégorie
    df_vente = df[df['category'] == 'vente']
    df_location = df[df['category'] == 'location']
    
    # Deux colonnes pour afficher côte à côte
    col1, col2 = st.columns(2)
    with col1:
        st.image("../images/sale.png", width=30)
        st.markdown("### Vente")
        if not df_vente.empty:
            avg_price_vente = df_vente.groupby('area')['price'].mean().sort_values(ascending=False).head(10)
            fig_vente = px.bar(x=avg_price_vente.index, y=avg_price_vente.values,
                               labels={'x': 'Area', 'y': 'Prix moyen (FCFA)'}, title="Top 10 - Vente")
            st.plotly_chart(fig_vente, use_container_width=True)
    with col2:
        st.image("../images/rent.png", width=30)
        st.markdown("### Location")
        if not df_location.empty:
            avg_price_location = df_location.groupby('area')['price'].mean().sort_values(ascending=False).head(10)
            fig_location = px.bar(x=avg_price_location.index, y=avg_price_location.values,
                                  labels={'x': 'Quartier', 'y': 'Prix moyen (FCFA)'}, title="Top 10 - Location")
            st.plotly_chart(fig_location, use_container_width=True)

    
    st.markdown("---")
    
    # Répartition par type
    col1, col2 = st.columns(2)
    with col1:
        st.image("../images/pie-chart.png", width=30)
        st.subheader("Répartition par type")
        type_counts = df['type_bien'].value_counts()
        fig = px.pie(values=type_counts.values, names=type_counts.index)
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        st.image("../images/city.png", width=30)
        st.subheader("Répartition par ville")
        city_counts = df['city'].value_counts().head(10)
        fig = px.bar(x=city_counts.index, y=city_counts.values, labels={'x': 'Ville', 'y': 'Nombre de biens'})
        st.plotly_chart(fig, use_container_width=True)

elif page == "Prédiction":
    st.title("Prédiction du Prix")
    st.markdown("Estimez le prix d'un bien immobilier en fonction de ses caractéristiques")

    import requests

    st.subheader("Entrer les caractéristiques du bien")

    superficie = st.number_input("Superficie (m²)", min_value=1.0, value=80.0)
    chambres = st.number_input("Nombre de chambres", min_value=0, value=2)
    sdb = st.number_input("Nombre de salles de bain", min_value=0, value=1)

    type_bien = st.selectbox("Type de bien", [
        "appartements",
        "villas",
        "studios",
        "terrains",
    ])

    category = st.selectbox("Catégorie", ["location", "vente"])
    area = st.text_input("Quartier", "grand_dakar")
    city = st.text_input("Ville", "dakar")

    if st.button("Prédire"):
        data = {
            "superficie": superficie,
            "nombre_chambres": chambres,
            "nombre_sdb": sdb,
            "type_bien": type_bien,
            "category": category,
            "area": area,
            "city": city,
        }

        with st.spinner("Calcul de la prédiction..."):
            try:
                response = requests.post(f"{API_URL}/predict", json=data)

                if response.status_code == 200:
                    result = response.json()
                    prix = result.get("prediction", None)

                    if prix is not None:
                        st.success(f"Prix estimé : **{prix:,.0f} FCFA**")

                        try:
                            metrics_response = requests.get(f"{API_URL}/metrics")

                            if metrics_response.status_code == 200:
                                m = metrics_response.json()

                                colM1, colM2, colM3 = st.columns(3)

                                with colM1:
                                    st.metric("Train R²", f"{m['train_r2']:.3f}")

                                with colM2:
                                    st.metric("Test R²", f"{m['test_r2']:.3f}")

                                with colM3:
                                    st.metric("Test MAE", f"{m['test_mae']:,.0f} FCFA")

                                colM4, colM5 = st.columns(2)
                                with colM4:
                                    st.metric("Test RMSE", f"{m['test_rmse']:,.0f} FCFA")

                                with colM5:
                                    st.metric("Modèle", m["model_name"])

                                st.caption(f"🕒 Dernière mise à jour : {m['timestamp']}")

                            else:
                                st.warning("Impossible de charger les métriques du modèle.")
                        except Exception as e:
                            st.error(f"Erreur lors de la récupération des métriques : {e}")
                    else:
                        st.error("Erreur : réponse API sans prix.")
                else:
                    st.error(f"Erreur API : {response.status_code}")
                    st.json(response.json())

            except Exception as e:
                st.error(f"Erreur de connexion à l'API : {e}")




elif page == "Recherche":
    col1, col2 = st.columns([1,12])
    with col1:
        st.image("../images/glass.png", width=80)
    with col2:
        st.title("Recherche de Biens")
        st.markdown("Filtrez et trouvez le bien immobilier idéal")
    
    # Filtres
    st.subheader("Filtres")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        type_filter = st.multiselect("Type de bien", df['type_bien'].unique())
    with col2:
        category_filter = st.multiselect("Catégorie", df['category'].unique())
    with col3:
        city_filter = st.multiselect("Ville", df['city'].unique())
    
    col1, col2 = st.columns(2)
    
    with col1:
        price_min = st.number_input(
            "Prix minimum (FCFA)", 
            min_value=0, 
            max_value=int(df['price'].max()),  
            value=int(df['price'].min()),       
            step=100000
        )
        price_max = st.number_input(
            "Prix maximum (FCFA)", 
            min_value=price_min,                
            max_value=int(df['price'].max()),  
            value=int(df['price'].max()), 
            step=100000
        )
        
    with col2:
        superficie_min = st.number_input(
            "Superficie minimum (m²)", 
            min_value=0, 
            max_value=int(df['superficie'].max()),  
            value=int(df['superficie'].min()),      
            step=10
        )
        superficie_max = st.number_input(
            "Superficie maximum (m²)", 
            min_value=superficie_min,               
            max_value=int(df['superficie'].max()), 
            value=int(df['superficie'].max()), 
            step=10
        )
    # Appliquer les filtres
    filtered_df = df.copy()
    
    if type_filter:
        filtered_df = filtered_df[filtered_df['type_bien'].isin(type_filter)]
    if category_filter:
        filtered_df = filtered_df[filtered_df['category'].isin(category_filter)]
    if city_filter:
        filtered_df = filtered_df[filtered_df['city'].isin(city_filter)]
    
    filtered_df = filtered_df[
        (filtered_df['price'].between(price_min, price_max)) &
        (filtered_df['superficie'].between(superficie_min, superficie_max))
    ]
    
    # Résultats
    st.markdown("---")
    st.subheader(f"Résultats ({len(filtered_df)} biens trouvés)")
    
    if not filtered_df.empty:
        # Tri
        sort_by = st.selectbox(
            "Trier par",
            ["Prix croissant", "Prix décroissant", "Superficie croissante", "Superficie décroissante"]
        )
        
        if sort_by == "Prix croissant":
            filtered_df = filtered_df.sort_values('price')
        elif sort_by == "Prix décroissant":
            filtered_df = filtered_df.sort_values('price', ascending=False)
        elif sort_by == "Superficie croissante":
            filtered_df = filtered_df.sort_values('superficie')
        else:
            filtered_df = filtered_df.sort_values('superficie', ascending=False)
        
        # Affichage
        st.dataframe(
            filtered_df[['type_bien', 'price', 'superficie', 'nombre_chambres', 'nombre_sdb', 'area', 'city', 'category']],
            use_container_width=True
        )
        
        # Export
        csv = filtered_df.to_csv(index=False)
        st.download_button(
            label="Télécharger les résultats (CSV)",
            data=csv,
            file_name=f"recherche_immobilier_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )
    else:
        st.warning("⚠️ Aucun bien ne correspond à vos critères")

