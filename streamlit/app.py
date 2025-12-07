# streamlit_app.py

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import sqlite3
import joblib
import requests
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

# --- CONFIG: URL DE L'API ---
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
    try:
        return f"{price:,.0f} FCFA"
    except Exception:
        return f"{price} FCFA"

# --- Helpers API ---
def call_predict_api(payload, timeout=6):
    """Appel POST /predict -> renvoie (result_dict, None) ou (None, error)"""
    try:
        r = requests.post(f"{API_URL}/predict", json=payload, timeout=timeout)
        if r.status_code == 200:
            return r.json(), None
        else:
            try:
                return None, r.json()
            except Exception:
                return None, f"API returned status {r.status_code}"
    except Exception as e:
        return None, str(e)

def call_metrics_api(timeout=4):
    """Appel GET /metrics -> renvoie dict ou None"""
    try:
        r = requests.get(f"{API_URL}/metrics", timeout=timeout)
        if r.status_code == 200:
            return r.json()
        else:
            return None
    except Exception:
        return None

# ============================
# SIDEBAR
# ============================

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

# ============================
# PAGE 2 : PRÉDICTION
# ============================

elif page == "Prédiction":
    st.title("Prédiction du Prix")
    st.markdown("Estimez le prix d'un bien immobilier en fonction de ses caractéristiques")

    # Charger le modèle local (fallback)
    #model, encoder, scaler, columns, preprocessing_info, metrics_local = load_model()

    #if model is None:
        # still allow API usage even if local model missing
        #pass
    # 1) Try remote API
    
  

    # Formulaire de prédiction
    st.subheader("Caractéristiques du bien")

    col1, col2 = st.columns(2)

    with col1:
        superficie = st.number_input("Superficie (m²)", min_value=10, max_value=1000, value=150)
        nombre_chambres = st.number_input("Nombre de chambres", min_value=1, max_value=10, value=3)
        nombre_sdb = st.number_input("Nombre de salles de bain", min_value=1, max_value=10, value=2)

    with col2:
        type_bien = st.selectbox("Type de bien", df['type_bien'].unique())
        category = st.selectbox("Catégorie", ['location', 'vente'])
        area = st.selectbox("Quartier", sorted(df['area'].unique()))
        city = st.selectbox("Ville", sorted(df['city'].unique()))

    if st.button("Prédire le prix", type="primary", use_container_width=True):
        with st.spinner("Calcul en cours..."):
            # Build payload for API
            payload = {
                "superficie": float(superficie),
                "nombre_chambres": int(nombre_chambres),
                "nombre_sdb": int(nombre_sdb),
                "type_bien": str(type_bien),
                "category": str(category),
                "area": str(area),
                "city": str(city),
            }

            # 1) Try remote API
            api_result, api_error =  call_predict_api(payload)
            # call_predict_api returns (result, error)
            # we used temporary variable above: api_result, api_error = call_predict_api(payload)

            api_result, api_error = api_result, api_error  # keep names consistent

            metrics_api = call_metrics_api()

            if api_result is not None:
                # handle API response
                try:
                    # API returns dictionary with "prediction" key
                    prediction = None
                    if isinstance(api_result, dict):
                        # prefer explicit keys
                        prediction = api_result.get("prediction") or api_result.get("predicted_price") or api_result.get("price")
                        if prediction is None:
                            # search for first numeric value in response
                            for v in api_result.values():
                                if isinstance(v, (int, float)):
                                    prediction = v
                                    break

                    if prediction is None:
                        raise ValueError(f"Réponse API inattendue: {api_result}")

                    # Display prediction
                    st.success("Prédiction (API) réussie !")
                    st.markdown("---")
                    st.subheader("Résultat de la prédiction (API)")

                    st.markdown("---")
                    st.metric("Prix estimé", format_price(prediction))
                    

                    # Display API metrics if available
                    if metrics_api and isinstance(metrics_api, dict):
                        m = metrics_api.get(category, {})
                        if m:
                            st.markdown("---")
                            st.subheader("Performance du modèle (API)")
                            
                            try:
                                with st.expander("performance du modele"):
                                    try:
                                        col1, col2, col3, col4 = st.columns(4)
                                        with col1:
                                            st.metric("R2 Test", f"{m.get('test_r2', 0):.4f}")
                                        with col2:
                                            st.metric("MAE", format_price(m.get('test_mae', 0)))
                                        with col3:
                                            st.metric("RMSE", format_price(m.get('test_rmse', 0)))
                                        with col4:
                                            st.caption(f"🕒 Dernière mise à jour (API): {m.get('timestamp','')}")
                                    except Exception:
                                        st.write("Metrique non disponible")
                            
                            except Exception:
                                st.write("API non disponible")
                                
                except Exception as e:
                    st.error(f"Erreur traitement réponse API: {e}")
                    st.exception(e)
            

# ============================
# PAGE 3 : RECHERCHE
# ============================

elif page == "Recherche":
    col1, col2 = st.columns([1,12])
    with col1:
        st.image("../images/glass.png", width=80)
    with col2:
        st.title("Recherche de Biens")
        st.markdown("Filtrez et trouvez le bien immobilier idéal")

    # Filtres
    st.subheader("Filtres")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        type_filter = st.multiselect("Type de bien", sorted(df['type_bien'].unique()))
    with col2:
        category_filter = st.multiselect("Catégorie", sorted(df['category'].unique()))
    with col3:
        are_filer = st.multiselect("Quartier", sorted(df["area"].unique()))
    with col4:
        city_filter = st.multiselect("Ville", sorted(df['city'].unique()))
    

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
