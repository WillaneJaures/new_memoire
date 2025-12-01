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

@st.cache_resource
def load_model():
    """Charger le modèle ML et ses artefacts"""
    try:
        model = joblib.load('../data/models/best_model.pkl')
        encoder = joblib.load('../data/models/encoder.pkl')
        metrics = joblib.load('../data/models/metrics.pkl')
        
        try:
            preprocessing_info = joblib.load('../data/models/preprocessing_info.pkl')
        except Exception:
            preprocessing_info = None
        
        try:
            scaler = joblib.load('../data/models/scaler.pkl')
        except Exception:
            scaler = None
        
        columns = preprocessing_info  # Pour compatibilité
        
        return model, encoder, scaler, columns, preprocessing_info, metrics
    except Exception as e:
        st.error(f" Erreur chargement modèle: {e}")
        return None, None, None, None, None, None

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
                               labels={'x': 'Quartier', 'y': 'Prix moyen (FCFA)'}, title="Top 10 - Vente")
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
        type_counts = df['type'].value_counts()
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
    
    # Charger le modèle
    model, encoder, scaler, columns, preprocessing_info, metrics = load_model()
    
    if model is None:
        st.error(" Modèle non disponible. Veuillez lancer l'entraînement.")
        st.stop()
    
    # Afficher les performances du modèle
    with st.expander("Performance du modèle"):
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("R² Test", f"{metrics.get('test_r2', 0):.4f}")
        with col2:
            st.metric("MAE", format_price(metrics.get('test_mae', 0)))
        with col3:
            st.metric("RMSE", format_price(metrics.get('test_rmse', 0)))
        with col4:
            st.metric("Overfitting", f"{metrics.get('overfitting', 0):.4f}")
        
        st.caption(f"Dernière mise à jour: {metrics.get('timestamp', 'N/A')}")
    
    st.markdown("---")
    
    # Formulaire de prédiction
    st.subheader("Caractéristiques du bien")
    
    col1, col2 = st.columns(2)
    
    with col1:
        superficie = st.number_input("Superficie (m²)", min_value=10, max_value=1000, value=150)
        nombre_chambres = st.number_input("Nombre de chambres", min_value=1, max_value=10, value=3)
        nombre_sdb = st.number_input("Nombre de salles de bain", min_value=1, max_value=10, value=2)
    
    with col2:
        type_bien = st.selectbox("Type de bien", df['type'].unique())
        category = st.selectbox("Catégorie", ['location', 'vente'])
        area = st.selectbox("Quartier", sorted(df['area'].unique()))
        city = st.selectbox("Ville", sorted(df['city'].unique()))
    
    if st.button("Prédire le prix", type="primary", use_container_width=True):
        with st.spinner("Calcul en cours..."):
            try:
                # Feature Engineering (identique à l'entraînement)
                ratio_sdb_chambres = nombre_sdb / (nombre_chambres + 1)
                surface_par_chambre = superficie / (nombre_chambres + 1)
                total_pieces = nombre_chambres + nombre_sdb
                density = nombre_chambres / (superficie + 1)
                log_superficie = np.log1p(superficie)
                sqrt_superficie = np.sqrt(superficie)
                superficie_squared = superficie ** 2
                
                # Features binaires
                premium_areas = ['almadies', 'ngor', 'mermoz', 'sacré-coeur', 'fann']
                is_premium_area = 1 if area.lower() in premium_areas else 0
                is_dakar = 1 if city.lower() == 'dakar' else 0
                is_villa = 1 if type_bien.lower() == 'villas' else 0
                is_location = 1 if category.lower() == 'location' else 0
                villa_large = 1 if (type_bien.lower() == 'villas' and superficie > 200) else 0
                appt_petit = 1 if (type_bien.lower() == 'appartements' and superficie < 80) else 0
                high_bathroom_ratio = 1 if nombre_sdb >= nombre_chambres else 0
                spacious = 1 if surface_par_chambre > 40 else 0
                
                # Construire les features
                numerical_data = {
                    'superficie': superficie,
                    'nombre_chambres': nombre_chambres,
                    'nombre_sdb': nombre_sdb,
                    'ratio_sdb_chambres': ratio_sdb_chambres,
                    'surface_par_chambre': surface_par_chambre,
                    'total_pieces': total_pieces,
                    'density': density,
                    'log_superficie': log_superficie,
                    'sqrt_superficie': sqrt_superficie,
                    'superficie_squared': superficie_squared,
                    'is_premium_area': is_premium_area,
                    'is_dakar': is_dakar,
                    'is_villa': is_villa,
                    'is_location': is_location,
                    'villa_large': villa_large,
                    'appt_petit': appt_petit,
                    'high_bathroom_ratio': high_bathroom_ratio,
                    'spacious': spacious
                }
                
                categorical_data = {
                    'type': type_bien,
                    'category': category,
                    'area': area,
                    'city': city
                }
                
                # Créer DataFrames
                X_num = pd.DataFrame([numerical_data])
                X_cat = pd.DataFrame([categorical_data])
                
                # Encoder
                X_cat_encoded = encoder.transform(X_cat)
                encoded_feature_names = encoder.get_feature_names_out(list(categorical_data.keys()))
                X_cat_df = pd.DataFrame(X_cat_encoded, columns=encoded_feature_names)
                
                # Combiner
                X = pd.concat([X_num.reset_index(drop=True), X_cat_df.reset_index(drop=True)], axis=1)
                
                # S'assurer que les colonnes sont dans le bon ordre
                if preprocessing_info and 'all_columns' in preprocessing_info:
                    all_columns = preprocessing_info['all_columns']
                    for col in all_columns:
                        if col not in X.columns:
                            X[col] = 0
                    X = X[all_columns]
                
                # Prédire
                prediction = model.predict(X)[0]
                
                # Intervalle de confiance
                rmse = metrics.get('test_rmse', prediction * 0.15)
                confidence_low = max(0, prediction - rmse)
                confidence_high = prediction + rmse
                
                # Affichage des résultats
                st.success("Prédiction réussie !")
                
                st.markdown("---")
                st.subheader("Résultat de la prédiction")
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Prix estimé", format_price(prediction))
                
                with col2:
                    st.metric("Fourchette basse", format_price(confidence_low))
                
                with col3:
                    st.metric("Fourchette haute", format_price(confidence_high))
                
                # Graphique Train vs Test
                st.markdown("---")
                st.subheader("Performance du modèle")
                
                fig = go.Figure()
                
                train_r2 = metrics.get('train_r2', 0)
                test_r2 = metrics.get('test_r2', 0)
                cv_mean = metrics.get('cv_mean', test_r2)
                
                fig.add_trace(go.Bar(
                    x=['Train R²', 'Test R²', 'CV R² (5-fold)'],
                    y=[train_r2, test_r2, cv_mean],
                    marker_color=['#3498db', '#e74c3c', '#2ecc71'],
                    text=[f'{train_r2:.4f}', f'{test_r2:.4f}', f'{cv_mean:.4f}'],
                    textposition='outside',
                    textfont=dict(size=14)
                ))
                
                fig.add_hline(
                    y=0.75,
                    line_dash="dash",
                    line_color="green",
                    annotation_text="Seuil bon modèle (0.75)"
                )
                
                fig.update_layout(
                    title=f"R² Score du modèle {metrics.get('model_name', 'ML')}",
                    yaxis_title="R² Score",
                    yaxis=dict(range=[0, 1]),
                    height=400
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Métriques supplémentaires
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("MAE", format_price(metrics['test_mae']))
                
                with col2:
                    st.metric("RMSE", format_price(metrics['test_rmse']))
                
                with col3:
                    overfitting = metrics.get('overfitting', train_r2 - test_r2)
                    status = " Excellent" if overfitting < 0.05 else "⚠️ Modéré"
                    st.metric("Overfitting", f"{overfitting:.4f}", delta=status)
                
                
                
            except Exception as e:
                st.error(f" Erreur: {e}")
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
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        type_filter = st.multiselect("Type de bien", df['type'].unique())
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
            max_value=int(df['price'].max()) * 2,  
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
            max_value=int(df['superficie'].max()) * 2, 
            value=int(df['superficie'].max()), 
            step=10
        )
    # Appliquer les filtres
    filtered_df = df.copy()
    
    if type_filter:
        filtered_df = filtered_df[filtered_df['type'].isin(type_filter)]
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
            filtered_df[['type', 'price', 'superficie', 'nombre_chambres', 'nombre_sdb', 'area', 'city', 'category']],
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