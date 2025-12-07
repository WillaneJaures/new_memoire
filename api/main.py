from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
import joblib
import json
import pandas as pd
import numpy as np
import os
import time

# ------------------------------------------------------------
# 1️⃣ Initialisation FastAPI
# ------------------------------------------------------------
api_description = "API pour les prédictions des prix immobiliers (location et vente)"

app = FastAPI(
    title="API Immobilier",
    description=api_description,
    version="1.0.0",
)

# ------------------------------------------------------------
# 2️⃣ Chargement des modèles et artefacts
# ------------------------------------------------------------
MODEL_DIR = "../data/models"

# Models
location_model = joblib.load(os.path.join(MODEL_DIR, "location_model.pkl"))
vente_model = joblib.load(os.path.join(MODEL_DIR, "vente_model.pkl"))

# Encoders
encoder_location = joblib.load(os.path.join(MODEL_DIR, "location_encoder.pkl"))
encoder_vente = joblib.load(os.path.join(MODEL_DIR, "vente_encoder.pkl"))

# Feature lists
with open(os.path.join(MODEL_DIR, "location_features.json"), "r") as f:
    location_features = json.load(f)

with open(os.path.join(MODEL_DIR, "vente_features.json"), "r") as f:
    vente_features = json.load(f)

# Metrics
with open(os.path.join(MODEL_DIR, "location_metrics.json"), "r") as f:
    location_metrics = json.load(f)

with open(os.path.join(MODEL_DIR, "vente_metrics.json"), "r") as f:
    vente_metrics = json.load(f)

# ------------------------------------------------------------
# 3️⃣ Pydantic Schema
# ------------------------------------------------------------
class UserData(BaseModel):
    superficie: float = Field(..., gt=0)
    nombre_chambres: int = Field(..., ge=0)
    nombre_sdb: int = Field(..., ge=0)
    type_bien: str
    category: str
    area: str
    city: str

    class Config:
        schema_extra = {
            "example": {
                "superficie": 120,
                "nombre_chambres": 3,
                "nombre_sdb": 2,
                "type_bien": "appartements",
                "category": "vente",
                "area": "yoff",
                "city": "dakar",
            }
        }

# ------------------------------------------------------------
# 4️⃣ Fonctions utilitaires
# ------------------------------------------------------------
def normalize(text):
    if isinstance(text, str):
        return text.lower().replace(" ", "_")
    return text

def preprocess_input(userdata: UserData):
    """
    Préprocessing complet pour prédiction.
    """
    # Normalisation et création dataframe
    df = pd.DataFrame([{
        "superficie": float(userdata.superficie),
        "nombre_chambres": int(userdata.nombre_chambres),
        "nombre_sdb": int(userdata.nombre_sdb),
        "type_bien": normalize(userdata.type_bien),
        "category": normalize(userdata.category),
        "area": normalize(userdata.area),
        "city": normalize(userdata.city),
    }])

    # Feature engineering
    df['ratio_sdb_chambres'] = df['nombre_sdb'] / (df['nombre_chambres'] + 1)
    df['surface_par_chambre'] = df['superficie'] / (df['nombre_chambres'] + 1)
    df['total_pieces'] = df['nombre_chambres'] + df['nombre_sdb']
    df['density'] = df['nombre_chambres'] / (df['superficie'] + 1)
    df['log_superficie'] = np.log1p(df['superficie'])
    df['sqrt_superficie'] = np.sqrt(df['superficie'])
    df['superficie_squared'] = df['superficie'] ** 2
    premium_areas = ['almadies', 'ngor', 'mermoz', 'sacré-coeur', 'fann']
    df['is_premium_area'] = df['area'].isin(premium_areas).astype(int)
    df['is_dakar'] = (df['city'] == 'dakar').astype(int)
    df['is_villa'] = (df['type_bien'] == 'villas').astype(int)
    df['is_location'] = (df['category'] == 'location').astype(int)
    df['villa_large'] = ((df['type_bien'] == 'villas') & (df['superficie'] > 200)).astype(int)
    df['appt_petit'] = ((df['type_bien'] == 'appartements') & (df['superficie'] < 80)).astype(int)
    df['high_bathroom_ratio'] = (df['nombre_sdb'] >= df['nombre_chambres']).astype(int)
    df['spacious'] = (df['surface_par_chambre'] > 40).astype(int)

    # Sélection du modèle et encoder selon category
    if df['category'].iloc[0] == "location":
        mdl = location_model
        enc = encoder_location
        features = location_features
    else:
        mdl = vente_model
        enc = encoder_vente
        features = vente_features

    # Encodage des colonnes catégorielles
    cat_cols = ['type_bien', 'area', 'city', 'category']
    encoded = enc.transform(df[cat_cols])
    encoded_cols = enc.get_feature_names_out(cat_cols)
    df_encoded = pd.DataFrame(encoded, columns=encoded_cols)

    # Concaténation et réindexation
    df_final = pd.concat([df.drop(columns=cat_cols), df_encoded], axis=1)
    df_final = df_final.reindex(columns=features, fill_value=0)

    return df_final, mdl

# ------------------------------------------------------------
# 5️⃣ Routes
# ------------------------------------------------------------
@app.get("/")
def root():
    return {"message": "API Immobilière fonctionnelle !"}

@app.post("/predict")
def predict_price(userdata: UserData):
    try:
        df_final, mdl = preprocess_input(userdata)
        prediction = mdl.predict(df_final)[0]
        return {
            "prediction": float(prediction),
            "input_used": df_final.to_dict(orient="records")[0],
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erreur prédiction : {str(e)}")

@app.get("/metrics")
def get_metrics():
    return {
        "location": location_metrics,
        "vente": vente_metrics
    }

@app.post("/debug/preprocess")
def debug_preprocess(userdata: UserData):
    try:
        df_final, mdl = preprocess_input(userdata)
        return {
            "status": "success",
            "input_data": userdata.dict(),
            "processed_features": {
                "shape": df_final.shape,
                "columns": list(df_final.columns),
                "values": df_final.to_dict(orient="records")[0],
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur debug preprocessing: {str(e)}")
