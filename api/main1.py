from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
import joblib
import json
import pandas as pd
import numpy as np
import uvicorn
import os


api_description = " Bienvenu dans l'api pour les predictions des prix immobiliers"

app = FastAPI(
    title="API Immobilier",
    description=api_description,
    version="1.0.0",
)

# ---------- Chargement des artefacts ----------
MODEL_DIR = "../data/models"

model = None
encoder = None
metrics = None
feature_list = None
categorical_cols = None

# Load model
try:
    model = joblib.load(os.path.join(MODEL_DIR, "model.pkl"))
    print("Modèle chargé avec succès")
except Exception as e:
    print(f"Erreur lors du chargement du modèle: {e}")

# Load encoder
try:
    encoder = joblib.load(os.path.join(MODEL_DIR, "encoder.pkl"))
    print("Encoder chargé avec succès")
except Exception as e:
    print(f"Erreur lors du chargement de l'encoder: {e}")

# Load metrics
try:
    with open(os.path.join(MODEL_DIR, "metrics.json"), "r", encoding="utf-8") as f:
        metrics = json.load(f)
    print("Métriques chargées avec succès")
except Exception as e:
    print(f"Erreur lors du chargement des metrics: {e}")

# Load feature list
try:
    with open(os.path.join(MODEL_DIR, "feature_list.json"), "r", encoding="utf-8") as f:
        feature_list = json.load(f)
    print("Feature list chargée avec succès")
except Exception as e:
    print(f"Erreur lors du chargement de feature_list: {e}")

# Load categorical cols
try:
    with open(os.path.join(MODEL_DIR, "categorical_cols.json"), "r") as f:
        categorical_cols = json.load(f)
    print("Categorical columns chargées avec succès")
except Exception as e:
    print(f"Erreur lors du chargement de categorical_cols: {e}")



# ---------- Pydantic Schema ----------
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


def normalize(value):
    """Normalisation du texte comme lors du preprocessing d'entraînement"""
    if isinstance(value, str):
        return value.lower().replace(" ", "_")
    return value


def preprocess_input(userdata: UserData):
    """
    Reproduction EXACTE du preprocessing utilisé pendant l'entraînement.
    """

    # Normalisation des valeurs textuelles
    data = {
        "superficie": float(userdata.superficie),
        "nombre_chambres": int(userdata.nombre_chambres),
        "nombre_sdb": int(userdata.nombre_sdb),
        "type_bien": normalize(userdata.type_bien),
        "category": normalize(userdata.category),
        "area": normalize(userdata.area),
        "city": normalize(userdata.city),
    }

    df_input = pd.DataFrame([data])

    # Séparer numérique / catégoriel
    object_cols = df_input.select_dtypes(include=[object]).columns.tolist()
    num_cols = df_input.select_dtypes(exclude=[object]).columns.tolist()

    # Encodage des variables catégorielles
    try:
        encoded = encoder.transform(df_input[object_cols])
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Erreur d'encodage: {str(e)}"
        )

    encoded_cols = encoder.get_feature_names_out(object_cols)
    df_encoded = pd.DataFrame(encoded, columns=encoded_cols)

    # Concaténation
    df_final = pd.concat([df_input[num_cols], df_encoded], axis=1)

    # IMPORTANT — respecter l'ordre utilisé pour entraîner le modèle
    df_final = df_final.reindex(columns=feature_list, fill_value=0)

    return df_final



# ---------- Routes ----------
@app.get("/")
def root():
    return {"message": "API Immobilière fonctionnelle !"}



@app.post("/predict")
def predict_price(userdata: UserData):
    """
    Endpoint principal pour faire une prédiction.
    """
    if model is None:
        raise HTTPException(status_code=500, detail="Modèle non chargé.")

    try:
        df_final = preprocess_input(userdata)

        prediction = model.predict(df_final)[0]

        return {
            "prediction": float(prediction),
            "input_used": df_final.to_dict(orient="records")[0],
        }

    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Erreur lors de la prédiction: {str(e)}"
        )



@app.get("/metrics")
def get_metrics():
    if metrics is None:
        raise HTTPException(
            status_code=404,
            detail="Métriques non disponibles."
        )
    return metrics



@app.post("/debug/preprocess")
def debug_preprocess(userdata: UserData):
    """
    Permet de vérifier les colonnes et valeurs envoyées au modèle.
    """
    try:
        df_final = preprocess_input(userdata)

        return {
            "shape": df_final.shape,
            "columns": list(df_final.columns),
            "values": df_final.to_dict(orient="records")[0],
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erreur debug preprocessing: {str(e)}"
        )
