from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import List, Optional
import joblib
import json
import uvicorn
import numpy as np
import pandas as pd
import os
from pydantic import BaseModel, Field


api_description = " Bienvenu dans l'api pour les predictions des prix immobiliers"

app = FastAPI(
    title = "API Immobilier",
    description = api_description,
    version = "1.0.0",
)

# -----------------------------------------
# CORS (si Streamlit appelle l’API)
# -----------------------------------------

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------------------
# Chargement des artefacts
# -----------------------------------------

MODEL_DIR = '../data/models'

model = None
scaler = None
encoder = None
metrics = None
FEATURE_LIST = None
CATEGORICAL_COLS = None
preprocessing_info = None
NO_SCALING_MODELS = {"GradientBoosting", "RandomForest", "Extra Trees", "Decision Tree", "XGBBoost"}

try:
    model = joblib.load(os.path.join(MODEL_DIR, "best_model.pkl"))
    print("Modèle chargé avec succès")
except Exception as e:
    print(f"Erreur lors du chargement du modèle: {e}")

try:
    scaler = joblib.load(os.path.join(MODEL_DIR, 'scaler.pkl'))
    print("✅ Scaler chargé avec succès")
except Exception as e:
    print(f"❌ Erreur lors du chargement du scaler: {e}")

try:
    encoder = joblib.load(os.path.join(MODEL_DIR, 'encoder.pkl'))
    print("✅ Encoder chargé avec succès")
except Exception as e:
    print(f"❌ Erreur lors du chargement de l'encoder: {e}")

try:
    metrics = joblib.load(os.path.join(MODEL_DIR, 'metrics.pkl'))
    print("✅ Métriques chargées avec succès")
except UnicodeDecodeError:
    print(f"❌ metrics.json n'est pas au format UTF-8")
    print("   Utilisez json.dump() au lieu de joblib.dump()")
    metrics = None
except Exception as e:
    print(f"❌ Erreur lors du chargement des métriques: {e}")
    metrics = None

try:
    preprocessing_info = joblib.load(os.path.join(MODEL_DIR, 'preprocessing_info.pkl'))
    print("Preprocessing chargé avec succes")
except Exception as e:
    print(f"Erreur lors du chargement des métriques: {e}")


def should_use_scaler() -> bool:
    """
    Détermine si le scaler doit être appliqué avant la prédiction.
    Si le meilleur modèle est un modèle entraîné SANS scaling (arbres, boosting, etc.),
    on ne doit pas appliquer le StandardScaler dans l'API.
    """
    if metrics is None or not isinstance(metrics, dict):
        return True  # par défaut on garde le scaler

    model_name = metrics.get("model_name")
    if not model_name:
        return True

    # Si le nom du modèle correspond à un modèle entraîné sans scaling, on ne scale pas
    for no_scale in NO_SCALING_MODELS:
        if no_scale.lower() in str(model_name).lower():
            return False

    return True

# ---- Schéma d'entrée avec validation stricte ----

class UserData(BaseModel):
    superficie: float = Field(..., gt=0, description="Superficie du bien en m²")
    nombre_chambres: int = Field(..., ge=0, description="Nombre de chambres")
    nombre_sdb: int = Field(..., ge=0, description="Nombre de salles de bain")

    type_bien: str = Field(..., description="Type de bien (ex: maison, villa, appartement)")
    category: str = Field(..., description="vente ou location")
    area: str = Field(..., description="Nom du quartier")
    city: str = Field(..., description="Ville du bien")

    class Config:
        schema_extra = {
            "example": {
                "superficie": 80,
                "nombre_chambres": 1,
                "nombre_sdb": 1,
                "type_bien": "appartements",
                "category": "location",
                "area": "grand_dakar",
                "city": "dakar"
            }
        }


# -----------------------------------------
# Préprocessing identique au modèle
# -----------------------------------------

def preprocess_input(data: UserData):
    """
    Reproduit EXACTEMENT le preprocessing utilisé lors de l'entraînement.

    data : UserData (pydantic model)

    Retourne :
        - X_final : numpy array prêt pour model.predict()
    """

    if preprocessing_info is None:
        raise ValueError("Preprocessing_info.pkl manquant : impossible de reconstruire les features.")

    try:
        # Récupération de la définition exacte des features utilisées à l'entraînement
        numerical_cols = preprocessing_info.get("numerical_cols", [])
        categorical_cols = preprocessing_info.get("categorical_cols", [])
        feature_names = preprocessing_info.get("feature_names", [])

        # 1) Construction d'une ligne de base à partir des données utilisateur
        #    (les noms de colonnes doivent correspondre à ceux utilisés dans l'entraînement)
        # IMPORTANT : Normalisation identique à l'entraînement (mydag.py ligne 1226)
        # df[col] = df[col].astype(str).str.lower().str.replace(" ", "_")
        
        def normalize_categorical(value: str) -> str:
            """Normalise une valeur catégorielle exactement comme dans l'entraînement"""
            if not value:
                return ""
            # Convertir en string, minuscules, remplacer TOUS les espaces par underscores
            normalized = str(value).lower().replace(" ", "_")
            # Gérer les cas spéciaux comme "Point-e" -> "point_e" (mais l'entraînement fait "Point E" -> "point_e")
            normalized = normalized.replace("point-e", "point_e")
            normalized = normalized.replace("point_e", "point_e")  # déjà géré
            return normalized
        
        base = {
            "superficie": float(data.superficie),
            "nombre_chambres": int(data.nombre_chambres),
            "nombre_sdb": int(data.nombre_sdb),
            "type_bien": normalize_categorical(data.type_bien),
            "category": normalize_categorical(data.category),
            "area": normalize_categorical(data.area),
            "city": normalize_categorical(data.city),
        }
        
        # Correction spéciale pour "immeubles" -> "terrains" (comme dans l'entraînement ligne 1230)
        if base["type_bien"] == "immeubles":
            base["type_bien"] = "terrains"

        # Créer le DataFrame de manière déterministe
        df_input = pd.DataFrame([base], index=[0])
        
        # ============================================================
        # TRANSFORMATIONS PRÉ-ENTRAÎNEMENT (comme dans mydag.py)
        # ============================================================
        
        # 1) Terrains = 0 chambres/sdb (ligne 1232-1237 de mydag.py)
        terrains_list = ["terrains", "terrains_agricoles", "terrains_commerciaux", "bureaux_&_commerces"]
        if df_input["type_bien"].iloc[0] in terrains_list:
            df_input.loc[0, "nombre_chambres"] = 0
            df_input.loc[0, "nombre_sdb"] = 0
        
        # 2) Corrections de valeurs aberrantes (lignes 1240-1257 de mydag.py)
        # NOTE: Ces corrections nécessitent les médianes du dataset d'entraînement
        # qui ne sont pas actuellement sauvegardées dans preprocessing_info.
        # 
        # Pour une solution complète, il faudrait modifier mydag.py pour sauvegarder :
        # - Les médianes par type_bien pour chaque colonne (superficie, nombre_chambres, nombre_sdb)
        # - Les quartiles (Q1, Q3) pour le traitement des outliers
        #
        # Pour l'instant, on applique seulement les corrections de base :
        
        # Si des statistiques sont disponibles dans preprocessing_info, les utiliser
        correction_stats = preprocessing_info.get("correction_stats", {}) if preprocessing_info else {}
        
        # Correction : Villas avec superficie < 150
        if (df_input["type_bien"].iloc[0] == "villas" and 
            df_input["superficie"].iloc[0] < 150 and 
            "villas_superficie_median" in correction_stats):
            df_input.loc[0, "superficie"] = correction_stats["villas_superficie_median"]
        
        # Correction : Appartements avec nombre_chambres > 6
        if (df_input["type_bien"].iloc[0] == "appartements" and 
            df_input["nombre_chambres"].iloc[0] > 6 and 
            "appartements_nombre_chambres_median" in correction_stats):
            df_input.loc[0, "nombre_chambres"] = correction_stats["appartements_nombre_chambres_median"]
        
        # Correction : Villas avec nombre_chambres > 9
        if (df_input["type_bien"].iloc[0] == "villas" and 
            df_input["nombre_chambres"].iloc[0] > 9 and 
            "villas_nombre_chambres_median" in correction_stats):
            df_input.loc[0, "nombre_chambres"] = correction_stats["villas_nombre_chambres_median"]
        
        # Correction : Appartements avec nombre_sdb > 6
        if (df_input["type_bien"].iloc[0] == "appartements" and 
            df_input["nombre_sdb"].iloc[0] > 6 and 
            "appartements_nombre_sdb_median" in correction_stats):
            df_input.loc[0, "nombre_sdb"] = correction_stats["appartements_nombre_sdb_median"]
        
        # Correction : Villas avec nombre_sdb > 9
        if (df_input["type_bien"].iloc[0] == "villas" and 
            df_input["nombre_sdb"].iloc[0] > 9 and 
            "villas_nombre_sdb_median" in correction_stats):
            df_input.loc[0, "nombre_sdb"] = correction_stats["villas_nombre_sdb_median"]
        
        # Correction : Bureaux avec superficie < 20
        if (df_input["type_bien"].iloc[0] == "bureaux_&_commerces" and 
            df_input["superficie"].iloc[0] < 20 and 
            "bureaux_superficie_median" in correction_stats):
            df_input.loc[0, "superficie"] = correction_stats["bureaux_superficie_median"]

        # 2) Recalcul des features d'ingénierie EXACTEMENT comme dans le script d'entraînement
        #    (cf. expery/try.py)
        df_input["ratio_sdb_chambres"] = df_input["nombre_sdb"] / (df_input["nombre_chambres"] + 1)
        df_input["surface_par_chambre"] = df_input["superficie"] / (df_input["nombre_chambres"] + 1)
        df_input["total_pieces"] = df_input["nombre_chambres"] + df_input["nombre_sdb"]
        df_input["density"] = df_input["nombre_chambres"] / (df_input["superficie"] + 1)

        df_input["log_superficie"] = np.log1p(df_input["superficie"])
        df_input["sqrt_superficie"] = np.sqrt(df_input["superficie"])
        df_input["superficie_squared"] = df_input["superficie"] ** 2

        premium_areas = ["almadies", "ngor", "mermoz", "sacré-coeur", "fann"]
        df_input["is_premium_area"] = df_input["area"].isin(premium_areas).astype(int)
        df_input["is_dakar"] = (df_input["city"] == "dakar").astype(int)

        df_input["is_villa"] = (df_input["type_bien"] == "villas").astype(int)
        df_input["is_location"] = (df_input["category"] == "location").astype(int)

        df_input["villa_large"] = ((df_input["type_bien"] == "villas") & (df_input["superficie"] > 200)).astype(int)
        df_input["appt_petit"] = ((df_input["type_bien"] == "appartements") & (df_input["superficie"] < 80)).astype(int)

        df_input["high_bathroom_ratio"] = (df_input["nombre_sdb"] >= df_input["nombre_chambres"]).astype(int)
        df_input["spacious"] = (df_input["surface_par_chambre"] > 40).astype(int)

        # 3) Séparation numérique / catégoriel selon preprocessing_info
        if not numerical_cols or not categorical_cols:
            raise ValueError("Les informations de preprocessing (numerical_cols / categorical_cols) sont incomplètes.")

        # Vérifier que toutes les colonnes numériques existent
        missing_num_cols = set(numerical_cols) - set(df_input.columns)
        if missing_num_cols:
            raise ValueError(f"Colonnes numériques manquantes après feature engineering : {missing_num_cols}")

        # Vérifier que toutes les colonnes catégorielles existent
        missing_cat_cols = set(categorical_cols) - set(df_input.columns)
        if missing_cat_cols:
            raise ValueError(f"Colonnes catégorielles manquantes : {missing_cat_cols}")

        X_num = df_input[numerical_cols]
        X_cat_raw = df_input[categorical_cols]

        # 4) Encodage catégoriel avec l'encoder entraîné
        if encoder is None:
            raise ValueError("Encoder non trouvé !")

        X_cat_encoded = encoder.transform(X_cat_raw)
        X_cat_df = pd.DataFrame(X_cat_encoded, columns=feature_names)

        # 5) Reconstruction finale des features dans le même ordre qu'à l'entraînement
        X_full = pd.concat(
            [X_num.reset_index(drop=True), X_cat_df.reset_index(drop=True)],
            axis=1,
        )

        # CRITIQUE : Réordonner les colonnes exactement comme à l'entraînement
        # Le modèle attend un ordre précis de features
        all_columns = preprocessing_info.get("all_columns", [])
        if all_columns:
            # Vérifier que toutes les colonnes attendues sont présentes
            missing_cols = set(all_columns) - set(X_full.columns)
            if missing_cols:
                raise ValueError(f"Colonnes manquantes après preprocessing : {missing_cols}")
            
            # Vérifier qu'on a le bon nombre de colonnes
            extra_cols = set(X_full.columns) - set(all_columns)
            if extra_cols:
                raise ValueError(f"Colonnes supplémentaires non attendues : {extra_cols}")
            
            # Réordonner selon l'ordre exact de l'entraînement
            # IMPORTANT : Utiliser reindex pour garantir l'ordre exact
            X_full = X_full.reindex(columns=all_columns)
            
            # Vérification finale : s'assurer que l'ordre est correct
            if list(X_full.columns) != all_columns:
                raise ValueError(f"Ordre des colonnes incorrect après réordonnancement")
        else:
            # Fallback : utiliser l'ordre actuel si all_columns n'est pas disponible
            print("⚠️ Warning: all_columns non trouvé dans preprocessing_info, utilisation de l'ordre actuel")

        # 6) Application du scaler (entraîné sur toutes les colonnes X)
        if scaler is not None and should_use_scaler():
            # Le scaler.transform() peut recevoir un DataFrame pandas directement
            # ou un numpy array. On s'assure que c'est bien un array 2D
            if isinstance(X_full, pd.DataFrame):
                X_full_array = X_full.values
            else:
                X_full_array = np.array(X_full)
            
            # S'assurer que c'est bien 2D
            if len(X_full_array.shape) == 1:
                X_full_array = X_full_array.reshape(1, -1)
            
            X_full = scaler.transform(X_full_array)
        else:
            # Si pas de scaler, convertir le DataFrame en numpy array
            if isinstance(X_full, pd.DataFrame):
                X_full = X_full.values
            else:
                X_full = np.array(X_full)

        # S'assurer qu'on retourne toujours un numpy array 2D
        X_full = np.array(X_full)
        if len(X_full.shape) == 1:
            X_full = X_full.reshape(1, -1)

        return X_full

    except Exception as e:
        raise ValueError(f"Erreur preprocessing : {e}")


# -----------------------------------------
# Route PREDICT
# -----------------------------------------

@app.post("/predict")
def predict_price(payload: UserData):
    """
    Prédit le prix du bien immobilier.
    """

    # --- Vérification que le modèle est chargé ---
    if model is None:
        raise HTTPException(status_code=500, detail="Modèle introuvable ou non chargé.")

    # --- Vérification valeurs manquantes ---
    for field, value in payload.dict().items():
        if value is None or (isinstance(value, str) and value.strip() == ""):
            raise HTTPException(
                status_code=400,
                detail=f"Le champ '{field}' est vide ou manquant."
            )

    try:
        # --- Preprocessing identique ---
        X_input = preprocess_input(payload)
        
        # Debug: vérifier que X_input est bien un array 2D
        X_input = np.array(X_input)
        if len(X_input.shape) == 1:
            X_input = X_input.reshape(1, -1)
        elif X_input.shape[0] != 1:
            raise ValueError(f"X_input doit avoir une seule ligne, mais a {X_input.shape[0]} lignes")

        # --- Prédiction ---
        y_pred = model.predict(X_input)[0]

        # Fourchette probable ±20%
        low = max(y_pred * 0.82, 0)
        high = y_pred * 1.18

        return {
            "status": "success",
            "prediction": int(y_pred),
            "fourchette_basse": int(low),
            "fourchette_haute": int(high)
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erreur interne lors de la prédiction : {e}"
        )








@app.get("/", tags=["Santé"])
async def root():
    return {"message": "API immobilier operationnelle"}


@app.post("/test/debug-same-input", tags=["Test"])
async def debug_same_input(payload: UserData):
    """
    Debug: Compare les résultats de /predict et /test/compare avec le même input
    pour identifier pourquoi ils donnent des résultats différents.
    """
    try:
        # Appel 1: Via preprocess_input directement (comme dans /predict)
        X1 = preprocess_input(payload)
        pred1 = model.predict(X1)[0]
        
        # Appel 2: Via preprocess_input dans le contexte de compare (comme dans /test/compare)
        X2 = preprocess_input(payload)
        pred2 = model.predict(X2)[0]
        
        # Comparer les features
        X1_array = np.array(X1)
        X2_array = np.array(X2)
        
        if len(X1_array.shape) == 1:
            X1_array = X1_array.reshape(1, -1)
        if len(X2_array.shape) == 1:
            X2_array = X2_array.reshape(1, -1)
        
        features_diff = {}
        if preprocessing_info and preprocessing_info.get("all_columns"):
            columns = preprocessing_info["all_columns"]
            for i, col in enumerate(columns):
                val1 = float(X1_array[0, i])
                val2 = float(X2_array[0, i])
                if abs(val1 - val2) > 1e-6:
                    features_diff[col] = {"call1": val1, "call2": val2, "diff": val2 - val1}
        
        return {
            "status": "success",
            "payload": payload.dict(),
            "prediction_call1": float(pred1),
            "prediction_call2": float(pred2),
            "difference": float(pred2 - pred1),
            "x1_shape": X1_array.shape,
            "x2_shape": X2_array.shape,
            "features_identical": len(features_diff) == 0,
            "different_features": features_diff,
            "num_different_features": len(features_diff)
        }
    except Exception as e:
        import traceback
        raise HTTPException(
            status_code=500,
            detail=f"Erreur lors du debug : {str(e)}\n\n{traceback.format_exc()}"
        )


@app.post("/test/compare", tags=["Test"])
async def compare_predictions(payload1: UserData):
    """
    Compare deux prédictions : une avec le payload fourni, une avec type_bien="villas".
    Utile pour vérifier que le modèle réagit bien aux changements de type de bien.
    """
    try:
        # Créer un payload2 identique mais avec type_bien="villas"
        payload2_dict = payload1.dict()
        original_type = payload2_dict.get("type_bien", "appartements")
        
        # Si c'est déjà une villa, tester avec appartements, sinon avec villas
        if original_type.lower() in ["villas", "villa"]:
            payload2_dict["type_bien"] = "appartements"
            comparison_note = f"Comparaison: {original_type} vs appartements"
        else:
            payload2_dict["type_bien"] = "villas"
            comparison_note = f"Comparaison: {original_type} vs villas"
        
        payload2 = UserData(**payload2_dict)
        
        # Preprocessing pour les deux
        X1 = preprocess_input(payload1)
        X2 = preprocess_input(payload2)
        
        # Prédictions
        pred1 = model.predict(X1)[0]
        pred2 = model.predict(X2)[0]
        
        # Différences dans les features
        diff_features = {}
        columns = []
        if preprocessing_info and preprocessing_info.get("all_columns"):
            columns = preprocessing_info["all_columns"]
            for i, col in enumerate(columns):
                val1 = float(X1[0, i])
                val2 = float(X2[0, i])
                if abs(val1 - val2) > 1e-6:  # Seuil pour éviter les erreurs de précision
                    diff_features[col] = {"payload1": val1, "payload2": val2, "diff": val2 - val1}
        
        return {
            "status": "success",
            "comparison": comparison_note,
            "payload1": payload1.dict(),
            "payload2": payload2.dict(),
            "prediction1": float(pred1),
            "prediction2": float(pred2),
            "difference": float(pred2 - pred1),
            "percent_difference": float((pred2 - pred1) / pred1 * 100) if pred1 != 0 else 0,
            "different_features": diff_features,
            "num_different_features": len(diff_features),
            "features_summary": {
                "total_features": len(columns),
                "changed_features": len(diff_features),
                "key_changes": {k: v for k, v in diff_features.items() if "type_bien" in k or "is_villa" in k}
            }
        }
    except Exception as e:
        import traceback
        raise HTTPException(
            status_code=500,
            detail=f"Erreur lors de la comparaison : {str(e)}\n\n{traceback.format_exc()}"
        )


@app.get("/metrics", tags=["Modèle"])
async def get_model_metrics():
    """
    Retourne les métriques du meilleur modèle entraîné.
    """
    if metrics is None:
        raise HTTPException(
            status_code=500,
            detail="Métriques introuvables ou non chargées."
        )

    try:
        # metrics peut être un dict, une liste ou un objet custom issu de joblib
        if isinstance(metrics, (dict, list, str, int, float)):
            return {"status": "success", "metrics": metrics}
        # fallback : conversion via json / string
        return {
            "status": "success",
            "metrics": json.loads(json.dumps(metrics, default=str))
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Impossible de sérialiser les métriques : {e}"
        )


@app.post("/debug/preprocess", tags=["Debug"])
async def debug_preprocess(payload: UserData):
    """
    Route de debug pour voir exactement ce qui est envoyé au modèle.
    Utile pour diagnostiquer les problèmes de preprocessing.
    """
    try:
        # Normaliser les données d'entrée pour voir ce qui sera utilisé
        def normalize_categorical(value: str) -> str:
            if not value:
                return ""
            normalized = str(value).lower().replace(" ", "_")
            normalized = normalized.replace("point-e", "point_e")
            return normalized
        
        normalized_input = {
            "type_bien": normalize_categorical(payload.type_bien),
            "category": normalize_categorical(payload.category),
            "area": normalize_categorical(payload.area),
            "city": normalize_categorical(payload.city),
        }
        if normalized_input["type_bien"] == "immeubles":
            normalized_input["type_bien"] = "terrains"
        
        # Récupérer les catégories attendues par l'encoder
        encoder_categories = {}
        if encoder is not None:
            try:
                # L'encoder OneHotEncoder stocke les catégories dans categories_
                if hasattr(encoder, 'categories_'):
                    cat_cols = preprocessing_info.get("categorical_cols", [])
                    for idx, col in enumerate(cat_cols):
                        if idx < len(encoder.categories_):
                            encoder_categories[col] = encoder.categories_[idx].tolist()
            except Exception as e:
                encoder_categories["error"] = str(e)
        
        X_input = preprocess_input(payload)
        
        # X_input devrait maintenant toujours être un numpy array 2D
        X_array = np.array(X_input)
        if len(X_array.shape) == 1:
            X_array = X_array.reshape(1, -1)
        
        # Convertir en liste pour la sérialisation JSON
        features_dict = {}
        if preprocessing_info and preprocessing_info.get("all_columns"):
            columns = preprocessing_info["all_columns"]
            if len(columns) == X_array.shape[1]:
                for i, col in enumerate(columns):
                    features_dict[col] = float(X_array[0, i])
            else:
                features_dict["error"] = f"Nombre de colonnes mismatch: {len(columns)} attendues, {X_array.shape[1]} obtenues"
                features_dict["raw_features"] = X_array[0].tolist()
        else:
            features_dict["raw_features"] = X_array[0].tolist()
            features_dict["warning"] = "all_columns non disponible dans preprocessing_info"
        
        return {
            "status": "success",
            "input_data": payload.dict(),
            "normalized_input": normalized_input,
            "features_shape": X_array.shape,
            "features": features_dict,
            "use_scaler": should_use_scaler(),
            "model_name": metrics.get("model_name") if metrics else None,
            "encoder_categories": encoder_categories,
            "preprocessing_info_keys": list(preprocessing_info.keys()) if preprocessing_info else None,
            "numerical_cols": preprocessing_info.get("numerical_cols", []) if preprocessing_info else None,
            "categorical_cols": preprocessing_info.get("categorical_cols", []) if preprocessing_info else None
        }
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Erreur lors du debug preprocessing : {str(e)}\n\nTraceback:\n{error_details}"
        )

