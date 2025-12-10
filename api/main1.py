from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, validator
from typing import Optional, Dict, List, Any, Literal
import joblib
import json
import pandas as pd
import numpy as np
import os
import time
from datetime import datetime
import traceback

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
# 2️⃣ Chargement des modèles et artefacts - GARDE DE VOTRE CODE
# ------------------------------------------------------------
MODEL_DIR = "../data/models"

try:
    # Models
    location_model = joblib.load(os.path.join(MODEL_DIR, "location_random_optimized_model.pkl"))
    vente_model = joblib.load(os.path.join(MODEL_DIR, "vente_grid_optimized_model.pkl"))

    # Encoders
    encoder_location = joblib.load(os.path.join(MODEL_DIR, "location_random_optimized_encoder.pkl"))
    encoder_vente = joblib.load(os.path.join(MODEL_DIR, "vente_grid_optimized_encoder.pkl"))

    # Feature lists
    with open(os.path.join(MODEL_DIR, "location_random_optimized_features.json"), "r") as f:
        location_features_data = json.load(f)
        location_features = location_features_data.get("features", [])

    with open(os.path.join(MODEL_DIR, "vente_grid_optimized_features.json"), "r") as f:
        vente_features_data = json.load(f)
        vente_features = vente_features_data.get("features", [])

    # Metrics
    with open(os.path.join(MODEL_DIR, "location_random_optimized_metrics.json"), "r") as f:
        location_metrics = json.load(f)

    with open(os.path.join(MODEL_DIR, "vente_grid_optimized_metrics.json"), "r") as f:
        vente_metrics = json.load(f)
        
    print("✅ Tous les modèles et artefacts chargés avec succès")
    print(f"📍 Location: {len(location_features)} features")
    print(f"💰 Vente: {len(vente_features)} features")
    
except Exception as e:
    print(f"❌ Erreur lors du chargement des modèles: {str(e)}")
    raise

# ------------------------------------------------------------
# 3️⃣ Pydantic Schema avec Enum pour éviter l'erreur "string"
# ------------------------------------------------------------
class UserData(BaseModel):
    superficie: float = Field(..., gt=0, description="Superficie en m²")
    nombre_chambres: int = Field(..., ge=0, description="Nombre de chambres")
    nombre_sdb: int = Field(..., ge=0, description="Nombre de salles de bain")
    type_bien: str = Field("villas", description="Type de bien (villas, appartements, bureaux_&_commerces, etc.)")
    category: Literal["location", "vente"] = Field("vente", description="Category: location ou vente")
    area: str = Field("almadies", description="Zone/quartier")
    city: str = Field("dakar", description="Ville")

    class Config:
        schema_extra = {
            "example": {
                "superficie": 120,
                "nombre_chambres": 3,
                "nombre_sdb": 2,
                "type_bien": "villas",
                "category": "vente",
                "area": "almadies",
                "city": "dakar"
            }
        }

# ------------------------------------------------------------
# 4️⃣ Fonctions utilitaires
# ------------------------------------------------------------
def normalize(text: str) -> str:
    """Normalise le texte (minuscules, remplace espaces par underscores)"""
    if isinstance(text, str):
        return text.lower().strip().replace(" ", "_").replace("-", "_").replace("é", "e")
    return str(text)

def create_features(df: pd.DataFrame) -> pd.DataFrame:
    """Crée toutes les features nécessaires - DOIT ÊTRE IDENTIQUE AU TRAINING"""
    df = df.copy()
    
    # Features de base
    df['ratio_sdb_chambres'] = df['nombre_sdb'] / (df['nombre_chambres'] + 1)
    df['surface_par_chambre'] = df['superficie'] / (df['nombre_chambres'] + 1)
    df['total_pieces'] = df['nombre_chambres'] + df['nombre_sdb']
    df['density'] = df['nombre_chambres'] / (df['superficie'] + 1)
    
    # Transformations de superficie
    df['log_superficie'] = np.log1p(df['superficie'])
    df['sqrt_superficie'] = np.sqrt(df['superficie'])
    df['superficie_squared'] = df['superficie'] ** 2
    
    # Features géographiques
    premium_areas = ['almadies', 'ngor', 'mermoz', 'sacre_coeur', 'fann']
    df['is_premium_area'] = df['area'].isin(premium_areas).astype(int)
    df['is_dakar'] = (df['city'] == 'dakar').astype(int)
    
    # Features de type de bien
    df['is_villa'] = (df['type_bien'] == 'villas').astype(int)
    df['is_location'] = (df['category'] == 'location').astype(int)
    
    # Features conditionnelles
    df['villa_large'] = ((df['type_bien'] == 'villas') & (df['superficie'] > 200)).astype(int)
    df['appt_petit'] = ((df['type_bien'] == 'appartements') & (df['superficie'] < 80)).astype(int)
    df['high_bathroom_ratio'] = (df['nombre_sdb'] >= df['nombre_chambres']).astype(int)
    df['spacious'] = (df['surface_par_chambre'] > 40).astype(int)
    
    return df

def prepare_for_prediction(userdata: UserData) -> tuple[pd.DataFrame, Any, List[str]]:
    """Prépare les données pour la prédiction"""
    # Normaliser les entrées
    normalized_data = {
        "superficie": float(userdata.superficie),
        "nombre_chambres": int(userdata.nombre_chambres),
        "nombre_sdb": int(userdata.nombre_sdb),
        "type_bien": normalize(userdata.type_bien),
        "category": userdata.category,  # Déjà validation via Literal
        "area": normalize(userdata.area),
        "city": normalize(userdata.city),
    }
    
    print(f"📝 Données normalisées: {normalized_data}")
    
    # Créer DataFrame
    df = pd.DataFrame([normalized_data])
    
    # Créer toutes les features
    df = create_features(df)
    
    # Sélectionner le modèle et les features appropriés
    if df['category'].iloc[0] == "location":
        model = location_model
        encoder = encoder_location
        expected_features = location_features
    else:
        model = vente_model
        encoder = encoder_vente
        expected_features = vente_features
    
    print(f"🔍 Utilisation du modèle: {'location' if df['category'].iloc[0] == 'location' else 'vente'}")
    
    # Encoder les colonnes catégorielles si encodeur disponible
    if encoder is not None:
        try:
            cat_cols = ['type_bien', 'area', 'city', 'category']
            existing_cat_cols = [col for col in cat_cols if col in df.columns]
            
            if existing_cat_cols:
                # Préparer les données catégorielles
                cat_data = df[existing_cat_cols].fillna("missing")
                
                # Encoder
                encoded = encoder.transform(cat_data)
                encoded_cols = encoder.get_feature_names_out(existing_cat_cols)
                encoded_df = pd.DataFrame(encoded, columns=encoded_cols, index=df.index)
                
                # Supprimer les colonnes catégorielles originales
                df = df.drop(columns=existing_cat_cols)
                # Ajouter les colonnes encodées
                df = pd.concat([df, encoded_df], axis=1)
                
        except Exception as e:
            print(f"⚠️ Avertissement d'encodage: {str(e)}")
            # Si l'encodage échoue, essayer de créer les colonnes manuellement
            try:
                encoded_cols = encoder.get_feature_names_out(existing_cat_cols)
                for col in encoded_cols:
                    if col not in df.columns:
                        df[col] = 0
            except:
                pass
    
    # Créer un DataFrame final avec toutes les features attendues
    df_final = pd.DataFrame(index=df.index)
    
    # Ajouter toutes les features attendues
    for feature in expected_features:
        if feature in df.columns:
            df_final[feature] = df[feature]
        else:
            df_final[feature] = 0  # Valeur par défaut pour les features manquantes
    
    # Réorganiser dans l'ordre exact des features attendues
    if expected_features:
        df_final = df_final[expected_features]
    
    return df_final, model, expected_features

# ------------------------------------------------------------
# 5️⃣ Routes
# ------------------------------------------------------------
@app.get("/")
def root():
    return {
        "message": "API Immobilière fonctionnelle !",
        "endpoints": {
            "/predict": "Prédire un prix (POST)",
            "/metrics": "Voir les métriques des modèles (GET)",
            "/models": "Informations sur les modèles chargés (GET)",
            "/debug/features": "Déboguer les features (POST)"
        },
        "note": "Utilisez /docs pour tester avec des valeurs par défaut pré-remplies"
    }

@app.get("/health")
def health_check():
    """Vérifie que tous les modèles sont chargés correctement"""
    return {
        "status": "healthy",
        "models_loaded": {
            "location": location_model is not None,
            "vente": vente_model is not None
        },
        "timestamp": datetime.now().isoformat()
    }

@app.get("/models")
def get_models_info():
    """Retourne des informations sur les modèles chargés"""
    return {
        "location": {
            "name": "location_random_optimized",
            "optimization_method": "random",
            "features_count": len(location_features),
            "test_r2": location_metrics.get("test_r2"),
            "test_mae": location_metrics.get("test_mae"),
            "test_rmse": location_metrics.get("test_rmse")
        },
        "vente": {
            "name": "vente_grid_optimized",
            "optimization_method": "grid",
            "features_count": len(vente_features),
            "test_r2": vente_metrics.get("test_r2"),
            "test_mae": vente_metrics.get("test_mae"),
            "test_rmse": vente_metrics.get("test_rmse")
        }
    }

@app.post("/predict")
def predict_price(userdata: UserData):
    """
    Prédit le prix d'un bien immobilier
    
    Utilisez les valeurs par défaut ou modifiez-les.
    """
    try:
        print(f"\n🎯 Nouvelle prédiction demandée:")
        print(f"   Category: {userdata.category}")
        print(f"   Type bien: {userdata.type_bien}")
        print(f"   Superficie: {userdata.superficie}")
        
        # Préparer les données pour la prédiction
        df_prepared, model, expected_features = prepare_for_prediction(userdata)
        
        # Vérifier que nous avons toutes les features
        if len(df_prepared.columns) != len(expected_features):
            error_msg = f"Mismatch de features: attendu {len(expected_features)}, obtenu {len(df_prepared.columns)}"
            print(f"❌ {error_msg}")
            raise HTTPException(status_code=500, detail=error_msg)
        
        # Faire la prédiction
        prediction = float(model.predict(df_prepared)[0])
        
        print(f"✅ Prédiction réussie: {prediction:,.0f} FCFA")
        
        # Formater la réponse
        response = {
            "status": "success",
            "prediction": prediction,
            "model_used": "location_random_optimized" if userdata.category == "location" else "vente_grid_optimized",
            "model_type": userdata.category,
            "optimization_method": "random" if userdata.category == "location" else "grid",
            "prediction_timestamp": datetime.now().isoformat(),
            "input_summary": {
                "superficie": userdata.superficie,
                "nombre_chambres": userdata.nombre_chambres,
                "nombre_sdb": userdata.nombre_sdb,
                "type_bien": userdata.type_bien,
                "area": userdata.area,
                "city": userdata.city,
                "category": userdata.category
            }
        }
        
        # Ajouter format monétaire
        if userdata.category == "vente":
            response["price_formatted"] = f"{prediction:,.0f} FCFA"
            response["price_in_millions"] = f"{prediction/1_000_000:.2f} millions FCFA"
        else:
            response["price_formatted"] = f"{prediction:,.0f} FCFA/mois"
        
        return response
        
    except HTTPException as he:
        print(f"❌ HTTPException: {he.detail}")
        raise he
    except Exception as e:
        error_trace = traceback.format_exc()
        print(f"❌ Erreur prédiction: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Erreur lors de la prédiction: {str(e)}"
        )

@app.get("/metrics")
def get_metrics():
    """Retourne les métriques de performance des modèles"""
    return {
        "location": location_metrics,
        "vente": vente_metrics,
        "updated_at": datetime.now().isoformat()
    }

@app.post("/debug/features")
def debug_features(userdata: UserData):
    """
    Débogage: Affiche les features générées pour une entrée donnée
    """
    try:
        # Préparer les données
        df_prepared, model, expected_features = prepare_for_prediction(userdata)
        
        # Informations détaillées
        feature_stats = []
        if not df_prepared.empty:
            for col in df_prepared.columns[:10]:
                value = df_prepared[col].iloc[0]
                feature_stats.append({
                    "feature": col,
                    "value": float(value) if isinstance(value, (int, float, np.number)) else value,
                    "is_zero": bool(value == 0)
                })
        
        return {
            "status": "success",
            "model_used": "location_random_optimized" if userdata.category == "location" else "vente_grid_optimized",
            "category": userdata.category,
            "processed_features": {
                "total_features": len(df_prepared.columns),
                "shape": df_prepared.shape,
                "sample_features": feature_stats
            }
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erreur debug features: {str(e)}"
        )

# ------------------------------------------------------------
# 6️⃣ Exemple de prédiction rapide
# ------------------------------------------------------------
@app.get("/example/predict_vente")
def example_predict_vente():
    """Exemple de prédiction pour une vente"""
    example_data = UserData(
        superficie=120,
        nombre_chambres=3,
        nombre_sdb=2,
        type_bien="villas",
        category="vente",
        area="almadies",
        city="dakar"
    )
    
    return predict_price(example_data)

@app.get("/example/predict_location")
def example_predict_location():
    """Exemple de prédiction pour une location"""
    example_data = UserData(
        superficie=80,
        nombre_chambres=2,
        nombre_sdb=1,
        type_bien="appartements",
        category="location",
        area="point_e",
        city="dakar"
    )
    
    return predict_price(example_data)

# ------------------------------------------------------------
# 7️⃣ Point d'entrée
# ------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    
    print("\n" + "="*60)
    print("🚀 API Immobilière - Démarrage...")
    print("="*60)
    print(f"📍 Modèles chargés:")
    print(f"   • Location: location_random_optimized")
    print(f"   • Vente: vente_grid_optimized")
    print(f"\n📊 Performances:")
    print(f"   • Location R²: {location_metrics.get('test_r2', 'N/A')}")
    print(f"   • Vente R²: {vente_metrics.get('test_r2', 'N/A')}")
    print(f"\n🌐 API disponible sur: http://localhost:8000")
    print("📚 Documentation: http://localhost:8000/docs")
    print("\n🧪 Exemples rapides:")
    print("   • http://localhost:8000/example/predict_vente")
    print("   • http://localhost:8000/example/predict_location")
    print("="*60 + "\n")
    
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)