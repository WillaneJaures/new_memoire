from airflow import DAG
from airflow.decorators import task
import os
from datetime import datetime, timedelta
import logging
import polars as pl 
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import json


default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='new_dag',
    start_date=datetime(2025, 11, 24),
    schedule='@weekly',
    default_args=default_args,
    description='A new Dag with simplify code structure',
    catchup=False,
    tags=['immobilier', 'scraping', 'data_processing']
) as dag:
    
    @task
    def scrape_coinmarket():
        # Lazy imports to speed up DAG parse time
        import pandas as pd
        from bs4 import BeautifulSoup as bs
        from requests import get
        import time

        df = pd.DataFrame()
        for i in range(1, 100):  
            url = f'https://sn.coinafrique.com/categorie/immobilier?page={i}'
            logging.info(f"🔎 Scraping page {i}: {url}")
            res = get(url)
            soup = bs(res.text, 'html.parser')
            info_all = soup.find_all('div', class_='col s6 m4 l3')
            links = ['https://sn.coinafrique.com' + a.find('a')['href'] for a in info_all]

            data = []
            for x in links:
                try:
                    res = get(x)
                    soup = bs(res.text, 'html.parser')

                    price_inf = soup.find('p', class_='price')
                    price = price_inf.text.replace(" ", "").replace("CFA", "") if price_inf else ''

                    description_tag = soup.find('h1', class_='title title-ad hide-on-large-and-down')
                    description = description_tag.text if description_tag else ''

                    info2 = soup.find_all("span", class_="valign-wrapper")
                    time_p = info2[0].text if len(info2) > 0 else ''
                    location = info2[1].text if len(info2) > 1 else ''
                    type_ = info2[2].text if len(info2) > 2 else ''

                    info3 = soup.find_all('span', class_="qt")
                    nbr_p = info3[0].text if len(info3) > 0 else ''
                    nbr_sb = info3[1].text if len(info3) > 1 else ''
                    sup = info3[2].text if len(info3) > 2 else ''

                    obj = {
                        'Price': price,
                        'Description': description,
                        'Location': location,
                        'posted_at': time_p,
                        'Type': type_,
                        'Nombre_de_piece': nbr_p,
                        'Nombre_de_salle_bain': nbr_sb,
                        'Superficie': sup
                    }

                    data.append(obj)
                    time.sleep(1)

                except Exception as e:
                    logging.error(f"❌ Error parsing page: {x}, {e}")
                    continue

            logging.info(f"✅ Scraped page {i}, total entries so far: {len(data)}")
            df = pd.concat([df, pd.DataFrame(data)], ignore_index=True)

        if not df.empty:
            df.to_csv('./data/coinmarket.csv', index=False)
            logging.info("✅ Data scraped and saved to ../data/coinmarket.csv")
        else:
            logging.warning("⚠️ No data scraped, DataFrame is empty.")


    @task
    def scrape_expatdakar():
        """Scrape ExpatDakar immobilier data using requests + BeautifulSoup"""
        import pandas as pd
        from bs4 import BeautifulSoup as bs
        from requests import get
        import logging
        import time
        import re
        import random

        results = []
        clean = lambda t: re.sub(r'\s+', ' ', t.strip()) if t else ''
        
        USER_AGENTS = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
        ]
        
        REFERERS = [
            'https://www.google.com/',
            'https://www.bing.com/',
            'https://www.expat-dakar.com/',
        ]

        for page in range(1, 144):
            url = f"https://www.expat-dakar.com/appartements-a-louer?page={page}"
            logging.info(f"🔎 Scraping ExpatDakar page {page}")
            
            headers = {
                'User-Agent': random.choice(USER_AGENTS),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
                'Referer': random.choice(REFERERS),
                'Cache-Control': 'no-cache',
            }
            
            try:
                time.sleep(random.uniform(0.75, 1.5))
                res = get(url, timeout=15, headers=headers)
                
                if res.status_code in [403, 429]:
                    logging.warning(f"⚠️ Page {page} retourne {res.status_code}, skip")
                    continue
                    
                res.raise_for_status()
                soup = bs(res.text, 'html.parser')
                
                links = []
                for a in soup.find_all('a', class_='listing-card__inner'):
                    href = a.get('href')
                    if href:
                        full_link = 'https://www.expat-dakar.com' + href if href.startswith('/') else href
                        links.append(full_link)
                
                if not links:
                    for a in soup.find_all('a', href=True):
                        if '/appartement' in a['href']:
                            full_link = 'https://www.expat-dakar.com' + a['href'] if a['href'].startswith('/') else a['href']
                            if full_link not in links:
                                links.append(full_link)
                
                logging.info(f"✅ Found {len(links)} listings on page {page}")
                
                for idx, link in enumerate(links[:10], 1):
                    try:
                        time.sleep(random.uniform(0.5, 1.0))
                        
                        headers['User-Agent'] = random.choice(USER_AGENTS)
                        res = get(link, timeout=15, headers=headers)
                        
                        if res.status_code in [403, 429]:
                            logging.warning(f"⚠️ Listing {idx} blocked ({res.status_code})")
                            continue
                            
                        res.raise_for_status()
                        soup = bs(res.text, 'html.parser')
                        
                        price_tag = soup.find('span', class_='listing-card__price__value')
                        price = clean(price_tag.text) if price_tag else ''
                        price = price.replace(' ', '').replace('FCfa', '').replace('F', '')
                        
                        quartier_tag = soup.find('span', class_='listing-item__address-location')
                        quartier = clean(quartier_tag.text) if quartier_tag else ''
                        
                        region_tag = soup.find('span', class_='listing-item__address-region')
                        region = clean(region_tag.text) if region_tag else 'Dakar'
                        
                        infos = []
                        for dd in soup.find_all('dd', class_='listing-item__properties__description'):
                            infos.append(clean(dd.text))
                        
                        nombre_chambres = infos[0] if len(infos) > 0 else ''
                        nombre_sdb = infos[1] if len(infos) > 1 else ''
                        superficie = infos[2].replace(' ', '').replace('m²', '').strip() if len(infos) > 2 else ''
                        
                        type_tag = soup.find('h1', class_='listing-item__title')
                        type_text = clean(type_tag.text) if type_tag else 'Appartement'
                        
                        results.append({
                            "Price": price,
                            "quartier": quartier,
                            "region": region,
                            "nombre_chambres": nombre_chambres,
                            "nombre_sdb": nombre_sdb,
                            "superficie": superficie,
                            "type": type_text
                        })
                        
                        if idx % 5 == 0:
                            logging.info(f"  📊 Scraped {idx}/{len(links)} listings from page {page}")
                        
                    except Exception as e:
                        logging.error(f"❌ Error parsing listing {idx}: {e}")
                        continue
                
                logging.info(f"✅ Page {page} complete - Total collected: {len(results)}")
                        
            except Exception as e:
                logging.error(f"❌ Error on page {page}: {e}")
                continue

        df = pd.DataFrame(results)
        output_path = "/tmp/expatdakar.csv"
        
        if not df.empty:
            df.to_csv(output_path, index=False, encoding='utf-8')
            df.to_csv("./data/expatdakar.csv", index=False, encoding='utf-8')
            logging.info(f"✅ {len(df)} annonces sauvegardées dans {output_path}")
        else:
            logging.warning("⚠️ Aucun résultat trouvé sur Expat-Dakar.")
            pd.DataFrame(columns=['Price', 'quartier', 'region', 'nombre_chambres', 
                                 'nombre_sdb', 'superficie', 'type']).to_csv(output_path, index=False)

    @task
    def transform_source1():
        """Transform CoinMarket data"""
        import polars as pl
        import logging
        import random
        import sys

        input_csv = './data/coinmarket.csv'
        
        try:
            # Vérification de l'existence du fichier
            if not os.path.exists(input_csv):
                print(f"❌ Error: File {input_csv} does not exist!")
                print(f"Current directory: {os.getcwd()}")
                print(f"Files in /tmp: {os.listdir('/tmp') if os.path.exists('/tmp') else 'Directory /tmp does not exist'}")
                sys.exit(1)

            print(f"✅ File {input_csv} found, size: {os.path.getsize(input_csv)} bytes")

            # ============================
            # 1️⃣  CHARGEMENT DES DONNÉES
            # ============================
            df = pl.read_csv(input_csv, infer_schema_length=10000)
            #df = df.rename({col: col.lower() for col in df.columns})
            cols = df.columns
            lower = [c.lower() for c in cols]

            if len(lower) != len(set(lower)):
                print("⚠️ Colonnes dupliquées détectées après .lower(). Correction automatique.")
                fixed = []
                seen = {}
                for c in lower:
                    if c in seen:
                        seen[c] += 1
                        fixed.append(f"{c}_{seen[c]}")
                    else:
                        seen[c] = 0
                        fixed.append(c)
                df = df.rename(dict(zip(cols, fixed)))
            else:
                df = df.rename(dict(zip(cols, lower)))

            print("Aperçu des données brutes :")
            print(df.head(5))

            # ============================
            # 2️⃣  SUPPRESSION DES DOUBLONS
            # ============================
            df1 = df.unique()
            print(f"Nombre de lignes avant : {df.height}, après suppression des doublons : {df1.height}")

            # ============================
            # 3️⃣  NETTOYAGE DES COLONNES (déplacement des m2)
            # ============================
            # Traiter nombre_de_piece
            df2 = df1.with_columns([
                pl.when(pl.col("nombre_de_piece").str.contains("m2"))
                .then(pl.col("nombre_de_piece").str.strip_chars())
                .otherwise(pl.col("superficie"))
                .alias("superficie"),
                pl.when(pl.col("nombre_de_piece").str.contains("m2"))
                .then(None)
                .otherwise(pl.col("nombre_de_piece").str.strip_chars())
                .alias("nombre_de_piece")
            ])

            # Traiter nombre_de_salle_bain
            df3 = df2.with_columns([
                pl.when(pl.col("nombre_de_salle_bain").str.contains("m2"))
                .then(pl.col("nombre_de_salle_bain").str.strip_chars())
                .otherwise(pl.col("superficie"))
                .alias("superficie"),
                pl.when(pl.col("nombre_de_salle_bain").str.contains("m2"))
                .then(None)
                .otherwise(pl.col("nombre_de_salle_bain").str.strip_chars())
                .alias("nombre_de_salle_bain")
            ])

            # Nettoyer la superficie (supprimer "m2")
            df4 = df3.with_columns([
                pl.col("superficie").str.replace("m2", "").str.strip_chars().alias("superficie")
            ])

            # ============================
            # 4️⃣  GESTION DU PRIX
            # ============================
            # Nettoyage : suppression des caractères non numériques
            df5 = df4.with_columns([
                pl.col("price").str.replace_all(r"[^0-9]", "").alias("price_clean")
            ])

            # Conversion en numérique
            df6 = df5.with_columns([
                pl.when(pl.col("price_clean") != "")
                .then(pl.col("price_clean").cast(pl.Int64, strict=False))
                .otherwise(None)
                .alias("prix_num")
            ])

            # Calcul de la moyenne pour remplacer les valeurs manquantes
            mean_price = int(df6["prix_num"].mean()) if df6["prix_num"].mean() is not None else 0

            df7 = df6.with_columns([
                pl.when(pl.col("prix_num").is_null())
                .then(pl.lit(mean_price))
                .otherwise(pl.col("prix_num"))
                .alias("price")
            ])

            # ============================
            # 5️⃣  EXTRACTION DE CATEGORY
            # ============================
            df8 = df6.with_columns([
                pl.col("description").str.split(" ").list.first().alias("category")
            ])

            # Supprimer les colonnes inutiles
            df9 = df8.drop(["posted_at", "prix_num", "description", "price_clean"])

            # ============================
            # 6️⃣  CORRECTIONS DE LOCATION / TYPE
            # ============================
            # Correction 1: Location == "Appartements"
            df10 = df9.with_columns([
                pl.when(pl.col("location") == "Appartements")
                .then(pl.col("type"))
                .otherwise(pl.col("location"))
                .alias("location_temp"),
                pl.when(pl.col("location") == "Appartements")
                .then(pl.col("location"))
                .otherwise(pl.col("type"))
                .alias("type")
            ]).drop("location").rename({"location_temp": "location"})

            # Correction 2: Location == "Villas"
            df11 = df10.with_columns([
                pl.when(pl.col("location") == "Villas")
                .then(pl.col("type"))
                .otherwise(pl.col("location"))
                .alias("location")
            ])

            # Correction 3: Location == "Terrains"
            df12 = df11.with_columns([
                pl.when(pl.col("location") == "Terrains")
                .then(pl.col("type"))
                .otherwise(pl.col("location"))
                .alias("location")
            ])

            # ============================
            # 7️⃣  NORMALISATION DE CATEGORY
            # ============================
            df13 = df12.with_columns([
                pl.col("category")
                .str.replace(r"Terrains?", "Terrain")
                .str.replace(r"(?i)locations?", "Location")
                .str.replace(r"Locaton|Location/", "Location")
                .str.replace(r"Terrain|Six|Parcelle|100", "Vente")
                .alias("category")
            ])

            # ============================
            # 8️⃣  NORMALISATION TYPE
            # ============================
            df14 = df13.with_columns([
                pl.col("type")
                .str.replace("appartements", "Appartements")
                .str.replace("villas", "Villas")
                .alias("type")
            ])

            # Filtrer category pour ne garder que Location ou Vente
            df15 = df14.with_columns([
                pl.when((pl.col("category") == "Location") | (pl.col("category") == "Vente"))
                .then(pl.col("category"))
                .otherwise(pl.lit("Vente"))
                .alias("category")
            ])

            # ============================
            # 9️⃣  AJUSTEMENTS TYPE / LOCATION / CITY
            # ============================
            df16 = df15.with_columns([
                pl.when(pl.col("type").str.contains("Sénégal"))
                .then(pl.lit("Appartements"))
                .otherwise(pl.col("type"))
                .alias("type")
            ])

            # Extraction de Area
            df17 = df16.with_columns([
                pl.col("location").str.split(",").list.first().alias("area")
            ])

            # Correction Area pour valeurs aberrantes
            df18 = df17.with_columns([
                pl.when(pl.col("area").str.contains("(?i)(Immeubles|Bureaux & Commerces|Fermes & Vergers|Maisons de vacances|Terrains agricoles|Appartements meublés)"))
                .then(pl.lit("Dakar"))
                .otherwise(pl.col("area"))
                .alias("area")
            ])

            # Extraction de City basée sur Location
            df19 = df18.with_columns([
                pl.when(pl.col("location").str.contains("(?i)Dakar")).then(pl.lit("Dakar"))
                .when(pl.col("location").str.contains("(?i)Thies")).then(pl.lit("Thies"))
                .when(pl.col("location").str.contains("(?i)Saint-Louis")).then(pl.lit("Saint-Louis"))
                .when(pl.col("location").str.contains("(?i)Kaolack")).then(pl.lit("Kaolack"))
                .when(pl.col("location").str.contains("(?i)Ziguinchor")).then(pl.lit("Ziguinchor"))
                .when(pl.col("location").str.contains("(?i)Louga")).then(pl.lit("Louga"))
                .when(pl.col("location").str.contains("(?i)Saly")).then(pl.lit("Saly"))
                .when(pl.col("location").str.contains("(?i)Keur Massar")).then(pl.lit("Keur Massar"))
                .when(pl.col("location").str.contains("(?i)Diamniadio")).then(pl.lit("Diamniadio"))
                .when(pl.col("location").str.contains("(?i)Mbour")).then(pl.lit("Mbour"))
                .when(pl.col("location").str.contains("(?i)Lac rose")).then(pl.lit("Lac rose"))
                .when(pl.col("location").str.contains("(?i)Rufisque")).then(pl.lit("Rufisque"))
                .when(pl.col("location").str.contains("(?i)Ndiass")).then(pl.lit("Ndiass"))
                .when(pl.col("location").str.contains("(?i)Kolda")).then(pl.lit("Kolda"))
                .when(pl.col("location").str.contains("(?i)Ngaparou")).then(pl.lit("Ngaparou"))
                .when(pl.col("location").str.contains("(?i)Niaga")).then(pl.lit("Niaga"))
                .when(pl.col("location").str.contains("(?i)Toubab Dialao")).then(pl.lit("Toubab Dialao"))
                .when(pl.col("location").str.contains("(?i)Mboro")).then(pl.lit("Mboro"))
                .otherwise(pl.lit("Null"))
                .alias("city")
            ])

            # Remplissage aléatoire des zones de Dakar
            choices = ["Almadies", "Yoff", "Ngor", "Point E", "Mamelles"]
            df20 = df19.with_columns([
                pl.when(pl.col("area") == "Dakar")
                .then(
                    pl.when(pl.lit(0).hash() % 5 == 0).then(pl.lit(choices[0]))
                    .when(pl.lit(0).hash() % 5 == 1).then(pl.lit(choices[1]))
                    .when(pl.lit(0).hash() % 5 == 2).then(pl.lit(choices[2]))
                    .when(pl.lit(0).hash() % 5 == 3).then(pl.lit(choices[3]))
                    .otherwise(pl.lit(choices[4]))
                )
                .otherwise(pl.col("area"))
                .alias("area")
            ])

            # Remplacer "Null" par "Dakar" dans City
            df21 = df20.with_columns([
                pl.when(pl.col("city") == "Null")
                .then(pl.lit("Dakar"))
                .otherwise(pl.col("city"))
                .alias("city")
            ]).drop("location")

            # ============================
            # 🔟  REMPLISSAGE VALEURS MANQUANTES
            # ============================
            # Nombre de pièces
            df22 = df21.with_columns([
                pl.when(pl.col("nombre_de_piece").is_null())
                .then(
                    pl.when(pl.lit(0).hash() % 5 == 0).then(pl.lit("4"))
                    .when(pl.lit(0).hash() % 5 == 1).then(pl.lit("3"))
                    .when(pl.lit(0).hash() % 5 == 2).then(pl.lit("2"))
                    .when(pl.lit(0).hash() % 5 == 3).then(pl.lit("5"))
                    .otherwise(pl.lit("1"))
                )
                .otherwise(pl.col("nombre_de_piece"))
                .alias("nombre_de_piece")
            ])

            # Nombre de salles de bain
            df23 = df22.with_columns([
                pl.when(pl.col("nombre_de_salle_bain").is_null())
                .then(
                    pl.when(pl.lit(0).hash() % 5 == 0).then(pl.lit("2"))
                    .when(pl.lit(0).hash() % 5 == 1).then(pl.lit("5"))
                    .when(pl.lit(0).hash() % 5 == 2).then(pl.lit("3"))
                    .when(pl.lit(0).hash() % 5 == 3).then(pl.lit("4"))
                    .otherwise(pl.lit("1"))
                )
                .otherwise(pl.col("nombre_de_salle_bain"))
                .alias("nombre_de_salle_bain")
            ])

            # Superficie
            df24 = df23.with_columns([
                pl.when(pl.col("superficie").is_null())
                .then(
                    pl.when(pl.lit(0).hash() % 5 == 0).then(pl.lit("100"))
                    .when(pl.lit(0).hash() % 5 == 1).then(pl.lit("200"))
                    .when(pl.lit(0).hash() % 5 == 2).then(pl.lit("300"))
                    .when(pl.lit(0).hash() % 5 == 3).then(pl.lit("400"))
                    .otherwise(pl.lit("500"))
                )
                .otherwise(pl.col("superficie"))
                .alias("superficie")
            ])

            # ============================
            # 1️⃣1️⃣  CONVERSION FINALE
            # ============================
            df25 = df24.with_columns([
                pl.col("nombre_de_piece").cast(pl.Int64, strict=False),
                pl.col("nombre_de_salle_bain").cast(pl.Int64, strict=False),
                pl.col("superficie").cast(pl.Int64, strict=False),
                pl.col("price").cast(pl.Int64, strict=False)
            ])

            # Renommer nombre_de_piece en nombre_chambres
            df_clean = df25.rename({"nombre_de_piece": "nombre_chambres"})
            df1_clean = df_clean.rename({"nombre_de_salle_bain": "nombre_sdb"})

            print("\n✅ Aperçu des données transformées :")
            print(df1_clean.head(20))

            print(f"\n📊 Dimensions finales : {df1_clean.height} lignes × {df1_clean.width} colonnes")
            print(f"\n📋 Colonnes finales : {df1_clean.columns}")
            print("valeur unique dans city:")
            print(df1_clean['city'].unique())
            print("\n🔍 VALEURS MANQUANTES APRÈS TRANSFORMATION:")
            for col in df1_clean.columns:
                null_count = df1_clean[col].null_count()
                if null_count > 0:
                    print(f"  {col}: {null_count} valeurs manquantes ({null_count/df1_clean.height*100:.1f}%)")

            # Optionnel: sauvegarder le résultat
            # df_clean.write_csv("/tmp/coinmarket_clean.csv")
            # ============================
            # 1️⃣2️⃣ SAUVEGARDE
            # ============================
            output_path = "./data/cleaned_data.csv"
            df1_clean.write_csv(output_path)
            print(f"✅ Données nettoyées sauvegardées dans : {output_path}")
        except Exception as e:
            logging.error(f"❌ Error transforming coinmarket data: {e}")
            raise
    
    @task
    def transform_source2():
        import polars as pl
        import os
        import sys
        import re
        input_csv = "./data/expatdakar.csv"

        def transform_expatdakar_standardized(input_csv):
            input_csv = "./data/coinmarket.csv"

            # Vérification de l'existence du fichier
            if not os.path.exists(input_csv):
                print(f"❌ Error: File {input_csv} does not exist!")
                print(f"Current directory: {os.getcwd()}")
                print(f"Files in /tmp: {os.listdir('/tmp') if os.path.exists('/tmp') else 'Directory /tmp does not exist'}")
                sys.exit(1)

            print(f"✅ File {input_csv} found, size: {os.path.getsize(input_csv)} bytes")

            # ============================
            # 1️⃣  CHARGEMENT DES DONNÉES
            # ============================
            df = pl.read_csv(input_csv, infer_schema_length=10000)
            df = df.rename({col: col.lower() for col in df.columns})

            print("Aperçu des données brutes :")
            print(df.head(5))

            # ============================
            # 2️⃣  SUPPRESSION DES DOUBLONS
            # ============================
            df1 = df.unique()
            print(f"Nombre de lignes avant : {df.height}, après suppression des doublons : {df1.height}")

            # ============================
            # 3️⃣  NETTOYAGE DES COLONNES (déplacement des m2)
            # ============================
            # Traiter nombre_de_piece
            df2 = df1.with_columns([
                pl.when(pl.col("nombre_de_piece").str.contains("m2"))
                .then(pl.col("nombre_de_piece").str.strip_chars())
                .otherwise(pl.col("superficie"))
                .alias("superficie"),
                pl.when(pl.col("nombre_de_piece").str.contains("m2"))
                .then(None)
                .otherwise(pl.col("nombre_de_piece").str.strip_chars())
                .alias("nombre_de_piece")
            ])

            # Traiter nombre_de_salle_bain
            df3 = df2.with_columns([
                pl.when(pl.col("nombre_de_salle_bain").str.contains("m2"))
                .then(pl.col("nombre_de_salle_bain").str.strip_chars())
                .otherwise(pl.col("superficie"))
                .alias("superficie"),
                pl.when(pl.col("nombre_de_salle_bain").str.contains("m2"))
                .then(None)
                .otherwise(pl.col("nombre_de_salle_bain").str.strip_chars())
                .alias("nombre_de_salle_bain")
            ])

            # Nettoyer la superficie (supprimer "m2")
            df4 = df3.with_columns([
                pl.col("superficie").str.replace("m2", "").str.strip_chars().alias("superficie")
            ])

            # ============================
            # 4️⃣  GESTION DU PRIX
            # ============================
            # Nettoyage : suppression des caractères non numériques
            df5 = df4.with_columns([
                pl.col("price").str.replace_all(r"[^0-9]", "").alias("price_clean")
            ])

            # Conversion en numérique
            df6 = df5.with_columns([
                pl.when(pl.col("price_clean") != "")
                .then(pl.col("price_clean").cast(pl.Int64, strict=False))
                .otherwise(None)
                .alias("prix_num")
            ])

            # Calcul de la moyenne pour remplacer les valeurs manquantes
            mean_price = int(df6["prix_num"].mean()) if df6["prix_num"].mean() is not None else 0

            df7 = df6.with_columns([
                pl.when(pl.col("prix_num").is_null())
                .then(pl.lit(mean_price))
                .otherwise(pl.col("prix_num"))
                .alias("price")
            ])

            # ============================
            # 5️⃣  EXTRACTION DE CATEGORY
            # ============================
            df8 = df7.with_columns([
                pl.col("description").str.split(" ").list.first().alias("category")
            ])

            # Supprimer les colonnes inutiles
            df9 = df8.drop(["posted_at", "prix_num", "description", "price_clean"])

            # ============================
            # 6️⃣  CORRECTIONS DE LOCATION / TYPE
            # ============================
            # Correction 1: Location == "Appartements"
            df10 = df9.with_columns([
                pl.when(pl.col("location") == "Appartements")
                .then(pl.col("type"))
                .otherwise(pl.col("location"))
                .alias("location_temp"),
                pl.when(pl.col("location") == "Appartements")
                .then(pl.col("location"))
                .otherwise(pl.col("type"))
                .alias("type")
            ]).drop("location").rename({"location_temp": "location"})

            # Correction 2: Location == "Villas"
            df11 = df10.with_columns([
                pl.when(pl.col("location") == "Villas")
                .then(pl.col("type"))
                .otherwise(pl.col("location"))
                .alias("location")
            ])

            # Correction 3: Location == "Terrains"
            df12 = df11.with_columns([
                pl.when(pl.col("location") == "Terrains")
                .then(pl.col("type"))
                .otherwise(pl.col("location"))
                .alias("location")
            ])

            # ============================
            # 7️⃣  NORMALISATION DE CATEGORY
            # ============================
            df13 = df12.with_columns([
                pl.col("category")
                .str.replace(r"Terrains?", "Terrain")
                .str.replace(r"(?i)locations?", "Location")
                .str.replace(r"Locaton|Location/", "Location")
                .str.replace(r"Terrain|Six|Parcelle|100", "Vente")
                .alias("category")
            ])

            # ============================
            # 8️⃣  NORMALISATION TYPE
            # ============================
            df14 = df13.with_columns([
                pl.col("type")
                .str.replace("appartements", "Appartements")
                .str.replace("villas", "Villas")
                .alias("type")
            ])

            # Filtrer category pour ne garder que Location ou Vente
            df15 = df14.with_columns([
                pl.when((pl.col("category") == "Location") | (pl.col("category") == "Vente"))
                .then(pl.col("category"))
                .otherwise(pl.lit("Vente"))
                .alias("category")
            ])

            # ============================
            # 9️⃣  AJUSTEMENTS TYPE / LOCATION / CITY
            # ============================
            df16 = df15.with_columns([
                pl.when(pl.col("type").str.contains("Sénégal"))
                .then(pl.lit("Appartements"))
                .otherwise(pl.col("type"))
                .alias("type")
            ])

            # Extraction de Area
            df17 = df16.with_columns([
                pl.col("location").str.split(",").list.first().alias("area")
            ])

            # Correction Area pour valeurs aberrantes
            df18 = df17.with_columns([
                pl.when(pl.col("area").str.contains("(?i)(Immeubles|Bureaux & Commerces|Fermes & Vergers|Maisons de vacances|Terrains agricoles|Appartements meublés)"))
                .then(pl.lit("Dakar"))
                .otherwise(pl.col("area"))
                .alias("area")
            ])

            # Extraction de City basée sur Location
            df19 = df18.with_columns([
                pl.when(pl.col("location").str.contains("(?i)Dakar")).then(pl.lit("Dakar"))
                .when(pl.col("location").str.contains("(?i)Thies")).then(pl.lit("Thies"))
                .when(pl.col("location").str.contains("(?i)Saint-Louis")).then(pl.lit("Saint-Louis"))
                .when(pl.col("location").str.contains("(?i)Kaolack")).then(pl.lit("Kaolack"))
                .when(pl.col("location").str.contains("(?i)Ziguinchor")).then(pl.lit("Ziguinchor"))
                .when(pl.col("location").str.contains("(?i)Louga")).then(pl.lit("Louga"))
                .when(pl.col("location").str.contains("(?i)Saly")).then(pl.lit("Saly"))
                .when(pl.col("location").str.contains("(?i)Keur Massar")).then(pl.lit("Keur Massar"))
                .when(pl.col("location").str.contains("(?i)Diamniadio")).then(pl.lit("Diamniadio"))
                .when(pl.col("location").str.contains("(?i)Mbour")).then(pl.lit("Mbour"))
                .when(pl.col("location").str.contains("(?i)Lac rose")).then(pl.lit("Lac rose"))
                .when(pl.col("location").str.contains("(?i)Rufisque")).then(pl.lit("Rufisque"))
                .when(pl.col("location").str.contains("(?i)Ndiass")).then(pl.lit("Ndiass"))
                .when(pl.col("location").str.contains("(?i)Kolda")).then(pl.lit("Kolda"))
                .when(pl.col("location").str.contains("(?i)Ngaparou")).then(pl.lit("Ngaparou"))
                .when(pl.col("location").str.contains("(?i)Niaga")).then(pl.lit("Niaga"))
                .when(pl.col("location").str.contains("(?i)Toubab Dialao")).then(pl.lit("Toubab Dialao"))
                .when(pl.col("location").str.contains("(?i)Mboro")).then(pl.lit("Mboro"))
                .otherwise(pl.lit("Null"))
                .alias("city")
            ])

            # Remplissage aléatoire des zones de Dakar
            choices = ["Almadies", "Yoff", "Ngor", "Point E", "Mamelles"]
            df20 = df19.with_columns([
                pl.when(pl.col("area") == "Dakar")
                .then(
                    pl.when(pl.lit(0).hash() % 5 == 0).then(pl.lit(choices[0]))
                    .when(pl.lit(0).hash() % 5 == 1).then(pl.lit(choices[1]))
                    .when(pl.lit(0).hash() % 5 == 2).then(pl.lit(choices[2]))
                    .when(pl.lit(0).hash() % 5 == 3).then(pl.lit(choices[3]))
                    .otherwise(pl.lit(choices[4]))
                )
                .otherwise(pl.col("area"))
                .alias("area")
            ])

            # Remplacer "Null" par "Dakar" dans City
            df21 = df20.with_columns([
                pl.when(pl.col("city") == "Null")
                .then(pl.lit("Dakar"))
                .otherwise(pl.col("city"))
                .alias("city")
            ]).drop("location")

            # ============================
            # 🔟  REMPLISSAGE VALEURS MANQUANTES
            # ============================
            # Nombre de pièces
            df22 = df21.with_columns([
                pl.when(pl.col("nombre_de_piece").is_null())
                .then(
                    pl.when(pl.lit(0).hash() % 5 == 0).then(pl.lit("4"))
                    .when(pl.lit(0).hash() % 5 == 1).then(pl.lit("3"))
                    .when(pl.lit(0).hash() % 5 == 2).then(pl.lit("2"))
                    .when(pl.lit(0).hash() % 5 == 3).then(pl.lit("5"))
                    .otherwise(pl.lit("1"))
                )
                .otherwise(pl.col("nombre_de_piece"))
                .alias("nombre_de_piece")
            ])

            # Nombre de salles de bain
            df23 = df22.with_columns([
                pl.when(pl.col("nombre_de_salle_bain").is_null())
                .then(
                    pl.when(pl.lit(0).hash() % 5 == 0).then(pl.lit("2"))
                    .when(pl.lit(0).hash() % 5 == 1).then(pl.lit("5"))
                    .when(pl.lit(0).hash() % 5 == 2).then(pl.lit("3"))
                    .when(pl.lit(0).hash() % 5 == 3).then(pl.lit("4"))
                    .otherwise(pl.lit("1"))
                )
                .otherwise(pl.col("nombre_de_salle_bain"))
                .alias("nombre_de_salle_bain")
            ])

            # Superficie
            df24 = df23.with_columns([
                pl.when(pl.col("superficie").is_null())
                .then(
                    pl.when(pl.lit(0).hash() % 5 == 0).then(pl.lit("100"))
                    .when(pl.lit(0).hash() % 5 == 1).then(pl.lit("200"))
                    .when(pl.lit(0).hash() % 5 == 2).then(pl.lit("300"))
                    .when(pl.lit(0).hash() % 5 == 3).then(pl.lit("400"))
                    .otherwise(pl.lit("500"))
                )
                .otherwise(pl.col("superficie"))
                .alias("superficie")
            ])

            # ============================
            # 1️⃣1️⃣  CONVERSION FINALE
            # ============================
            df25 = df24.with_columns([
                pl.col("nombre_de_piece").cast(pl.Int64, strict=False),
                pl.col("nombre_de_salle_bain").cast(pl.Int64, strict=False),
                pl.col("superficie").cast(pl.Int64, strict=False),
                pl.col("price").cast(pl.Int64, strict=False)
            ])

            # Renommer nombre_de_piece en nombre_chambres
            df_clean = df25.rename({"nombre_de_piece": "nombre_chambres"})
            df1_clean = df_clean.rename({"nombre_de_salle_bain": "nombre_sdb"})

            print("\n✅ Aperçu des données transformées :")
            print(df1_clean.head(20))

            print(f"\n📊 Dimensions finales : {df1_clean.height} lignes × {df1_clean.width} colonnes")
            print(f"\n📋 Colonnes finales : {df1_clean.columns}")
        
            return df1_clean
        df_clean = transform_expatdakar_standardized(input_csv)
        output_path = "/tmp/cleaned_coinmarket.csv"
        df_clean.write_csv(output_path)
        print(f"✅ Données nettoyées sauvegardées dans : {output_path}")


    @task
    def merge_datasets():
        """Merge and clean both datasets"""
        import polars as pl
        import pandas as pd
        import logging
        import numpy as np
        
        try:
            df_coin = pl.read_csv('./data/cleaned_data.csv')
            df_expat = pl.read_csv('./data/cleaned_expatdakar.csv')

            print("coinmarket dataset:")
            print(f"Columns: {df_coin.columns}")
            print(f"Shape: {df_coin.shape}")
            print(df_coin.head(2))

            print("\nexpatdakar dataset:")
            print(f"Columns: {df_expat.columns}")
            print(f"Shape: {df_expat.shape}")
            print(df_expat.head(2))

            # Reorder expat dataset columns to match coin dataset
            df_expat_aligned = df_expat.select([
                'price', 'type', 'nombre_chambres', 'nombre_sdb', 'superficie', 'category', 'area', 'city'
            ])

            df_merged = pl.concat([df_coin, df_expat_aligned], how="vertical")

            #---renomage---
            df_rev = df_merged.rename({'type': 'type_bien'})

            # --- Correction area : Point-e → Point E ---
            df_rev = df_rev.with_columns(
                pl.col("area").str.replace_all("Point-e", "Point E").alias("area")
            )

            df_rev = df_rev.with_columns(
                pl.col("city").str.replace("(?i)thiès", "thies", literal=False).alias("city")
            )
            
            
            

            # --- Remplacement aléatoire des "Dakar" ---
            mask_dakar = pl.col("area") == "Dakar"
            other_areas = df_rev.filter(~mask_dakar)["area"].to_list()

            df_rev = df_rev.with_columns([
                pl.when(pl.col("area") == "Dakar")
                .then(pl.lit(np.random.choice(other_areas)))
                .otherwise(pl.col("area"))
                .alias("area")
            ])

            # --- Minuscule + underscores pour toutes les colonnes catégorielles ---
            categorical_cols = [
                col for col, dtype in zip(df_rev.columns, df_rev.dtypes) 
                if dtype == pl.Utf8
            ]

            for col in categorical_cols:
                df_rev = df_rev.with_columns([
                    pl.col(col)
                    .str.to_lowercase()
                    .str.replace_all(" ", "_")
                    .alias(col)
                ])

            #remplacer les superficies d'appartements <20 par la mediane des superfices
            df_rev = df_rev.with_columns(
                pl.when(
                    (pl.col("type_bien") == "appartements") & 
                    (pl.col("superficie") <= 20)
                )
                .then(
                    # Calculer la médiane à la volée (sans les valeurs ≤ 20)
                    df_rev.filter(
                        (pl.col("type_bien") == "appartements") & 
                        (pl.col("superficie") > 20)
                    )["superficie"].median()
                )
                .otherwise(pl.col("superficie"))
                .alias("superficie")
            )

            df_rev = df_rev.with_columns(
                pl.col("area").str.replace("(?i)grand-yoff", "grand_yoff", literal=False).alias("area")
            )

             #remplacer les superficies d'appartements <20 par la mediane des superfices

            df_rev = df_rev.with_columns(
                pl.when(
                    (pl.col("type_bien") == "villas") & 
                    (pl.col("superficie") <= 20)
                )
                .then(
                    # Calculer la médiane à la volée (sans les valeurs ≤ 20)
                    df_rev.filter(
                        (pl.col("type_bien") == "villas") & 
                        (pl.col("superficie") > 20)
                    )["superficie"].median()
                )
                .otherwise(pl.col("superficie"))
                .alias("superficie")
            )

            # Remplacer nombre_chambre > 1 par 1 quand type_bien = "chambre"
            df_rev = df_rev.with_columns(
                pl.when(
                    (pl.col("type_bien") == "chambres") & 
                    (pl.col("nombre_chambres") > 1)
                )
                .then(pl.lit(1))  # Une chambre = 1 chambre
                .otherwise(pl.col("nombre_chambres"))
                .alias("nombre_chambres")
            )

            # Remplacer nombre_sdb > 1 par 1 quand type_bien = "chambre"
            df_rev = df_rev.with_columns(
                pl.when(
                    (pl.col("type_bien") == "chambres") & 
                    (pl.col("nombre_sdb") > 1)
                )
                .then(pl.lit(1))  # Une chambre = 1 chambre
                .otherwise(pl.col("nombre_sdb"))
                .alias("nombre_sdb")
            )

            #remplacer les superficies de chambres < 10 par 15
            df_rev = df_rev.with_columns(
                pl.when(
                    (pl.col("type_bien") == "chambres") & 
                    (pl.col("superficie") < 10)
                )
                .then(pl.lit(15))  # Une chambre = 1 chambre
                .otherwise(pl.col("superficie"))
                .alias("superficie")
            )

            #remplacer les superficies de chambre < 20 par la mediane des superfice
            df_rev = df_rev.with_columns(
            pl.when(
                (pl.col("type_bien") == "chambres") & 
                (pl.col("superficie") > 20)
            )
            .then(
                # Créer un tableau numpy avec des valeurs aléatoires
                pl.Series(np.random.randint(10, 16, size=len(df_rev)))
            )
            .otherwise(pl.col("superficie"))
            .alias("superficie")
        )

            #remplacer les prix de chambre < 50000 par la mediane des prix de chambres
            df_rev = df_rev.with_columns(
                pl.when(
                    (pl.col("type_bien") == "chambres") & 
                    (pl.col("price") < 50000)
                )
                .then(
                    # Calculer la médiane à la volée (sans les valeurs ≤ 20)
                    df_rev.filter(
                        (pl.col("type_bien") == "chambres") & 
                        (pl.col("price") >= 50000)
                    )["price"].median()
                )
                .otherwise(pl.col("price"))
                .alias("price")
            )

            #remplacer les prix de chambre < 100000 par la mediane des prix de chambres
            df_rev = df_rev.with_columns(
                pl.when(
                    (pl.col("type_bien") == "chambres") & 
                    (pl.col("price") > 100000)
                )
                .then(
                    # Calculer la médiane à la volée (sans les valeurs ≤ 20)
                    df_rev.filter(
                        (pl.col("type_bien") == "chambres") & 
                        (pl.col("price") <= 100000)
                    )["price"].median()
                )
                .otherwise(pl.col("price"))
                .alias("price")
            )

            df_rev = df_rev.with_columns(
                pl.col("type_bien").str.replace("(?i)maisons_de_vacances", "villas", literal=False).alias("type_bien")
            )

            df_rev = df_rev.with_columns(
                pl.col("type_bien").str.replace("(?i)immobilier", "villas", literal=False).alias("type_bien")
            )


            #remplacer valeur nombre_chambres et nombre_sdb par 0 pour les type de bien ferme et vergers
            conditions = pl.col("type_bien").str.to_lowercase().str.contains("fermes|vergers")

            df_rev = df_rev.with_columns([
                pl.when(conditions).then(0).otherwise(pl.col("nombre_chambres")).alias("nombre_chambres"),
                pl.when(conditions).then(0).otherwise(pl.col("nombre_sdb")).alias("nombre_sdb")
            ])


            # Remplacer les prix < 10 millions par la médiane des prix >= 10 millions
            df_rev = df_rev.with_columns(
                pl.when(
                    (pl.col("type_bien") == "fermes_&_vergers") & 
                    (pl.col("price") < 10000000)
                )
                .then(
                    # Calculer la médiane des prix >= 10 millions
                    df_rev.filter(
                        (pl.col("type_bien") == "fermes_&_vergers") & 
                        (pl.col("price") >= 10000000)
                    )["price"].median()
                )
                .otherwise(pl.col("price"))
                .alias("price")
            )






            print(df_rev.head(2))

            # --- type_bien: "immeubles" → "terrains" ---
            df_rev = df_rev.with_columns([
                pl.when(pl.col("type_bien") == "immeubles")
                .then(pl.lit("terrains"))  # ← CORRECTION ICI
                .otherwise(pl.col("type_bien"))
                .alias("type_bien")
            ])

            # --- Forcer chambres/SDB = 0 pour les terrains ---
            terrains_list = ['terrains', 'terrains_agricoles', 'terrains_commerciaux']

            df_rev = df_rev.with_columns([
                pl.when(pl.col("type_bien").is_in(terrains_list))
                .then(pl.lit(0))
                .otherwise(pl.col("nombre_chambres"))
                .alias("nombre_chambres"),

                pl.when(pl.col("type_bien").is_in(terrains_list))
                .then(pl.lit(0))
                .otherwise(pl.col("nombre_sdb"))
                .alias("nombre_sdb"),
            ])

            # --- Corrections de valeurs aberrantes ---
            corrections = [
                ('villas', 'superficie', '<', 150),
                ('appartements', 'nombre_chambres', '>', 6),
                ('villas', 'nombre_chambres', '>', 9),
                ('appartements', 'nombre_sdb', '>', 6),
                ('villas', 'nombre_sdb', '>', 9),
                ('bureaux_&_commerces', 'superficie', '<', 20),
            ]

            for type_bien, colonne, operation, seuil in corrections:
                if operation == ">":
                    mask = (pl.col("type_bien") == type_bien) & (pl.col(colonne) > seuil)
                else:
                    mask = (pl.col("type_bien") == type_bien) & (pl.col(colonne) < seuil)

                mediane = df_rev.filter(pl.col("type_bien") == type_bien)[colonne].median()

                df_rev = df_rev.with_columns([
                    pl.when(mask)
                    .then(pl.lit(mediane))
                    .otherwise(pl.col(colonne))
                    .alias(colonne)
                ])

            # --- Séparation location / vente ---
            df_loc = df_rev.filter(pl.col("category") == "location")
            df_vente = df_rev.filter(pl.col("category") == "vente")

            # --- Prix anormaux location (villas < 300k) ---
            median_villas_loc = df_loc.filter(pl.col("type_bien") == "villas")["price"].median()

            df_loc = df_loc.with_columns([
                pl.when((pl.col("type_bien") == "villas") & (pl.col("price") < 300000))
                .then(pl.lit(median_villas_loc))
                .otherwise(pl.col("price"))
                .alias("price")
            ])


            #anoamalies dans les prix des locations
            # remplacer les plus d'appartements > 150 par la median des prix appa loc
            


            df_loc = df_loc.with_columns(
                pl.when(
                    (pl.col("type_bien") == "appartements") & 
                    (pl.col("price") < 100000)
                )
                .then(
                    # Calculer la médiane à la volée (sans les valeurs ≤ 20)
                    df_loc.filter(
                        (pl.col("type_bien") == "appartements") & 
                        (pl.col("price") >= 100000)
                    )["price"].median()
                )
                .otherwise(pl.col("price"))
                .alias("price")
            )

            df_loc = df_loc.with_columns(
                pl.when(
                    (pl.col("type_bien") == "appartements") & 
                    (pl.col("price") > 1500000)
                )
                .then(
                    # CORRECTION: Utiliser df_loc au lieu de df_rev
                    df_loc.filter(
                        (pl.col("type_bien") == "appartements") & 
                        (pl.col("price") <= 1500000)
                    )["price"].median()
                )
                .otherwise(pl.col("price"))
                .alias("price")
            )

            


            # --- Prix anormaux vente (villas < 20M) ---
            median_villas_vente = df_vente.filter(pl.col("type_bien") == "villas")["price"].median()

            df_vente = df_vente.with_columns([
                pl.when((pl.col("type_bien") == "villas") & (pl.col("price") < 20000000))
                .then(pl.lit(median_villas_vente))
                .otherwise(pl.col("price"))
                .alias("price")
            ])

            # --- Prix terrains trop bas (< 5M) ---
            median_terrains = df_vente.filter(
                pl.col("type_bien").is_in(["terrains", "terrains_agricoles", "terrains_commerciaux"])
            )["price"].median()

            df_vente = df_vente.with_columns([
                pl.when(
                    pl.col("type_bien").is_in(["terrains", "terrains_agricoles", "terrains_commerciaux"]) 
                    & (pl.col("price") < 5000000)
                )
                .then(pl.lit(median_terrains))
                .otherwise(pl.col("price"))
                .alias("price")
            ])

            # --- Final merge ---
            df_fin = pl.concat([df_loc, df_vente], how="vertical")

            '''df00 = df_fin.with_columns([
                pl.when(pl.col('superficie') < 20)
                .then(pl.col('superficie').median().over('type_bien'))
                .otherwise(pl.col('superficie'))
                .alias('superficie')
            ])'''
            print("🔄 Conversion appartements meublés → appartements")

            # 1. Marquer et remplacer le type
            df = df_fin.with_columns([
                (pl.col('type_bien') == 'appartements_meublés').alias('_was_meuble'),
                pl.when(pl.col('type_bien') == 'appartements_meublés')
                .then(pl.lit('appartements'))
                .otherwise(pl.col('type_bien'))
                .alias('type_bien')
            ])

            # 2. Créer groupe de similarité
            df = df.with_columns([
                pl.concat_str([
                    pl.col('nombre_chambres').cast(pl.Utf8),
                    pl.col('nombre_sdb').cast(pl.Utf8),
                    pl.col('area'),
                    pl.col('city')
                ], separator='_').alias('_group')
            ])

            # 3. Calculer médianes par groupe (uniquement vrais appartements)
            group_medians = (
                df.filter(~pl.col('_was_meuble'))
                .group_by('_group')  # ← CORRECTION: group_by au lieu de groupby
                .agg([
                    pl.col('price').median().alias('_group_price'),
                    pl.col('superficie').median().alias('_group_sup')
                ])
            )

            # 4. Médianes globales (fallback)
            global_price = df.filter(~pl.col('_was_meuble'))['price'].median()
            global_sup = df.filter(~pl.col('_was_meuble'))['superficie'].median()

            # 5. Joindre et remplacer
            df = df.join(group_medians, on='_group', how='left')

            df = df.with_columns([
                pl.when(pl.col('_was_meuble'))
                .then(pl.coalesce(pl.col('_group_price'), pl.lit(global_price)))
                .otherwise(pl.col('price'))
                .alias('price'),
                
                pl.when(pl.col('_was_meuble'))
                .then(pl.coalesce(pl.col('_group_sup'), pl.lit(global_sup)))
                .otherwise(pl.col('superficie'))
                .alias('superficie')
            ])

            # 6. Statistiques
            n_meubles = df['_was_meuble'].sum()
            n_with_group = df.filter(pl.col('_was_meuble') & pl.col('_group_price').is_not_null()).height
            n_fallback = n_meubles - n_with_group

            print(f"✅ {n_meubles} appartements meublés traités:")
            print(f"   - {n_with_group} avec similaires")
            print(f"   - {n_fallback} avec médiane globale")

            # 7. Nettoyer
            df = df.drop(['_was_meuble', '_group', '_group_price', '_group_sup'])

            print("✅ Remplacement terminé!")

            # 8. Assigner à df_fin pour la suite
            df_fin = df

            df_final = (
                df_fin
                .unique()
                .filter(pl.col("price") > 0)
                .filter(pl.col("superficie") > 0)
                .filter(pl.col("nombre_chambres") >= 0)
                .filter(pl.col("nombre_sdb") >= 0)
            )
            
            
            

            output_path = "./data/joined_cleaned_data.csv"
            df_final.write_csv(output_path)
            
            logging.info(f"✅ Merged dataset saved: {df_final.height} rows")
            logging.info(f"📊 Final columns: {df_final.columns}")
            print(f"Final dataset shape: {df_final.shape}")
            print(df_final.head(10))
            print("df shape after merging:")

            
            if "source" in df_final.columns:
                stats = df_final.group_by("source").agg([
                    pl.count().alias("count"),
                    pl.col("price").mean().alias("avg_price"),
                    pl.col("superficie").mean().alias("avg_superficie")
                ])
                logging.info(f"📈 Distribution by source:\n{stats}")
            
            if df_final.height == 0:
                raise ValueError("No data after cleaning - check data quality")
                
            return True
            
        except Exception as e:
            logging.error(f"❌ Error merging datasets: {e}")
            raise
    
    @task
    def upload_join_to_sqlite():
        """Upload joined data to SQLite database - creates 'realestate' table"""
        import pandas as pd
        import sqlite3
        import os
        
        # Chemin vers la base de données SQLite
        db_path = './data/immobilier.db'
        
        # Chemin vers les données jointes
        file_path = './data/joined_cleaned_data.csv'
        
        # Vérifier si c'est un fichier ou un répertoire
        if os.path.isdir(file_path):
            csv_files = [f for f in os.listdir(file_path) if f.endswith('.csv')]
            if csv_files:
                actual_file = os.path.join(file_path, csv_files[0])
                file_path = actual_file
            else:
                logging.error(f"❌ No CSV files found in directory {file_path}")
                return False
        
        try:
            # Lire le CSV
            df = pd.read_csv(file_path)
            logging.info(f"✅ Data read from {file_path}, shape: {df.shape}")
            
            # Connexion à SQLite
            try:
                conn = sqlite3.connect(db_path)
                logging.info(f"🔗 Connected to SQLite database at {db_path}")
                cursor = conn.cursor()
            except Exception as e:
                logging.error(f"❌ Error connecting to SQLite: {e}")
                return False
            

            cursor.execute("DROP TABLE IF EXISTS realestate")
            # Créer la table 'realestate' avec le bon schéma
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS realestate (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    price INTEGER,
                    type_bien TEXT,
                    superficie INTEGER,
                    nombre_chambres INTEGER,
                    nombre_sdb INTEGER,
                    category TEXT,
                    area TEXT,
                    city TEXT
                    
                )
            """)
            
            # Supprimer les données existantes et insérer les nouvelles
            cursor.execute("DELETE FROM realestate")
            
            # Préparer les données pour l'insertion
            df_clean = df.copy()
            
            # Nettoyer et convertir les types de données
            if 'price' in df_clean.columns:
                df_clean['price'] = pd.to_numeric(df_clean['price'], errors='coerce').fillna(0).astype(int)
            if 'superficie' in df_clean.columns:
                df_clean['superficie'] = pd.to_numeric(df_clean['superficie'], errors='coerce').fillna(0).astype(int)
            if 'nombre_chambres' in df_clean.columns:
                df_clean['nombre_chambres'] = pd.to_numeric(df_clean['nombre_chambres'], errors='coerce').fillna(0).astype(int)
            if 'nombre_sdb' in df_clean.columns:
                df_clean['nombre_sdb'] = pd.to_numeric(df_clean['nombre_sdb'], errors='coerce').fillna(0).astype(int)
            
            # Remplir les valeurs manquantes pour les colonnes texte
            text_columns = ['type_bien', 'category', 'area', 'city']
            for col in text_columns:
                if col in df_clean.columns:
                    df_clean[col] = df_clean[col].fillna('Unknown')
                else:
                    df_clean[col] = 'Unknown'
            
            # Insérer les données
            df_clean.to_sql('realestate', conn, if_exists='append', index=False)
            
            # Vérifier les données insérées
            cursor.execute("SELECT COUNT(*) FROM realestate")
            count = cursor.fetchone()[0]
            logging.info(f"✅ {count} records inserted into SQLite table 'realestate'")
            
            # Afficher un aperçu des données
            cursor.execute("SELECT * FROM realestate LIMIT 5")
            rows = cursor.fetchall()
            logging.info(f"✅ Preview of SQLite data: {len(rows)} rows shown")
            
            # Afficher les colonnes de la table
            cursor.execute("PRAGMA table_info(realestate)")
            columns_info = cursor.fetchall()
            logging.info(f"✅ Table 'realestate' schema: {[col[1] for col in columns_info]}")
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            logging.error(f"❌ Error uploading to SQLite: {e}")
            return False
    @task
    def train_model_after_etl():
        """
        Entraîne automatiquement le modèle ML juste après l’ETL, incluant
        l'optimisation des hyperparamètres par différentes méthodes (Grid, Random, Optuna).
        Sauvegarde UNIQUEMENT le modèle du MEILLEUR optimiseur.
        """
        import pandas as pd
        import numpy as np
        import sqlite3
        import joblib
        import os
        # Désactiver les logs verbeux de XGBoost
        os.environ["XGBOOST_DISABLE_STDLOG"] = "1"
        import json
        import logging
        import time
        import warnings

        from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV, RandomizedSearchCV
        from sklearn.preprocessing import OneHotEncoder
        from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
        from xgboost import XGBRegressor

        # Optional: Bayesian optimization
        try:
            import optuna
        except Exception as e:
            optuna = None
            logging.warning("optuna not available. Optuna optimization will be skipped.")

        warnings.filterwarnings("ignore")
        logging.basicConfig(level=logging.INFO)

        # ------------------------------------------------------------
        # 1️⃣ CHARGEMENT DES DONNÉES
        # ------------------------------------------------------------
        db_path = "../data/immobilier.db"
        if not os.path.exists(db_path):
            db_path = "./data/immobilier.db"
            if not os.path.exists(db_path):
                raise FileNotFoundError(
                    "❌ Base SQLite introuvable. Chemins vérifiés : ../data/immobilier.db et ./data/immobilier.db"
                )

        conn = sqlite3.connect(db_path)
        df = pd.read_sql("SELECT * FROM realestate", conn)
        conn.close()

        if df.empty:
            raise ValueError("❌ Aucune donnée dans la base.")

        logging.info(f"✅ Données chargées : {df.shape}")

        # ------------------------------------------------------------
        # 2️⃣ PRÉTRAITEMENTS
        # ------------------------------------------------------------
        df = df.copy()

        # Renommages et corrections basiques
        if 'type' in df.columns and 'type_bien' not in df.columns:
            df.rename(columns={'type': 'type_bien'}, inplace=True)

        if 'area' in df.columns:
            df['area'] = df['area'].replace("Point-e", "Point E")

        if 'city' in df.columns:
            df['city'] = df['city'].replace("thiès", "thies")

        # Gestion des zones génériques "Dakar"
        if 'area' in df.columns:
            mask_dakar = df['area'] == 'Dakar'
            other_areas = df.loc[~mask_dakar, 'area'].dropna().unique()
            if len(other_areas) > 0 and mask_dakar.sum() > 0:
                df.loc[mask_dakar, 'area'] = np.random.choice(other_areas, size=mask_dakar.sum())

        # Nettoyage texte
        categorical_cols = df.select_dtypes(include=[object]).columns.tolist()
        for col in categorical_cols:
            df[col] = df[col].astype(str).str.lower().str.replace(" ", "_")

        # Terrains -> 0 chambres/sdb
        mask_terr = df["type_bien"].isin([
            "terrains", "terrains_agricoles", "terrains_commerciaux", "fermes_&_vergers"
        ])
        if 'nombre_chambres' in df.columns and 'nombre_sdb' in df.columns:
            df.loc[mask_terr, ["nombre_chambres", "nombre_sdb"]] = 0

        # Remplacements basiques
        if 'type_bien' in df.columns:
            df['type_bien'] = df['type_bien'].replace({
                'appartements_meublés': 'appartements',
                'maisons_de_vacances': 'villas',
                'immobilier': 'villas',
                'immeubles': 'terrains'
            })

        # Nettoyage doublons et valeurs nulles/négatives
        df = df.drop_duplicates()
        if 'price' in df.columns:
            df = df[df['price'] > 0]
        if 'superficie' in df.columns:
            df = df[df['superficie'] > 0]

        # Filtre types inconnus
        if 'type_bien' in df.columns:
            df = df[~df['type_bien'].isin(['unknown', 'immobilier', 'terrains'])]

        # ------------------------------------------------------------
        # 3️⃣ SPLIT LOCATION / VENTE
        # ------------------------------------------------------------
        df_loc = df[df['category'] == 'location'].copy() if 'category' in df.columns else df.copy()
        df_vente = df[df['category'] == 'vente'].copy() if 'category' in df.columns else df.copy()

        # ------------------------------------------------------------
        # 4️⃣ GESTION PRIX ABERRANTS (Remplacement par mediane)
        # ------------------------------------------------------------
        def replace_price_by_median_if_condition(df_group, cond_mask, col='price'):
            if cond_mask.any():
                med = df_group.loc[~cond_mask, col].median()
                df_group.loc[cond_mask, col] = med
            return df_group

        if not df_loc.empty and 'type_bien' in df_loc.columns and 'price' in df_loc.columns:
            mask_low = (df_loc['type_bien'] == 'villas') & (df_loc['price'] < 300000)
            df_loc = replace_price_by_median_if_condition(df_loc, mask_low, 'price')

        if not df_vente.empty and 'type_bien' in df_vente.columns and 'price' in df_vente.columns:
            mask_low_v = (df_vente['type_bien'] == 'villas') & (df_vente['price'] < 20000000)
            df_vente = replace_price_by_median_if_condition(df_vente, mask_low_v, 'price')

            mask_terrains_bas = df_vente['type_bien'].isin(["terrains", "terrains_agricoles", "terrains_commerciaux"]) & (df_vente['price'] < 5000000)
            df_vente = replace_price_by_median_if_condition(df_vente, mask_terrains_bas, 'price')

        # ------------------------------------------------------------
        # 5️⃣ OUTLIERS - CAPPING (IQR)
        # ------------------------------------------------------------
        def impute_outliers_group(df_group, feature):
            if feature not in df_group.columns or df_group[feature].dropna().empty:
                return df_group
            q1 = df_group[feature].quantile(0.25)
            q3 = df_group[feature].quantile(0.75)
            iqr = q3 - q1
            low = q1 - 1.5 * iqr
            high = q3 + 1.5 * iqr
            df_group.loc[df_group[feature] < low, feature] = low
            df_group.loc[df_group[feature] > high, feature] = high
            return df_group

        for feature in ["price", "superficie", "nombre_chambres", "nombre_sdb"]:
            if feature in df_loc.columns:
                df_loc = df_loc.groupby('type_bien', group_keys=False).apply(lambda g: impute_outliers_group(g, feature))
            if feature in df_vente.columns:
                df_vente = df_vente.groupby('type_bien', group_keys=False).apply(lambda g: impute_outliers_group(g, feature))

        logging.info("✅ Outliers traités (group-wise capping)")
        print(f"📍 Locations: {df_loc.shape[0]} annonces")
        print(f"💰 Ventes: {df_vente.shape[0]} annonces")

        # ------------------------------------------------------------
        # 6️⃣ FEATURE ENGINEERING
        # ------------------------------------------------------------
        def prepare_features(df_in):
            df = df_in.copy()
            
            # FillNa pour division safe
            for col in ['nombre_chambres', 'nombre_sdb']:
                if col in df.columns: df[col] = df[col].fillna(0)
                else: df[col] = 0
            
            if 'superficie' in df.columns: df['superficie'] = df['superficie'].fillna(0.0)
            else: df['superficie'] = 0.0

            # Ratios
            df['ratio_sdb_chambres'] = df['nombre_sdb'] / (df['nombre_chambres'] + 1)
            df['surface_par_chambre'] = df['superficie'] / (df['nombre_chambres'] + 1)
            df['total_pieces'] = df['nombre_chambres'] + df['nombre_sdb']
            df['density'] = df['nombre_chambres'] / (df['superficie'] + 1)

            # Transformations mathématiques
            df['log_superficie'] = np.log1p(df['superficie'])
            df['sqrt_superficie'] = np.sqrt(df['superficie'])
            df['superficie_squared'] = df['superficie'] ** 2

            # Booléens
            premium_areas = ['almadies', 'ngor', 'mermoz', 'sacré-coeur', 'fann']
            df['is_premium_area'] = df['area'].isin(premium_areas).astype(int) if 'area' in df.columns else 0
            df['is_dakar'] = (df['city'] == 'dakar').astype(int) if 'city' in df.columns else 0
            df['is_villa'] = (df['type_bien'] == 'villas').astype(int) if 'type_bien' in df.columns else 0
            df['is_location'] = (df['category'] == 'location').astype(int) if 'category' in df.columns else 0
            
            # Spécifiques
            df['villa_large'] = ((df['type_bien'] == 'villas') & (df['superficie'] > 200)).astype(int) if 'type_bien' in df.columns else 0
            df['appt_petit'] = ((df['type_bien'] == 'appartements') & (df['superficie'] < 80)).astype(int) if 'type_bien' in df.columns else 0
            df['high_bathroom_ratio'] = (df['nombre_sdb'] >= df['nombre_chambres']).astype(int)
            df['spacious'] = (df['surface_par_chambre'] > 40).astype(int)

            return df

        df_loc_prepared = prepare_features(df_loc)
        df_vente_prepared = prepare_features(df_vente)

        logging.info("✅ Feature engineering terminé")

        # ------------------------------------------------------------
        # 7️⃣ PREPARE MODEL DATA
        # ------------------------------------------------------------
        def prepare_model_data(df):
            df = df.copy()
            numerical_cols = [
                'superficie', 'nombre_chambres', 'nombre_sdb',
                'ratio_sdb_chambres', 'surface_par_chambre', 'total_pieces', 'density',
                'log_superficie', 'sqrt_superficie', 'superficie_squared',
                'is_premium_area', 'is_dakar', 'is_villa', 'is_location',
                'villa_large', 'appt_petit', 'high_bathroom_ratio', 'spacious'
            ]
            categorical_cols = ["type_bien", "area", "city", "category"]

            # Filtrer colonnes existantes
            numerical_cols = [c for c in numerical_cols if c in df.columns]
            categorical_cols = [c for c in categorical_cols if c in df.columns]

            X_num = df[numerical_cols].reset_index(drop=True) if numerical_cols else pd.DataFrame(index=df.index)
            X_cat = df[categorical_cols].fillna("missing").reset_index(drop=True) if categorical_cols else pd.DataFrame()

            encoder = None
            if categorical_cols:
                encoder = OneHotEncoder(sparse_output=False, drop="first", handle_unknown="ignore")
                X_cat_encoded = encoder.fit_transform(X_cat)
                feature_names = encoder.get_feature_names_out(categorical_cols)
                X_cat_df = pd.DataFrame(X_cat_encoded, columns=feature_names)
                X = pd.concat([X_num.reset_index(drop=True), X_cat_df.reset_index(drop=True)], axis=1)
            else:
                X = X_num

            y = df["price"].reset_index(drop=True) if 'price' in df.columns else pd.Series([])

            return X, y, encoder

        X_loc, y_loc, encoder_loc = prepare_model_data(df_loc_prepared)
        X_vente, y_vente, encoder_vente = prepare_model_data(df_vente_prepared)

        print(f"📍 Location: {X_loc.shape[1]} features, {X_loc.shape[0]} échantillons")
        print(f"💰 Vente: {X_vente.shape[1]} features, {X_vente.shape[0]} échantillons")

        # ------------------------------------------------------------
        # 8️⃣ OPTIMIZATION HELPERS
        # ------------------------------------------------------------
        def optimize_grid_search(X, y, task_type):
            print("\n🔍 GRID SEARCH...")
            param_grid = {
                "n_estimators": [200, 400, 600],
                "max_depth": [3, 4, 5],
                "learning_rate": [0.005, 0.01, 0.02],
                "subsample": [0.6, 0.8, 1.0],
                "colsample_bytree": [0.6, 0.8, 1.0]
            }
            model = XGBRegressor(eval_metric="rmse", random_state=42, n_jobs=-1, verbosity=0)
            gs = GridSearchCV(model, param_grid, scoring="r2", cv=3, n_jobs=-1, verbose=0)
            gs.fit(X, y)
            return gs.best_estimator_, gs.best_params_, gs.best_score_

        def optimize_random_search(X, y, task_type, n_iter=40):
            print("\n🎲 RANDOM SEARCH...")
            param_dist = {
                "n_estimators": np.arange(200, 1001, 100),
                "max_depth": np.arange(2, 9),
                "learning_rate": np.linspace(0.005, 0.05, 10),
                "subsample": np.linspace(0.5, 1.0, 6),
                "colsample_bytree": np.linspace(0.5, 1.0, 6),
                "gamma": np.linspace(0, 1, 10),
                "min_child_weight": np.arange(1, 20)
            }
            model = XGBRegressor(eval_metric="rmse", random_state=42, n_jobs=-1, verbosity=0)
            rs = RandomizedSearchCV(model, param_dist, scoring="r2", cv=3, n_jobs=-1, n_iter=n_iter, verbose=0, random_state=42)
            rs.fit(X, y)
            return rs.best_estimator_, rs.best_params_, rs.best_score_

        def optimize_bayesian_optuna(X, y, task_type, n_trials=40):
            if optuna is None:
                logging.warning("Optuna non installé.")
                return None, None, None
            print("\n🤖 OPTUNA BAYESIAN OPTIMIZATION...")

            def objective(trial):
                params = {
                    "n_estimators": trial.suggest_int("n_estimators", 300, 1000),
                    "max_depth": trial.suggest_int("max_depth", 3, 8),
                    "learning_rate": trial.suggest_float("learning_rate", 0.005, 0.05),
                    "subsample": trial.suggest_float("subsample", 0.5, 1.0),
                    "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
                    "gamma": trial.suggest_float("gamma", 0.0, 1.0),
                    "min_child_weight": trial.suggest_int("min_child_weight", 1, 20),
                    "reg_alpha": trial.suggest_float("reg_alpha", 0.0, 5.0),
                    "reg_lambda": trial.suggest_float("reg_lambda", 0.1, 10.0),
                }
                model = XGBRegressor(**params, eval_metric="rmse", random_state=42, n_jobs=-1, verbosity=0)
                scores = cross_val_score(model, X, y, scoring="r2", cv=3, n_jobs=-1)
                return float(scores.mean())

            study = optuna.create_study(direction="maximize")
            study.optimize(objective, n_trials=n_trials, show_progress_bar=False)
            best_params = study.best_params
            best_score = study.best_value
            best_model = XGBRegressor(**best_params, eval_metric="rmse", random_state=42, n_jobs=-1, verbosity=0)
            best_model.fit(X, y)
            return best_model, best_params, best_score

        # ------------------------------------------------------------
        # 9️⃣ TRAIN + OPTIMIZE PIPELINE
        # ------------------------------------------------------------
        def train_and_optimize_all(X, y, encoder, dataset_name, output_dir="./data/models"):
            os.makedirs(output_dir, exist_ok=True)

            if X.shape[0] < 30:
                logging.warning(f"Dataset {dataset_name} trop petit. Skip.")
                return None

            # 1) Try all optimizers
            methods = {}
            try:
                m_gs, p_gs, s_gs = optimize_grid_search(X, y, dataset_name)
                methods["grid"] = (m_gs, p_gs, s_gs)
            except Exception as e:
                logging.exception(f"Grid search failed: {e}")

            try:
                m_rs, p_rs, s_rs = optimize_random_search(X, y, dataset_name)
                methods["random"] = (m_rs, p_rs, s_rs)
            except Exception as e:
                logging.exception(f"Random search failed: {e}")

            try:
                m_opt, p_opt, s_opt = optimize_bayesian_optuna(X, y, dataset_name)
                methods["optuna"] = (m_opt, p_opt, s_opt)
            except Exception as e:
                logging.exception(f"Optuna failed: {e}")

            # Evaluate on Hold-out Test set
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, shuffle=True)

            def eval_model(m):
                y_pred = m.predict(X_test)
                r2 = r2_score(y_test, y_pred)
                mae = mean_absolute_error(y_test, y_pred)
                rmse = np.sqrt(mean_squared_error(y_test, y_pred))
                return r2, mae, rmse

            # Select Best Model based on Hold-out R2
            best_method = None
            best_pack = None
            best_r2_test = -np.inf
            best_eval_metrics = (None, None, None)

            for m_name, (model_obj, params_obj, _) in methods.items():
                if model_obj is None: continue
                
                curr_r2, curr_mae, curr_rmse = eval_model(model_obj)
                
                if curr_r2 > best_r2_test:
                    best_r2_test = curr_r2
                    best_method = m_name
                    best_pack = (model_obj, params_obj)
                    best_eval_metrics = (curr_r2, curr_mae, curr_rmse)

            if best_pack is None:
                logging.error(f"Aucun modèle valide pour {dataset_name}.")
                return None

            # Unpack winner
            best_model_obj, best_params_obj = best_pack
            final_r2, final_mae, final_rmse = best_eval_metrics

            # Compute CV Mean on full data for metrics
            try:
                cv_scores = cross_val_score(best_model_obj, X, y, cv=3, scoring="r2", n_jobs=-1)
                cv_mean = float(cv_scores.mean())
                cv_std = float(cv_scores.std())
            except:
                cv_mean, cv_std = None, None

            # Filenames
            base_name = f"{dataset_name.lower()}_{best_method}_optimized"
            model_file = os.path.join(output_dir, f"{base_name}_model.pkl")
            encoder_file = os.path.join(output_dir, f"{base_name}_encoder.pkl")
            features_file = os.path.join(output_dir, f"{base_name}_features.json")
            metrics_file = os.path.join(output_dir, f"{base_name}_metrics.json")
            importance_file = os.path.join(output_dir, f"{base_name}_importance.csv")

            # Save Artifacts
            joblib.dump(best_model_obj, model_file)
            if encoder:
                joblib.dump(encoder, encoder_file)
            else:
                joblib.dump(None, encoder_file)

            # Feature Importance
            try:
                imp_df = pd.DataFrame({
                    "feature": X.columns,
                    "importance": best_model_obj.feature_importances_
                }).sort_values("importance", ascending=False)
                imp_df.to_csv(importance_file, index=False)
            except: pass

            # Save Features List
            try:
                with open(features_file, "w") as f:
                    json.dump({"features": X.columns.tolist()}, f, indent=2)
            except: pass

            # ============================================================
            # ✨ SAVE METRICS (CORRECTION DU TYPAGE JSON)
            # ============================================================
            
            # Nettoyage des paramètres (conversion numpy -> types natifs pour JSON)
            safe_params = {}
            if isinstance(best_params_obj, dict):
                for k, v in best_params_obj.items():
                    if isinstance(v, (np.integer, int)):
                        safe_params[k] = int(v)
                    elif isinstance(v, (np.floating, float)):
                        safe_params[k] = float(v)
                    else:
                        safe_params[k] = v

            metrics = {
                "dataset": dataset_name,
                "best_method": best_method,
                # Casting explicite en float pour éviter l'erreur de sérialisation JSON
                "test_r2": float(final_r2) if final_r2 is not None else None,
                "test_mae": float(final_mae) if final_mae is not None else None,
                "test_rmse": float(final_rmse) if final_rmse is not None else None,
                "cv_mean": float(cv_mean) if cv_mean is not None else None,
                "cv_std": float(cv_std) if cv_std is not None else None,
                "params": safe_params,
                "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
            }

            with open(metrics_file, "w") as f:
                json.dump(metrics, f, indent=4)

            logging.info(f"💾 Modèle sauvegardé : {model_file} (R2={final_r2:.4f})")

            return {
                "dataset": dataset_name,
                "best_method": best_method,
                "best_r2_test": float(final_r2)
            }

        # ------------------------------------------------------------
        # 10️⃣ EXECUTION
        # ------------------------------------------------------------
        models_output_dir = "./data/models"
        os.makedirs(models_output_dir, exist_ok=True)

        loc_res = None
        if X_loc.shape[0] >= 30:
            loc_res = train_and_optimize_all(X_loc, y_loc, encoder_loc, "location", models_output_dir)

        vente_res = None
        if X_vente.shape[0] >= 30:
            vente_res = train_and_optimize_all(X_vente, y_vente, encoder_vente, "vente", models_output_dir)

        print("\n" + "="*80)
        print("🏁 RAPPORT FINAL")
        print("="*80)
        if loc_res: print(f"📍 LOCATION: {loc_res['best_method']} | R2: {loc_res['best_r2_test']:.4f}")
        if vente_res: print(f"💰 VENTE:    {vente_res['best_method']} | R2: {vente_res['best_r2_test']:.4f}")

        return {"status": "success", "loc": loc_res, "vente": vente_res}

    # Définir l'ordre d'exécution
    coinmarket = scrape_coinmarket()
    expatdakar = scrape_expatdakar()
    transform1 = transform_source1()
    transform2 = transform_source2()
    merged = merge_datasets()
    upload = upload_join_to_sqlite()
    model_training = train_model_after_etl()

    # Définir les dépendances
    coinmarket >> transform1
    expatdakar >> transform2
    [transform1, transform2] >> merged >> upload >> model_training