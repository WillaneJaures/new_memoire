from airflow import DAG
from airflow.decorators import task
import os
from datetime import datetime, timedelta
import logging
import polars as pl 

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
        for i in range(1, 444):  
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
        import logging
        
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
            
            # Based on your logs, the datasets have different column structures
            # Let's align them properly
            
            # Coin dataset columns from your log: 
            # ['price', 'type', 'nombre_chambres', 'nombre_sdb', 'superficie', 'category', 'area', 'city']
            
            # Expat dataset columns from your log:
            # ['price', 'type', 'superficie', 'nombre_chambres', 'nombre_sdb', 'category', 'area', 'city']
            
            # Reorder expat dataset columns to match coin dataset
            df_expat_aligned = df_expat.select([
                'price', 'type', 'nombre_chambres', 'nombre_sdb', 'superficie', 'category', 'area', 'city'
            ])
            
            df_merged = pl.concat([df_coin, df_expat_aligned], how="vertical")
            
            df_final = (
                df_merged
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
            
            # Créer la table 'realestate' avec le bon schéma
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS realestate (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    price INTEGER,
                    type TEXT,
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
            text_columns = ['type', 'category', 'area', 'city']
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
        Entraîne automatiquement le modèle ML juste après la fin de l'ETL
        (upload_join_to_sqlite). Sauvegarde le modèle et les objets preprocessing.
        """
        import pandas as pd
        import numpy as np
        import sqlite3
        import joblib
        import os
        from sklearn.model_selection import train_test_split, cross_val_score
        from sklearn.preprocessing import StandardScaler, OneHotEncoder
        from sklearn.ensemble import GradientBoostingRegressor
        from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
        from datetime import datetime

        logging.info("🚀 Début de l'entraînement du modèle ML")

        db_path = './data/immobilier.db'
        if not os.path.exists(db_path):
            raise FileNotFoundError(f" Base SQLite introuvable : {db_path}")

        # Chargement des données
        conn = sqlite3.connect(db_path)
        df = pd.read_sql("SELECT * FROM realestate", conn)
        conn.close()

        if df.empty:
            raise ValueError(" Aucune donnée trouvée pour l'entraînement du modèle")

        # Préparation des données
        # Supprimer colonnes inutiles
        df = df.drop(columns=['id'], errors='ignore')

        # Supprimer doublons
        df = df.drop_duplicates()

        print("\n")
        print("verifivation valeur manquante")
        print(df.isnull().sum())

        # CORRECTION : Gestion sécurisée des petites superficies
        def safe_correct_superficie(df, property_type, min_superficie=50):
            """Corrige les petites superficies de manière sécurisée"""
            # Vérifier si le type existe dans les données
            if property_type not in df['type'].unique():
                print(f"⚠️  Type '{property_type}' non trouvé - ignoré")
                return df
                
            mask_small = (df['type'] == property_type) & (df['superficie'] < min_superficie)
            count_small = mask_small.sum()
            
            if count_small == 0:
                return df
                
            # Calculer la moyenne uniquement sur les données valides
            valid_data = df[(df['type'] == property_type) & (df['superficie'] >= min_superficie)]['superficie']
            
            if len(valid_data) == 0:
                # Si pas de données valides, utiliser la médiane globale
                mean_value = df['superficie'].median()
                print(f"⚠️  Aucune donnée valide pour {property_type}, utilisation médiane globale: {mean_value}")
            else:
                mean_value = valid_data.mean()
                print(f"📊 {property_type}: {count_small} corrections avec moyenne {mean_value:.1f}")
            
            # CORRECTION : Utiliser .round() directement sans conversion
            df.loc[mask_small, 'superficie'] = round(mean_value)
            return df

        # Appliquer les corrections de manière sécurisée
        df = safe_correct_superficie(df, 'appartements')
        df = safe_correct_superficie(df, 'villas') 
        df = safe_correct_superficie(df, 'appartements_meublés')

        # Uniformiser noms de colonnes
        df.columns = df.columns.str.lower().str.replace(' ', '_')

        # Supprimer valeurs aberrantes dans type
        df = df[~df['type'].isin(['Unknown', 'Immobilier', 'unknown', 'immobilier'])]

        # Uniformiser valeurs catégorielles
        strings = list(df.dtypes[df.dtypes == 'object'].index)
        for col in strings:
            df[col] = df[col].str.lower().str.replace(' ', '_')

        # CORRECTION : Renommer les valeurs pour uniformiser
        df['type'] = df['type'].replace({
            'appartement': 'appartements',
            'villa': 'villas',
        })
        
        # CORRECTION : Gérer les valeurs manquantes avant le traitement des outliers
        df = df.dropna(subset=['price', 'superficie'])

        # Traiter outliers avec IQR
        features = ['price', 'superficie', 'nombre_chambres', 'nombre_sdb']

        def impute_outliers(df, feature):
            # CORRECTION : Vérifier que la feature existe et a des données
            if feature not in df.columns or df[feature].isna().all():
                return
                
            clean_data = df[feature].dropna()
            if len(clean_data) == 0:
                return
                
            q1 = np.percentile(clean_data, 25)
            q3 = np.percentile(clean_data, 75) 
            iqr = q3 - q1
            
            # Éviter les cas où iqr = 0
            if iqr == 0:
                return
                
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            
            # Compter les outliers corrigés
            outliers_before = ((df[feature] < lower_bound) | (df[feature] > upper_bound)).sum()
            
            df.loc[df[feature] < lower_bound, feature] = lower_bound
            df.loc[df[feature] > upper_bound, feature] = upper_bound
            
            outliers_after = ((df[feature] < lower_bound) | (df[feature] > upper_bound)).sum()
            
            if outliers_before > 0:
                print(f"📊 {feature}: {outliers_before} outliers traités")

        for feature in features:
            if feature in df.columns:
                impute_outliers(df, feature)
        
        # CORRECTION : Gérer les divisions par zéro
        df['nombre_chambres'] = df['nombre_chambres'].fillna(0)
        df['nombre_sdb'] = df['nombre_sdb'].fillna(0)
        
        # Créer nouvelles features (sans utiliser la cible)
        df['ratio_sdb_chambres'] = df['nombre_sdb'] / (df['nombre_chambres'].replace(0, 1) + 1e-10)
        df['surface_par_chambre'] = df['superficie'] / (df['nombre_chambres'].replace(0, 1) + 1e-10)

        # Log transform pour les distributions asymétriques
        df['log_price'] = np.log1p(df['price'])
        df['log_superficie'] = np.log1p(df['superficie'])

        # ============================
        # ENCODAGE ONEHOT
        # ============================

        categorical_cols = ['type', 'category', 'area', 'city']
        numerical_cols = ['superficie', 'nombre_chambres', 'nombre_sdb', 
                        'ratio_sdb_chambres', 'surface_par_chambre',
                        'log_superficie']

        # CORRECTION : Sélectionner uniquement les colonnes existantes
        categorical_cols = [col for col in categorical_cols if col in df.columns]
        numerical_cols = [col for col in numerical_cols if col in df.columns]

        print(f"🔧 Colonnes utilisées - Catégorielles: {categorical_cols}")
        print(f"🔧 Colonnes utilisées - Numériques: {numerical_cols}")

        X_num = df[numerical_cols]
        X_cat = df[categorical_cols]

        # OneHotEncoder
        encoder = OneHotEncoder(sparse_output=False, drop='first', handle_unknown='ignore')
        X_cat_encoded = encoder.fit_transform(X_cat)
        feature_names = encoder.get_feature_names_out(categorical_cols)

        X_cat_df = pd.DataFrame(X_cat_encoded, columns=feature_names, index=df.index)
        X = pd.concat([X_num.reset_index(drop=True), X_cat_df.reset_index(drop=True)], axis=1)
        y = df['price'].reset_index(drop=True)  # Garder y non normalisé
        
        preprocessing_info = {
            'numerical_cols': numerical_cols,
            'categorical_cols': categorical_cols,
            'feature_names': feature_names.tolist(),
            'all_columns': X.columns.tolist()
        }

        # ============================
        # SPLIT TRAIN/TEST
        # ============================

        # CORRECTION : Gérer le cas où 'category' n'existe pas
        stratify_col = None
        if 'category' in df.columns:
            stratify_col = df['category'].reset_index(drop=True)
            print("🎯 Utilisation de la stratification par category")
        else:
            print("⚠️  Colonne 'category' non trouvée - pas de stratification")

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, 
            test_size=0.2, 
            random_state=42, 
            stratify=stratify_col
        )

        print(f"📊 Données d'entraînement: {X_train.shape[0]} échantillons")
        print(f"📊 Données de test: {X_test.shape[0]} échantillons")

        model = GradientBoostingRegressor(
            n_estimators=300,       
            max_depth=4,           
            learning_rate=0.05,      
            subsample=0.8,           
            min_samples_split=20,
            min_samples_leaf=10,
            random_state=42
        )
        
        model.fit(X_train, y_train)
        
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)
        
        train_r2 = r2_score(y_train, y_train_pred)
        test_r2 = r2_score(y_test, y_test_pred)
        test_mae = mean_absolute_error(y_test, y_test_pred)
        test_rmse = np.sqrt(mean_squared_error(y_test, y_test_pred))

        cv_scores = cross_val_score(model, X_train, y_train, cv=5, scoring='r2')

        metrics = {
            'train_r2': float(train_r2),
            'test_r2': float(test_r2),
            'test_mae': float(test_mae),
            'test_rmse': float(test_rmse),
            'overfitting': float(train_r2 - test_r2),
            'cv_mean': float(cv_scores.mean()),
            'cv_std': float(cv_scores.std()),
            'timestamp': datetime.now().isoformat()
        }
        # Sauvegarde du modèle
        models_dir = './data/models'
        os.makedirs(models_dir, exist_ok=True)
        joblib.dump(model, f'{models_dir}/best_model.pkl')
        joblib.dump(metrics, f'{models_dir}/metrics.pkl')
        joblib.dump(encoder, f'{models_dir}/encoder.pkl')
        joblib.dump(preprocessing_info, f'{models_dir}/preprocessing_info.pkl')
        #joblib.dump(scaler, f'{models_dir}/scaler.pkl')
        

        logging.info(f" Entraînement terminé — Train R²={train_r2:.4f}")
        logging.info(f"MAE={test_mae:,.0f}")
        logging.info(f"RMSE={test_rmse:,.0f}")
        logging.info(f" Modèles sauvegardés dans {models_dir}")
        logging.info(f" Entraînement exécuté à {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # Convertir cv_scores (numpy array) en liste Python pour la sérialisation
        return {
            'train_r2': train_r2,
            'test_r2': test_r2, 
            'mae': test_mae, 
            'rmse': test_rmse, 
            'cross_val': cv_scores.tolist(), 
            'cross_val_mean': float(cv_scores.mean()),  
            'cross_val_std': float(cv_scores.std())      
        }

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