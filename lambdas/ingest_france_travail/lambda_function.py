import json
import boto3
import requests
import logging
from datetime import datetime, timezone

# CONFIGURATION DU LOGGER (Cloudwatch)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# CONSTANTES, centralisation des cfgs ici

# AWS Secrets Manager ; credentials France Travail
SECRET_NAME = "job-market/france-travail"

# Region AWS
AWS_REGION = "eu-west-3"

# Bucket S3 raw ou on va stocker les JSON bruts
BUCKET_RAW = "job-market-raw-784336"

# URL l'API France Travail
FT_BASE_URL = "https://api.francetravail.io/partenaire/offresdemploi/v2"

# URL pour recuperer le token OAuth2
FT_TOKEN_URL = "https://entreprise.francetravail.fr/connexion/oauth2/access_token?realm=%2Fpartenaire"

# METIERS A COLLECTER
METIERS = [
    "Data Engineer",
    "Analytics Engineer",
    "Data Analyst",
    "Data Scientist",
    "Machine Learning Engineer",
    "MLOps",
    "Business Analyst",
    "Product Owner",
    "Chef de projet IT",
    "Developpeur Full Stack",
    "Developpeur Backend",
    "Cloud Engineer",
    "DevOps",
    "Consultant Data",
    "QA Engineer",
    "Security Engineer",
]

# REGIONS FRANCE
# On couvre toute la France metropolitaine
REGIONS_FRANCE = [
    "84",  # Auvergne-Rhone-Alpes
    "27",  # Bourgogne-Franche-Comte
    "53",  # Bretagne
    "24",  # Centre-Val de Loire
    "94",  # Corse
    "44",  # Grand Est
    "32",  # Hauts-de-France
    "11",  # Ile-de-France
    "28",  # Normandie
    "75",  # Nouvelle-Aquitaine
    "76",  # Occitanie
    "52",  # Pays de la Loire
    "93",  # Provence-Alpes-Cote d'Azur
]


# func Recuperer les credentials depuis AWS Secrets Manager
def get_credentials():
    client = boto3.client("secretsmanager", region_name=AWS_REGION)
    response = client.get_secret_value(SecretId=SECRET_NAME)
    secret = json.loads(response["SecretString"])
    return secret["client_id"], secret["client_secret"]


# func S'authentifier sur l'API France Travail (OAuth2 Client Credentials)
# on envoie notre client_id + client_secret, on recoit un access_token.
# Ce token est valable 30 minutes — largement suffisant pour notre run.
def get_access_token(client_id, client_secret):
    logger.info("Recuperation du token OAuth2 France Travail...")

    response = requests.post(
        FT_TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "api_offresdemploiv2 o2dsoffre",
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )

    # Si echec->stop
    response.raise_for_status()
    token = response.json()["access_token"]
    logger.info("Token OAuth2 recupere avec succes")
    return token


# func Recuperer toutes les offres pour un metier + region donnee
# On pagine avec le parametre "range" : "0-149", "150-299", etc.
# On s'arrete quand on recoit moins de 150 resultats (= derniere page)
# ou quand l'API retourne 204 No Content (= plus rien a recuperer).
def fetch_offres(token, metier, region):
    offres = []
    start = 0
    page_size = 149  # Max autorise par l'API (0-indexed donc 150 offres)

    headers = {"Authorization": f"Bearer {token}"}

    while True:
        params = {
            "motsCles": metier,
            "region": region,
            "range": f"{start}-{start + page_size}",
        }

        response = requests.get(
            f"{FT_BASE_URL}/offres/search",
            headers=headers,
            params=params,
        )

        # 204 = pas de resultats sur cette page
        if response.status_code == 204:
            break

        response.raise_for_status()
        data = response.json()

        resultats = data.get("resultats", [])
        offres.extend(resultats)

        logger.info(
            f"Metier: {metier} | Region: {region} | "
            f"Page {start}-{start + page_size} | {len(resultats)} offres"
        )

        # Si on a moins de 150 resultats : c'est la derniere page
        if len(resultats) < 150:
            break

        start += 150

    return offres


# func Sauvegarder les offres en JSON dans S3
# struct : raw/france-travail/date=YYYY-MM-DD/data.json
# Glue et Athena devraient reconnaitre automatiquement ce format pour partition
def save_to_s3(offres, date_partition):
    s3 = boto3.client("s3", region_name=AWS_REGION)

    # Cle S3 avec partitionnement par date
    s3_key = f"raw/france-travail/date={date_partition}/data.json"

    # On serialise la liste d'offres en JSON
    body = json.dumps(offres, ensure_ascii=False, indent=2)

    s3.put_object(
        Bucket=BUCKET_RAW,
        Key=s3_key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )

    logger.info(f"[OK] {len(offres)} offres sauvegardees -> s3://{BUCKET_RAW}/{s3_key}")
    return s3_key


# HANDLER PRINCIPAL
# EventBridge
# Flow complet :
# 1. Recupere les credentials depuis Secrets Manager
# 2. S'authentifie et obtient le token OAuth2
# 3. Boucle sur 16 metiers x 13 regions France
# 4. Collecte toutes les offres avec pagination
# 5. Deduplique par job_id (une offre peut apparaitre sur plusieurs metiers)
# 6. Sauvegarde le JSON brut dans S3 partitionne par date
def lambda_handler(event, context):
    logger.info("=== Demarrage de l'ingestion France Travail ===")

    # Date du jour pour le partitionnement S3
    date_partition = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    logger.info(f"Date de partition : {date_partition}")

    # 1 Recuperer les credentials
    client_id, client_secret = get_credentials()

    # 2 S'authentifier
    token = get_access_token(client_id, client_secret)

    # 3,4 Collecter toutes les offres
    toutes_offres = []

    for metier in METIERS:
        for region in REGIONS_FRANCE:
            try:
                offres = fetch_offres(token, metier, region)
                toutes_offres.extend(offres)
                logger.info(f"[OK] {metier} / region {region} -> {len(offres)} offres")
            except Exception as e:
                # On log l'erreur mais on continue les autres metiers
                # Un echec sur un metier ne doit pas bloquer tout le pipeline
                logger.error(f"[ERR] {metier} / region {region} : {str(e)}")
                continue

    logger.info(f"Total brut collecte : {len(toutes_offres)} offres")

    # 5 Deduplication par identifiant unique France Travail
    # Une meme offre peut remonter sur plusieurs metiers
    seen = set()
    offres_dedup = []
    for offre in toutes_offres:
        job_id = offre.get("id")
        if job_id and job_id not in seen:
            seen.add(job_id)
            offres_dedup.append(offre)

    logger.info(f"Apres deduplication : {len(offres_dedup)} offres uniques")

    # 6 Sauvegarder dans S3
    s3_key = save_to_s3(offres_dedup, date_partition)

    logger.info("=== Ingestion France Travail terminee avec succes ===")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "date": date_partition,
            "total_offres": len(offres_dedup),
            "s3_key": s3_key,
        })
    }
