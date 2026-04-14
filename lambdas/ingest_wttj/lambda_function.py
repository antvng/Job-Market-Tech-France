import json
import boto3
import requests
import logging
from datetime import datetime, timezone

# Logger -> CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# CONSTANTES
AWS_REGION = "eu-west-3"
BUCKET_RAW = "job-market-raw-784336"

# Algolia API — clés récupérées depuis le JS du site WTTJ
ALGOLIA_APP_ID = "CSEKHVMS53"
ALGOLIA_API_KEY = "4bd8f6215d0cc52b26430765769e65a0"
ALGOLIA_URL = f"https://{ALGOLIA_APP_ID}-dsn.algolia.net/1/indexes/wttj_jobs_production_fr/query"

# Métiers à collecter — mêmes que France Travail pour pouvoir comparer
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
    "Développeur Full Stack",
    "Développeur Backend",
    "Cloud Engineer",
    "DevOps",
    "Consultant Data",
    "QA Engineer",
    "Security Engineer",
]

# Nb d'offres par page Algolia — max autorisé
PAGE_SIZE = 100


# func Récupérer toutes les offres pour un métier donné
# On s'arrête quand on a parcouru toutes les pages disponibles
def fetch_offres_wttj(metier):
    offres = []
    page = 0

    headers = {
        "X-Algolia-Application-Id": ALGOLIA_APP_ID,
        "X-Algolia-API-Key": ALGOLIA_API_KEY,
        "Content-Type": "application/json",
        "Referer": "https://www.welcometothejungle.com/",
        "Origin": "https://www.welcometothejungle.com",
    }

    while True:
        payload = {
            "query": metier,
            "hitsPerPage": PAGE_SIZE,
            "page": page,
            "filters": 'offices.country_code:"FR"',
            "attributesToRetrieve": [
                "id",
                "name",
                "slug",
                "contract_type",
                "remote",
                "salary_minimum",
                "salary_maximum",
                "salary_currency",
                "salary_period",
                "experience_level_minimum",
                "published_at",
                "offices",
                "organization",
                "sectors",
                "key_missions",
                "profile",
            ],
        }

        response = requests.post(ALGOLIA_URL, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()

        hits = data.get("hits", [])
        nb_pages = data.get("nbPages", 0)

        # Construit l'URL publique depuis org slug + job slug
        # format : welcometothejungle.com/fr/companies/{org}/jobs/{job}
        for hit in hits:
            org_slug = hit.get("organization", {}).get("slug", "")
            job_slug = hit.get("slug", "")
            if org_slug and job_slug:
                hit["url_publique"] = f"https://www.welcometothejungle.com/fr/companies/{org_slug}/jobs/{job_slug}"
            else:
                hit["url_publique"] = None

        offres.extend(hits)

        logger.info(
            f"Métier: {metier} | Page {page + 1}/{nb_pages} | {len(hits)} offres"
        )

        # Stop si dernière page
        if page >= nb_pages - 1:
            break

        page += 1

    return offres


# func Sauvegarder les offres en JSON dans S3
# struct : raw/wttj/date=YYYY-MM-DD/data.json
def save_to_s3(offres, date_partition):
    s3 = boto3.client("s3", region_name=AWS_REGION)

    s3_key = f"raw/wttj/date={date_partition}/data.json"
    body = json.dumps(offres, ensure_ascii=False, indent=2)

    s3.put_object(
        Bucket=BUCKET_RAW,
        Key=s3_key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )

    logger.info(f"{len(offres)} offres sauvegardées → s3://{BUCKET_RAW}/{s3_key}")
    return s3_key


# HANDLER PRINCIPAL
# Flow :
# 1. Boucle sur 16 métiers
# 2. Collecte toutes les pages d'offres par métier
# 3. Construit l'URL publique depuis org slug + job slug
# 4. Déduplique par objectID WTTJ
# 5. Sauvegarde JSON dans S3
def lambda_handler(event, context):
    logger.info("=== Démarrage de l'ingestion WTTJ ===")

    date_partition = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    logger.info(f"Date de partition : {date_partition}")

    toutes_offres = []

    # 1, 2, 3 Collecte par métier + construction URLs
    for metier in METIERS:
        try:
            offres = fetch_offres_wttj(metier)
            toutes_offres.extend(offres)
            logger.info(f"{metier} → {len(offres)} offres")
        except Exception as e:
            logger.error(f"Erreur {metier} : {str(e)}")
            continue

    logger.info(f"Total brut collecté : {len(toutes_offres)} offres")

    # 4 Déduplication par objectID WTTJ
    seen = set()
    offres_dedup = []
    for offre in toutes_offres:
        job_id = offre.get("objectID")
        if job_id and job_id not in seen:
            seen.add(job_id)
            offres_dedup.append(offre)

    logger.info(f"Après déduplication : {len(offres_dedup)} offres uniques")

    # 5 Sauvegarde S3
    s3_key = save_to_s3(offres_dedup, date_partition)

    logger.info("=== Ingestion WTTJ terminée avec succès ===")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "date": date_partition,
            "total_offres": len(offres_dedup),
            "s3_key": s3_key,
        })
    }
