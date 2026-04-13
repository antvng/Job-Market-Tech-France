import sys
import re
import hashlib
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import *

# INIT GLUE + SPARK
# GlueContext wrappe SparkContext avec les connecteurs AWS natifs
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# CONSTANTES
BUCKET_RAW = "job-market-raw-784336"
BUCKET_CURATED = "job-market-curated-784336"
DATE_PARTITION = datetime.utcnow().strftime("%Y-%m-%d")

# Liste des 60+ technos et skills qu'on cherche dans les descriptions.
# On utilise des regex word-boundary (\b) pour éviter les faux positifs
SKILLS_KEYWORDS = [
    # Data & BI
    "sql", "python", "spark", "pyspark", "scala", "java", "r",
    "dbt", "airflow", "kafka", "hadoop", "hive", "presto", "trino",
    "snowflake", "databricks", "redshift", "bigquery", "synapse",
    "powerbi", "power bi", "tableau", "looker", "microstrategy", "qlik",
    "excel", "pandas", "numpy", "scikit-learn", "sklearn",
    # Cloud
    "aws", "azure", "gcp", "google cloud",
    "s3", "glue", "athena", "lambda", "redshift", "emr",
    "blob storage", "data factory", "azure synapse",
    "bigquery", "dataflow", "pub/sub",
    # DevOps & outils
    "docker", "kubernetes", "k8s", "terraform", "ansible",
    "git", "github", "gitlab", "jenkins", "ci/cd",
    "linux", "bash", "shell",
    # ML/AI
    "machine learning", "deep learning", "nlp", "llm",
    "tensorflow", "pytorch", "keras", "mlflow", "mlops",
    "scikit", "xgboost", "lightgbm",
    # Méthodes
    "agile", "scrum", "kanban", "devops", "dataops",
    "api", "rest", "graphql", "microservices",
]


# FUNC : Nettoyer le HTML
# WTTJ stocke le profile en HTML — on supprime les balises avant le NLP
def clean_html(text):
    if not text:
        return ""
    # Supprime toutes les balises HTML
    clean = re.sub(r"<[^>]+>", " ", text)
    # Supprime les entités HTML (&amp; &nbsp; etc.)
    clean = re.sub(r"&[a-zA-Z]+;", " ", clean)
    # Nettoie les espaces multiples
    clean = re.sub(r"\s+", " ", clean).strip()
    return clean


# FUNC : Extraire les skills par regex sur un texte
# On passe le texte en minuscules et on cherche chaque keyword
# Retourne une liste dédupliquée de skills trouvés
def extract_skills(text):
    if not text:
        return []
    text_lower = text.lower()
    found = []
    for skill in SKILLS_KEYWORDS:
        # \b = word boundary — "sql" ne matche pas dans "nosql"
        pattern = r"\b" + re.escape(skill) + r"\b"
        if re.search(pattern, text_lower):
            found.append(skill)
    return list(set(found))


# FUNC : Parser le salaire France Travail
# On extrait les deux montants avec regex et on convertit en annuel
def parse_salaire_ft(libelle):
    if not libelle:
        return None, None, None

    # Extrait les nombres du libellé
    nombres = re.findall(r"[\d]+(?:[.,]\d+)?", libelle.replace(" ", ""))
    nombres = [float(n.replace(",", ".")) for n in nombres]

    if len(nombres) < 2:
        return None, None, None

    salaire_min = nombres[0]
    salaire_max = nombres[1]

    # Détecte la période
    libelle_lower = libelle.lower()
    if "annuel" in libelle_lower or "an" in libelle_lower:
        periode = "annuel"
    elif "mensuel" in libelle_lower or "mois" in libelle_lower:
        periode = "mensuel"
        # Convertit en annuel
        salaire_min *= 12
        salaire_max *= 12
    elif "horaire" in libelle_lower or "heure" in libelle_lower:
        periode = "horaire"
        # Convertit en annuel (35h/semaine × 52 semaines)
        salaire_min *= 35 * 52
        salaire_max *= 35 * 52
    else:
        periode = "annuel"

    return int(salaire_min), int(salaire_max), periode


# FUNC : Parser les années d'expérience France Travail
def parse_experience_ft(libelle):
    if not libelle:
        return None
    if "débutant" in libelle.lower():
        return 0
    match = re.search(r"(\d+)", libelle)
    return int(match.group(1)) if match else None


# FUNC : Normaliser le type de contrat
# FT : "CDI", "CDD", "MIS" (intérim)
# WTTJ : "full_time", "internship", "apprenticeship", "freelance"
# On normalise vers un vocabulaire commun
def normalize_contrat(contrat, source):
    if not contrat:
        return None

    contrat_lower = contrat.lower()

    if source == "france-travail":
        mapping = {
            "cdi": "CDI",
            "cdd": "CDD",
            "mis": "Intérim",
            "fre": "Freelance",
            "sta": "Stage",
            "alt": "Alternance",
        }
        return mapping.get(contrat_lower[:3], contrat)

    elif source == "wttj":
        mapping = {
            "full_time": "CDI",
            "part_time": "CDD",
            "internship": "Stage",
            "apprenticeship": "Alternance",
            "freelance": "Freelance",
            "temporary": "Intérim",
        }
        return mapping.get(contrat_lower, contrat)

    return contrat


# FUNC : Normaliser le remote
# WTTJ : "fulltime", "partial", "punctual", "no", "unknown"
# FT : pas de champ, on retourne None
def normalize_remote(remote, source):
    if source == "france-travail":
        return None
    if not remote or remote == "unknown":
        return None
    mapping = {
        "fulltime": "fulltime",
        "partial": "hybrid",
        "punctual": "punctual",
        "no": "no",
    }
    return mapping.get(remote, None)


# FUNC : Générer une clé de déduplication
# Hash MD5 sur titre normalisé + entreprise + ville
# Permet de détecter les doublons cross-sources
def generate_dedup_key(titre, entreprise, ville):
    # Normalise avant le hash pour maximiser les matches
    def norm(s):
        if not s:
            return ""
        return re.sub(r"\s+", " ", s.lower().strip())

    key = f"{norm(titre)}|{norm(entreprise)}|{norm(ville)}"
    return hashlib.md5(key.encode()).hexdigest()


# UDFs PYSPARK
# On enregistre nos fonctions Python comme UDFs (User Defined Functions)
# pour pouvoir les appliquer sur des colonnes Spark
extract_skills_udf = F.udf(extract_skills, ArrayType(StringType()))
clean_html_udf = F.udf(clean_html, StringType())
normalize_remote_udf = F.udf(
    lambda r, s: normalize_remote(r, s), StringType()
)
normalize_contrat_udf = F.udf(
    lambda c, s: normalize_contrat(c, s), StringType()
)
generate_dedup_key_udf = F.udf(generate_dedup_key, StringType())

# Salaire FT retourne 3 valeurs, on crée 3 UDFs séparées
parse_sal_min_udf = F.udf(
    lambda l: parse_salaire_ft(l)[0], IntegerType()
)
parse_sal_max_udf = F.udf(
    lambda l: parse_salaire_ft(l)[1], IntegerType()
)
parse_sal_periode_udf = F.udf(
    lambda l: parse_salaire_ft(l)[2], StringType()
)
parse_exp_udf = F.udf(parse_experience_ft, IntegerType())


# 1 LECTURE DES JSON BRUTS DEPUIS S3
# On lit les deux fichiers JSON du jour en cours
print(f"=== Lecture des données brutes pour {DATE_PARTITION} ===")

# Lecture France Travail
df_ft_raw = spark.read.option("multiline", "true").json(
    f"s3://{BUCKET_RAW}/raw/france-travail/date={DATE_PARTITION}/data.json"
)
print(f"France Travail brut : {df_ft_raw.count()} offres")

# Lecture WTTJ
df_wttj_raw = spark.read.option("multiline", "true").json(
    f"s3://{BUCKET_RAW}/raw/wttj/date={DATE_PARTITION}/data.json"
)
print(f"WTTJ brut : {df_wttj_raw.count()} offres")


# 2 Normalisation FT pour schéma commun
# On mappe les champs FT vers nos 20 colonnes standardisées
print("=== Normalisation France Travail ===")

df_ft = df_ft_raw.select(
    # Identifiants
    F.col("id").alias("job_id"),
    F.lit("france-travail").alias("source"),

    # Titre et entreprise
    F.col("intitule").alias("titre"),
    F.col("entreprise.nom").alias("entreprise"),

    # Localisation
    F.col("lieuTravail.libelle").alias("ville"),
    F.col("lieuTravail.libelle").alias("region"),  # on parse la région depuis le libellé

    # Contrat
    normalize_contrat_udf(
        F.col("typeContrat"), F.lit("france-travail")
    ).alias("type_contrat"),

    # Remote None ici
    F.lit(None).cast(StringType()).alias("remote"),

    # Expérience
    parse_exp_udf(F.col("experienceLibelle")).alias("experience_annees"),

    # Salaire 
    parse_sal_min_udf(F.col("salaire.libelle")).alias("salaire_min"),
    parse_sal_max_udf(F.col("salaire.libelle")).alias("salaire_max"),
    parse_sal_periode_udf(F.col("salaire.libelle")).alias("salaire_periode"),

    # Secteur
    F.col("secteurActiviteLibelle").alias("secteur"),

    # Taille entreprise
    F.col("trancheEffectifEtab").alias("taille_entreprise"),

    # Niveau études premier élément de la liste formations
    F.when(
        F.size(F.col("formations")) > 0,
        F.col("formations")[0]["niveauLibelle"]
    ).otherwise(None).alias("niveau_etudes"),

    # Description complète
    F.col("description").alias("description_full"),

    # Dates
    F.to_date(F.col("dateCreation")).alias("date_publication"),
    F.lit(DATE_PARTITION).alias("date_partition"),
)

# Extraction NLP skills sur la description
df_ft = df_ft.withColumn(
    "skills_nlp",
    extract_skills_udf(F.col("description_full"))
)

# Clé de déduplication
df_ft = df_ft.withColumn(
    "dedup_key",
    generate_dedup_key_udf(
        F.col("titre"), F.col("entreprise"), F.col("ville")
    )
)

print(f"France Travail normalisé : {df_ft.count()} offres")


# 3 Normalisation WTTJ pour schéma commun
print("=== Normalisation WTTJ ===")

# Concaténisation de  key_missions + profile pour maximiser la détection NLP
# key_missions = liste, on joint en string
# profile = HTML, on nettoie d'abord
df_wttj = df_wttj_raw.select(
    # Identifiants
    F.col("objectID").alias("job_id"),
    F.lit("wttj").alias("source"),

    # Titre et entreprise
    F.col("name").alias("titre"),
    F.col("organization.name").alias("entreprise"),

    # Localisation 
    F.when(
        F.size(F.col("offices")) > 0,
        F.col("offices")[0]["city"]
    ).otherwise(None).alias("ville"),
    F.when(
        F.size(F.col("offices")) > 0,
        F.col("offices")[0]["state"]
    ).otherwise(None).alias("region"),

    # Contrat
    normalize_contrat_udf(
        F.col("contract_type"), F.lit("wttj")
    ).alias("type_contrat"),

    # Remote
    normalize_remote_udf(
        F.col("remote"), F.lit("wttj")
    ).alias("remote"),

    # Expérience 
    F.col("experience_level_minimum").cast(IntegerType()).alias("experience_annees"),

    # Salaire on convertit en annuel si nécessaire
    F.when(
        F.col("salary_period") == "daily",
        (F.col("salary_minimum") * 220).cast(IntegerType())  # ~220 jours travaillés/an
    ).when(
        F.col("salary_period") == "monthly",
        (F.col("salary_minimum") * 12).cast(IntegerType())
    ).otherwise(
        F.col("salary_minimum").cast(IntegerType())
    ).alias("salaire_min"),

    F.when(
        F.col("salary_period") == "daily",
        (F.col("salary_maximum") * 220).cast(IntegerType())
    ).when(
        F.col("salary_period") == "monthly",
        (F.col("salary_maximum") * 12).cast(IntegerType())
    ).otherwise(
        F.col("salary_maximum").cast(IntegerType())
    ).alias("salaire_max"),

    F.lit("annuel").alias("salaire_periode"),  # tout converti en annuel

    # Secteur premier secteur parent
    F.when(
        F.size(F.col("sectors")) > 0,
        F.col("sectors")[0]["parent_name"]
    ).otherwise(None).alias("secteur"),

    # Taille entreprise
    F.col("organization.nb_employees").cast(IntegerType()).alias("taille_entreprise"),

    # Niveau études pas disponible dans WTTJ
    F.lit(None).cast(StringType()).alias("niveau_etudes"),

    # Description = key_missions jointes + profile nettoyé
    F.concat_ws(
        " ",
        F.concat_ws(" ", F.col("key_missions")),  # liste vers string
        clean_html_udf(F.col("profile"))           # HTML vers texte
    ).alias("description_full"),

    # Dates
    F.to_date(F.col("published_at")).alias("date_publication"),
    F.lit(DATE_PARTITION).alias("date_partition"),
)

# Extraction NLP skills
df_wttj = df_wttj.withColumn(
    "skills_nlp",
    extract_skills_udf(F.col("description_full"))
)

# Clé de déduplication
df_wttj = df_wttj.withColumn(
    "dedup_key",
    generate_dedup_key_udf(
        F.col("titre"), F.col("entreprise"), F.col("ville")
    )
)

print(f"WTTJ normalisé : {df_wttj.count()} offres")


# 4 Union FT et WTTJ
print("Union FT + WTTJ")

df_all = df_ft.unionByName(df_wttj)
print(f"Total après union : {df_all.count()} offres")


# 5 Dédup
# exact match sur dedup_key (même titre + entreprise + ville)
# FT en prio si doublons sur les 2 sources (FT + de métadata)
print("Déduplication")

# FT en premier pour la prio
df_sorted = df_all.orderBy(
    F.when(F.col("source") == "france-travail", 0).otherwise(1)
)

# dropDuplicates garde la première occurrence
df_dedup = df_sorted.dropDuplicates(["dedup_key"])
print(f"Après déduplication : {df_dedup.count()} offres uniques")


# 6 Parquet partitionné par date
# S3curated sous PARQUET et parti Hive
# Crawler reconnaîtra automatiquement pour le Data Catalog
print("Écriture Parquet dans S3 curated")

df_dedup.write \
    .mode("overwrite") \
    .partitionBy("date_partition") \
    .parquet(f"s3://{BUCKET_CURATED}/jobs/")

print(f"{df_dedup.count()} offres écrites dans s3://{BUCKET_CURATED}/jobs/date={DATE_PARTITION}/")

job.commit()
print("Job Glue terminé avec succès")