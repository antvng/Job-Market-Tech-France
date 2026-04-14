# Job Market Tech France

Pipeline AWS serverless qui collecte quotidiennement les offres d'emploi tech en France, les transforme et alimente un dashboard interactif.
"En data engineering, Python et SQL apparaissent dans 80% des offres CDI — mais le salaire médian varie de 42k en ESN à 58k en startup."

---

## C'est quoi ce projet ?

J'ai construit un pipeline end-to-end qui répond à une question concrète : quelles sont les compétences les plus demandées dans la tech en France ?
Mais aussi pour centraliser les offres de diverses sources différentes pour pouvoir postuler plus facilement!

Les données viennent de deux sources officielles, l'API France Travail (OAuth2) et l'API Algolia de Welcome to the Jungle soit environ 10 000 offres collectées chaque jour, automatiquement, depuis 16 métiers tech couvrant toute la France.

---

## Stack

Outils & Rôles

AWS Lambda : Ingestion des APIs (France Travail + WTTJ)
Amazon S3 : Data lake, raw (JSON) et curated (Parquet)
AWS Glue : Transformation PySpark et normalisation, NLP, déduplication
AWS Athena : Requêtes SQL serverless sur le Parquet
AWS Step Functions : Orchestration du pipeline (parallel ingest puis Glue puis Crawler)
Amazon EventBridge : Déclenchement quotidien à 7h UTC 
AWS Secrets Manager : Credentials OAuth2 France Travail
Streamlit : CloudDashboard 

---

## Architecture

```
EventBridge (quotidien 9h Paris)
        |
        +-- Lambda ingest-france-travail --> S3 raw/france-travail/date=YYYY-MM-DD/
        +-- Lambda ingest-wttj           --> S3 raw/wttj/date=YYYY-MM-DD/
        |
Step Functions (parallel → sync)
        |
Glue ETL Job (PySpark)
    Normalisation schema commun (20 colonnes)
    Extraction NLP skills (regex, 60+ keywords)
    Deduplication cross-sources (hash MD5)
    Ecriture Parquet partitionné par date
        |
S3 curated/jobs/date=YYYY-MM-DD/*.parquet
        |
Glue Crawler --> Data Catalog
        |
Athena SQL
        |
Streamlit Cloud (dashboard public)
```

Architecture médaillon : `raw/` = JSON brut, `curated/` = Parquet nettoyé.

---

## Dashboard

**Vue d'ensemble**

KPIs : offres actives, % avec salaire affiché, salaire médian, % full remote, nouvelles 48h.
Répartition des sources, top entreprises recruteuses, top 15 skills détectés par NLP.
Répartition contrats, politique remote, niveau d'expérience requis.

**Tableau des offres**

Table paginée avec titre cliquable (lien direct WTTJ ou recherche Google pour France Travail), badge NEW pour les offres publiées dans les 48 dernières heures, filtres sidebar complets : métier, source, contrat, expérience, région, période.

**Benchmark salaires**

Salaire médian par métier, comparatif ESN vs startup vs grand groupe, scatter plot salaire par tranche d'expérience.

[Voir le dashboard](https://job-market-tech-france.streamlit.app)

---

## Pipeline et infrastructure

**Ingestion**

Deux Lambdas Python tournent en parallèle via Step Functions. La Lambda France Travail couvre 16 métiers x 13 régions avec pagination OAuth2. La Lambda WTTJ interroge l'API Algolia (index `wttj_jobs_production_fr`) sur les mêmes 16 métiers, construit les URLs publiques depuis les slugs organisation/offre.

**Transformation Glue**

Le job PySpark normalise les deux sources vers un schéma commun de 20 colonnes : parsing des salaires (annuel/mensuel/horaire → annuel), normalisation des contrats, extraction NLP des compétences par regex sur les descriptions (60+ keywords : SQL, Python, Spark, dbt, Airflow, AWS, Azure, Docker, etc.), déduplication par hash MD5 (titre + entreprise + ville).

---

## Sources de données

France Travail - API officielle OAuth2
Welcome to the Jungle - API Algolia (clés publiques)

Les deux sources sont dédupliquées cross-source par hash sur titre + entreprise + ville.

---

## Evolutions envisagées

- Indeed : ancienne API de recherche dépréciée, accès actuel réservé aux partenaires recruteurs avec budget publicitaire
- Glassdoor : partenariats API arrêtés, aucun accès public disponible
- HelloWork : pas d'API publique de recherche d'offres documentée, scraping non officiel uniquement
- APEC : pas d'API publique disponible, nécessiterait du scraping non officiel
- LinkedIn Jobs : API restreinte aux partenaires RH agréés
- Enrichissement NLP : classification automatique des métiers par modèle (vs regex actuelle)
- Alertes email : notification quotidienne sur les nouvelles offres matchant un profil

