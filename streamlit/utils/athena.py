import os
import streamlit as st

if "aws" in st.secrets:
    os.environ["AWS_ACCESS_KEY_ID"] = st.secrets["aws"]["AWS_ACCESS_KEY_ID"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = st.secrets["aws"]["AWS_SECRET_ACCESS_KEY"]
    os.environ["AWS_DEFAULT_REGION"] = st.secrets["aws"]["AWS_DEFAULT_REGION"]

import awswrangler as wr
import pandas as pd
import streamlit as st

DATABASE = "job_market_db"
TABLE = "jobs"
S3_OUTPUT = "s3://job-market-curated-784336/athena-results/"


@st.cache_data(ttl=3600)
def get_all_jobs(metier=None, source=None, type_contrat=None, jours=None):
    # Charge toutes les offres depuis Athena avec filtres optionnels
    # Cache 1h pour éviter de requêter Athena à chaque interaction
    filters = ["1=1"]

    if metier:
        # Multiselect — on génère un OR sur chaque métier sélectionné
        conditions = " OR ".join([f"LOWER(titre) LIKE '%{m.lower()}%'" for m in metier])
        filters.append(f"({conditions})")

    if source:
        filters.append(f"source = '{source}'")

    if type_contrat:
        contrats_str = ", ".join([f"'{c}'" for c in type_contrat])
        filters.append(f"type_contrat IN ({contrats_str})")

    if jours:
        filters.append(f"date_publication >= current_date - interval '{jours}' day")

    query = f"""
        SELECT *
        FROM {DATABASE}.{TABLE}
        WHERE {" AND ".join(filters)}
        ORDER BY date_publication DESC
    """

    return wr.athena.read_sql_query(
        sql=query,
        database=DATABASE,
        s3_output=S3_OUTPUT,
    )


def get_top_skills(df):
    # Calcule le top 15 skills directement sur le DataFrame
    if "skills_nlp" not in df.columns or df.empty:
        return pd.DataFrame(columns=["skill", "count"])

    skills_exploded = df["skills_nlp"].explode().dropna()
    skills_exploded = skills_exploded[skills_exploded != ""]

    if skills_exploded.empty:
        return pd.DataFrame(columns=["skill", "count"])

    top = skills_exploded.value_counts().head(15).reset_index()
    top.columns = ["skill", "count"]
    return top


def classify_entreprise(nom, taille):
    # Classifie ESN / Startup / Grand groupe / Autre
    # Combinaison mots-clés + taille
    ESN_KEYWORDS = [
        "sopra", "capgemini", "atos", "thales", "cgi", "alten", "altran",
        "accenture", "ibm", "talan", "sii", "devoteam", "wavestone",
        "sia partners", "extia", "hardis", "neurones", "meritis", "exalt",
        "nexton", "adecco", "manpower", "randstad", "hays", "michael page",
        "jakala", "actinvision"
    ]

    nom_lower = str(nom).lower() if pd.notna(nom) else ""

    for esn in ESN_KEYWORDS:
        if esn in nom_lower:
            return "ESN"

    try:
        t = int(taille) if pd.notna(taille) else 0
        if t > 5000:
            return "Grand groupe"
        elif t < 200:
            return "Startup"
    except (ValueError, TypeError):
        pass

    return "Autre"
