import streamlit as st
import plotly.express as px
import pandas as pd
from datetime import timedelta
from utils.athena import get_all_jobs, get_top_skills, classify_entreprise

st.set_page_config(
    page_title="Job Market Tech France",
    layout="wide",
)

with st.sidebar:
    st.title("Job Market Tech France")

    if st.button("Actualiser"):
        st.cache_data.clear()
        st.rerun()

    st.markdown("---")

    METIERS = [
        "Data Engineer", "Analytics Engineer", "Data Analyst",
        "Data Scientist", "Machine Learning Engineer", "MLOps",
        "Business Analyst", "Product Owner", "Chef de projet IT",
        "Développeur Full Stack", "Développeur Backend",
        "Cloud Engineer", "DevOps", "Consultant Data",
        "QA Engineer", "Security Engineer",
    ]
    metiers = st.multiselect("Métier", METIERS, placeholder="Tous les métiers")

    source = st.selectbox("Source", ["Toutes", "france-travail", "wttj"])

    type_contrat = st.multiselect("Contrat",
        ["CDI", "CDD", "Stage", "Alternance", "Freelance", "Intérim"])

    experience = st.multiselect("Expérience",
        ["Junior (0-2 ans)", "Confirmé (3-5 ans)", "Senior (5+ ans)", "Non renseigné"])

    REGIONS = {
        "Île-de-France": ["île-de-france", "paris", "75 -", "77 -", "78 -", "91 -", "92 -", "93 -", "94 -", "95 -"],
        "Auvergne-Rhône-Alpes": ["auvergne", "rhône-alpes", "lyon", "grenoble", "01 -", "03 -", "07 -", "15 -", "26 -", "38 -", "42 -", "43 -", "63 -", "69 -", "73 -", "74 -"],
        "Provence-Alpes-Côte d'Azur": ["provence", "marseille", "nice", "04 -", "05 -", "06 -", "13 -", "83 -", "84 -"],
        "Occitanie": ["occitanie", "toulouse", "montpellier", "09 -", "11 -", "12 -", "30 -", "31 -", "32 -", "34 -", "46 -", "48 -", "65 -", "66 -", "81 -", "82 -"],
        "Nouvelle-Aquitaine": ["nouvelle-aquitaine", "bordeaux", "16 -", "17 -", "19 -", "23 -", "24 -", "33 -", "40 -", "47 -", "64 -", "79 -", "86 -", "87 -"],
        "Grand Est": ["grand est", "strasbourg", "08 -", "10 -", "51 -", "52 -", "54 -", "55 -", "57 -", "67 -", "68 -", "88 -"],
        "Hauts-de-France": ["hauts-de-france", "lille", "02 -", "59 -", "60 -", "62 -", "80 -"],
        "Bretagne": ["bretagne", "rennes", "22 -", "29 -", "35 -", "56 -"],
        "Pays de la Loire": ["pays de la loire", "nantes", "44 -", "49 -", "53 -", "72 -", "85 -"],
        "Normandie": ["normandie", "rouen", "caen", "14 -", "27 -", "50 -", "61 -", "76 -"],
        "Bourgogne-Franche-Comté": ["bourgogne", "franche-comté", "dijon", "21 -", "25 -", "39 -", "58 -", "70 -", "71 -", "89 -", "90 -"],
        "Centre-Val de Loire": ["centre-val de loire", "tours", "orléans", "18 -", "28 -", "36 -", "37 -", "41 -", "45 -"],
        "Corse": ["corse", "ajaccio", "2a -", "2b -"],
    }
    regions = st.multiselect("Région", list(REGIONS.keys()), placeholder="Toutes les régions")

    jours = st.selectbox("Période", [
        "Tout", "Aujourd'hui", "3 jours", "7 jours", "30 jours", "3 mois"
    ])
    jours_val = {"Tout": None, "Aujourd'hui": 1, "3 jours": 3,
                 "7 jours": 7, "30 jours": 30, "3 mois": 90}[jours]

    st.markdown("---")
    st.caption("Lambda · Glue · Athena · S3")

with st.spinner("Chargement..."):
    df = get_all_jobs(
        metier=metiers if metiers else None,
        source=None if source == "Toutes" else source,
        type_contrat=type_contrat if type_contrat else None,
        jours=jours_val,
    )

if df.empty:
    st.warning("Aucune offre trouvée avec ces filtres.")
    st.stop()

def cat_exp(x):
    if pd.isna(x): return "Non renseigné"
    elif x <= 2: return "Junior (0-2 ans)"
    elif x <= 5: return "Confirmé (3-5 ans)"
    else: return "Senior (5+ ans)"

df["exp_cat"] = df["experience_annees"].apply(cat_exp)
if experience:
    df = df[df["exp_cat"].isin(experience)]

if regions:
    def match_region(row):
        ville = row.get("ville")
        region = row.get("region")
        text = " ".join([
            str(ville) if pd.notna(ville) else "",
            str(region) if pd.notna(region) else ""
        ]).lower()
        for r in regions:
            for keyword in REGIONS[r]:
                if keyword.lower() in text:
                    return True
        return False
    df = df[df.apply(match_region, axis=1)]
    
CONTRAT_MAPPING = {
    "CDI": "CDI", "CDD": "CDD", "Stage": "Stage",
    "Alternance": "Alternance", "Freelance": "Freelance", "Intérim": "Intérim",
}
df["type_contrat_clean"] = df["type_contrat"].map(CONTRAT_MAPPING).fillna("Autre")

if df.empty:
    st.warning("Aucune offre trouvée avec ces filtres.")
    st.stop()

df_sal_valid = df[
    df["salaire_min"].notna() &
    (df["salaire_min"] > 15000) &
    (df["salaire_min"] < 200000)
]

k1, k2, k3, k4, k5 = st.columns(5)
with k1:
    st.metric("Offres actives", f"{len(df):,}")
with k2:
    pct_sal = round(df["salaire_min"].notna().sum() / len(df) * 100)
    st.metric("Avec salaire", f"{pct_sal}%")
with k3:
    med = int(df_sal_valid["salaire_min"].median()) if not df_sal_valid.empty else 0
    st.metric("Salaire médian", f"{med:,} €" if med else "N/A")
with k4:
    pct_remote = round((df["remote"] == "fulltime").sum() / len(df) * 100)
    st.metric("Full remote", f"{pct_remote}%")
with k5:
    nb_new = (pd.to_datetime(df["date_publication"]) >=
              pd.Timestamp.now().normalize() - timedelta(days=2)).sum()
    st.metric("Nouvelles 48h", f"{nb_new:,}")

st.markdown("---")

col1, col2, col3 = st.columns([1, 1.5, 1.5])

with col1:
    st.subheader("Sources")
    src = df["source"].value_counts().reset_index()
    src.columns = ["source", "count"]
    fig = px.pie(src, values="count", names="source", hole=0.4, height=280)
    fig.update_layout(margin=dict(t=0, b=0))
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Top entreprises")
    top_ent = df["entreprise"].value_counts().head(10).reset_index()
    top_ent.columns = ["entreprise", "count"]
    fig = px.bar(top_ent, x="count", y="entreprise", orientation="h", height=280)
    fig.update_layout(
        yaxis=dict(autorange="reversed", title=None),
        xaxis=dict(title=None),
        margin=dict(t=0, b=0)
    )
    st.plotly_chart(fig, use_container_width=True)

with col3:
    st.subheader("Top 15 Skills")
    top_skills = get_top_skills(df)
    if not top_skills.empty:
        fig = px.bar(top_skills, x="count", y="skill", orientation="h",
                     color="count", color_continuous_scale="Blues", height=280)
        fig.update_layout(
            yaxis=dict(autorange="reversed", title=None),
            xaxis=dict(title=None),
            coloraxis_showscale=False,
            margin=dict(t=0, b=0)
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Pas de skills détectés.")

st.markdown("---")

col4, col5, col6 = st.columns(3)

with col4:
    st.subheader("Contrats")
    c = df["type_contrat_clean"].value_counts().reset_index()
    c.columns = ["contrat", "count"]
    fig = px.pie(c, values="count", names="contrat", hole=0.3, height=250)
    fig.update_layout(margin=dict(t=0, b=0))
    st.plotly_chart(fig, use_container_width=True)

with col5:
    st.subheader("Remote")
    remote_labels = {
        "fulltime": "Full remote", "hybrid": "Hybride",
        "punctual": "Occasionnel", "no": "Présentiel"
    }
    r = df["remote"].map(remote_labels).fillna("Non renseigné").value_counts().reset_index()
    r.columns = ["remote", "count"]
    fig = px.pie(r, values="count", names="remote", hole=0.3, height=250)
    fig.update_layout(margin=dict(t=0, b=0))
    st.plotly_chart(fig, use_container_width=True)

with col6:
    st.subheader("Expérience")
    e = df["exp_cat"].value_counts().reset_index()
    e.columns = ["experience", "count"]
    fig = px.pie(e, values="count", names="experience", hole=0.3, height=250)
    fig.update_layout(margin=dict(t=0, b=0))
    st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

st.subheader("Offres récentes")

today = pd.Timestamp.now().normalize()
df["is_new"] = pd.to_datetime(df["date_publication"]) >= today - timedelta(days=2)

def build_titre_link(row):
    if row["source"] == "wttj" and pd.notna(row.get("url_publique")):
        url = row["url_publique"]
    else:
        titre_enc = str(row["titre"] if pd.notna(row.get("titre")) else "").replace(" ", "+")
        entreprise_enc = str(row["entreprise"] if pd.notna(row.get("entreprise")) else "").replace(" ", "+")
        site = "welcometothejungle.com" if row["source"] == "wttj" else "francetravail.fr"
        url = f"https://www.google.com/search?q={titre_enc}+{entreprise_enc}+site:{site}"
    titre_text = str(row["titre"] if pd.notna(row.get("titre")) else "")
    return f'<a href="{url}" target="_blank">{titre_text}</a>'

def fmt_salaire(row):
    if pd.isna(row["salaire_min"]) or row["salaire_min"] == 0:
        return "N/R"
    if pd.isna(row["salaire_max"]) or row["salaire_max"] == 0:
        return f"{int(row['salaire_min']):,} €"
    return f"{int(row['salaire_min']):,}-{int(row['salaire_max']):,} €"

def fmt_skills(val):
    if not isinstance(val, list) or len(val) == 0:
        return "N/R"
    return ", ".join(val[:4])

df["Titre"] = df.apply(build_titre_link, axis=1)
df["New"] = df["is_new"].apply(lambda x: "NEW" if x else "")
df["Salaire"] = df.apply(fmt_salaire, axis=1)
df["Skills"] = df["skills_nlp"].apply(fmt_skills)
df["Date"] = pd.to_datetime(df["date_publication"]).dt.strftime("%d/%m/%Y")
df["Remote"] = df["remote"].map(
    {"fulltime": "Full remote", "hybrid": "Hybride",
     "punctual": "Occasionnel", "no": "Présentiel"}
).fillna("N/R")

df_show = df[["New", "Titre", "Date", "entreprise", "ville",
              "type_contrat_clean", "Salaire", "Remote",
              "exp_cat", "Skills", "source"]].rename(columns={
    "entreprise": "Entreprise", "ville": "Ville",
    "type_contrat_clean": "Contrat", "source": "Source",
    "exp_cat": "Expérience"
})

PAGE_SIZE = 25
total = len(df_show)
nb_pages = max(1, (total - 1) // PAGE_SIZE + 1)

col_p, _ = st.columns([1, 4])
with col_p:
    page = st.number_input("Page", min_value=1, max_value=nb_pages, value=1, step=1)

start = (page - 1) * PAGE_SIZE
end = min(start + PAGE_SIZE, total)
st.caption(f"{start+1}-{end} sur {total} offres")

st.write(df_show.iloc[start:end].to_html(escape=False, index=False), unsafe_allow_html=True)

st.markdown("---")

st.subheader("Benchmark Salaires")

if df_sal_valid.empty:
    st.info("Pas assez de données salaires avec ces filtres.")
else:
    df_sal = df_sal_valid.copy()
    st.caption(f"Basé sur {len(df_sal):,} offres avec salaire ({round(len(df_sal)/len(df)*100)}% du total)")

    df_sal["type_entreprise"] = df_sal.apply(
        lambda r: classify_entreprise(r["entreprise"], r["taille_entreprise"]), axis=1
    )

    sal_s1, sal_s2, sal_s3 = st.columns(3)

    with sal_s1:
        st.markdown("**Salaire médian par métier**")
        def extract_metier(titre):
            t = str(titre).lower()
            if "data engineer" in t: return "Data Engineer"
            elif "analytics engineer" in t: return "Analytics Engineer"
            elif "data analyst" in t or "analyste" in t: return "Data Analyst"
            elif "data scientist" in t: return "Data Scientist"
            elif "machine learning" in t: return "ML Engineer"
            elif "mlops" in t: return "MLOps"
            elif "business analyst" in t: return "Business Analyst"
            elif "product owner" in t: return "Product Owner"
            elif "devops" in t: return "DevOps"
            elif "cloud" in t: return "Cloud Engineer"
            elif "full stack" in t or "fullstack" in t: return "Dev Full Stack"
            elif "backend" in t: return "Dev Backend"
            elif "qa" in t or "test" in t: return "QA Engineer"
            elif "secu" in t or "security" in t or "cyber" in t: return "Security Engineer"
            else: return "Autre"

        df_sal["metier_cat"] = df_sal["titre"].apply(extract_metier)
        sal_m = (df_sal.groupby("metier_cat")["salaire_min"]
                 .median().reset_index()
                 .rename(columns={"metier_cat": "metier", "salaire_min": "médian"})
                 .sort_values("médian", ascending=True))
        fig = px.bar(sal_m, x="médian", y="metier", orientation="h",
                     color="médian", color_continuous_scale="Blues", height=300)
        fig.update_layout(
            coloraxis_showscale=False,
            xaxis=dict(title=None),
            yaxis=dict(title=None),
            margin=dict(t=0, b=0)
        )
        st.plotly_chart(fig, use_container_width=True)

    with sal_s2:
        st.markdown("**ESN vs Startup**")
        sal_t = (df_sal.groupby("type_entreprise")["salaire_min"]
                 .agg(["median", "min", "max", "count"]).reset_index()
                 .rename(columns={"type_entreprise": "Type", "median": "Médian",
                                  "min": "Min", "max": "Max", "count": "Nb"}))
        for col in ["Médian", "Min", "Max"]:
            sal_t[col] = sal_t[col].apply(lambda x: f"{int(x):,} €")
        st.dataframe(sal_t, use_container_width=True, hide_index=True)

    with sal_s3:
        st.markdown("**Salaire vs Expérience**")
        df_sal["exp_cat"] = df_sal["experience_annees"].apply(cat_exp)
        df_scatter = df_sal[df_sal["exp_cat"] != "Non renseigné"]
        if not df_scatter.empty:
            fig = px.strip(df_scatter, x="exp_cat", y="salaire_min", color="source",
                           height=300,
                           labels={"exp_cat": "", "salaire_min": "Salaire (€)"},
                           category_orders={"exp_cat": ["Junior (0-2 ans)", "Confirmé (3-5 ans)", "Senior (5+ ans)"]})
            fig.update_layout(margin=dict(t=0, b=0))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Pas assez de données.")