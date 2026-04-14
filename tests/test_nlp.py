import re
import pytest

SKILLS_KEYWORDS = [
    "sql", "python", "spark", "pyspark", "scala", "java",
    "dbt", "airflow", "kafka", "hadoop", "snowflake", "databricks",
    "powerbi", "power bi", "tableau", "looker", "pandas", "numpy",
    "scikit-learn", "aws", "azure", "gcp", "google cloud",
    "s3", "glue", "athena", "lambda", "docker", "kubernetes", "k8s",
    "terraform", "git", "github", "gitlab", "jenkins", "ci/cd",
    "linux", "bash", "shell", "machine learning", "deep learning",
    "nlp", "llm", "tensorflow", "pytorch", "mlflow", "mlops",
    "xgboost", "lightgbm", "agile", "scrum", "api", "rest",
    "selenium", "pytest", "jira", "sonarqube", "splunk",
]


# Copie standalone de extract_skills depuis glue_jobs/transform_jobs.py
def extract_skills(text):
    if not text:
        return []
    text_lower = text.lower()
    found = []
    for skill in SKILLS_KEYWORDS:
        # \b = word boundary — évite les faux positifs (ex: "nosql" ne matche pas "sql")
        pattern = r"\b" + re.escape(skill) + r"\b"
        if re.search(pattern, text_lower):
            found.append(skill)
    return list(set(found))


# --- Tests extract_skills ---
# On teste extract_skills car c'est le coeur du NLP du projet.

def test_detect_python_et_sql():
    # Cas de base : deux skills courants dans une phrase réaliste
    skills = extract_skills("Maîtrise de Python et SQL requise")
    assert "python" in skills
    assert "sql" in skills


def test_detect_aws_et_docker():
    # Skills cloud/devops fréquents dans les offres data
    skills = extract_skills("Expérience AWS et Docker souhaitée")
    assert "aws" in skills
    assert "docker" in skills


def test_texte_vide():
    # Cas None et chaîne vide — fréquents quand la description est absente
    # La fonction doit retourner une liste vide sans planter
    assert extract_skills("") == []
    assert extract_skills(None) == []


def test_word_boundary_nosql():
    # "nosql" contient "sql" mais ne doit pas le détecter
    skills = extract_skills("Expérience nosql database")
    assert "sql" not in skills


def test_multiple_skills():
    # Stack technique réaliste, tous les skills doivent être détectés
    text = "Stack : Python, Spark, Airflow, dbt, AWS, Docker, Git, CI/CD"
    skills = extract_skills(text)
    for expected in ["python", "spark", "airflow", "dbt", "aws", "docker", "git", "ci/cd"]:
        assert expected in skills
