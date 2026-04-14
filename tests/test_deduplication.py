import re
import hashlib
import pytest


# Copie standalone de generate_dedup_key depuis glue_jobs/transform_jobs.py
def generate_dedup_key(titre, entreprise, ville):
    def norm(s):
        if not s:
            return ""
        # Lowercase + suppression espaces multiples pour maximiser les matches cross-sources
        return re.sub(r"\s+", " ", s.lower().strip())
    key = f"{norm(titre)}|{norm(entreprise)}|{norm(ville)}"
    return hashlib.md5(key.encode()).hexdigest()


# --- Tests generate_dedup_key ---
# On teste la déduplication car une offre peut apparaître sur FT et WTTJ en même temps.
# Si le hash est mal calculé, on garde des doublons qui gonflent les stats.
# Si le hash est trop strict (casse, espaces), on rate des vrais doublons.

def test_meme_offre_meme_hash():
    # Cas de base pour vérifier la stabilité de la fonction
    h1 = generate_dedup_key("Data Engineer", "Capgemini", "Paris")
    h2 = generate_dedup_key("Data Engineer", "Capgemini", "Paris")
    assert h1 == h2


def test_casse_differente_meme_hash():
    # La normalisation lowercase doit absorber cette différence
    h1 = generate_dedup_key("Data Engineer", "Capgemini", "Paris")
    h2 = generate_dedup_key("DATA ENGINEER", "CAPGEMINI", "PARIS")
    assert h1 == h2


def test_espaces_multiples_meme_hash():
    # Les deux sources peuvent avoir des espacements différents dans les champs
    h1 = generate_dedup_key("Data Engineer", "Capgemini", "Paris")
    h2 = generate_dedup_key("Data  Engineer", "Capgemini  ", "  Paris")
    assert h1 == h2


def test_offres_differentes_hash_differents():
    # Deux offres distinctes doivent avoir des hash différents
    h1 = generate_dedup_key("Data Engineer", "Capgemini", "Paris")
    h2 = generate_dedup_key("Data Analyst", "Sopra Steria", "Lyon")
    assert h1 != h2
