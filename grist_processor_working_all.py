# -*- coding: utf-8 -*-
import os
import sys
import re
import json as json_module
import requests
import unicodedata
import repetable_processor as rp
from dotenv import load_dotenv
from datetime import datetime
from queries import get_demarche, get_dossier, get_demarche_dossiers, dossier_to_flat_data, format_complex_json_for_grist
from queries_graphql import get_demarche_dossiers_filtered


# Configuration du niveau de log
LOG_LEVEL = 1  # 0=minimal, 1=normal, 2=verbose

def log(message, level=1):
    """Fonction de log conditionnelle selon le niveau défini"""
    if level <= LOG_LEVEL:
        print(message)

def log_verbose(message):
    """Log uniquement en mode verbose"""
    log(message, 2)

def log_error(message):
    """Log d'erreur (toujours affiché)"""
    print(f"ERREUR: {message}")

# APRÈS avoir défini les fonctions de log, importez le module schema_utils
try:
    from schema_utils import (
    get_demarche_schema, 
    create_columns_from_schema, 
    update_grist_tables_from_schema,
    get_demarche_schema_enhanced  # NOUVELLE FONCTION OPTIMISÉE
)
    log("Module schema_utils trouvé et chargé avec succès.")
except ImportError:
    log_error("Module schema_utils non trouvé. La création de schéma avancée ne sera pas disponible.")
    # Configuration de migration progressive
# Configuration : version optimisée stabilisée
def get_optimized_schema(demarche_number):
    """
    Récupération optimisée du schéma avec fallback automatique.
    """
    try:
        log("📊 Récupération optimisée du schéma")
        return get_demarche_schema_enhanced(demarche_number, prefer_robust=True)
    except Exception as e:
        log_error(f"Erreur version optimisée: {e}")
        log("🔄 Fallback vers version classique")
        return get_demarche_schema(demarche_number)

def log_schema_improvements(schema, demarche_number):
    """Affiche les améliorations apportées par la nouvelle version"""
    if schema.get("metadata", {}).get("optimized"):
        log("🎯 AMÉLIORATIONS DÉTECTÉES:")
        revision_id = schema.get("metadata", {}).get("revision_id", "N/A")
        retrieved_at = schema.get("metadata", {}).get("retrieved_at", "N/A")
        log(f"   🔍 Révision active: {revision_id}")
        log(f"   ⏱️  Récupéré à: {retrieved_at}")
        log("   🧹 Filtrage automatique des champs problématiques activé")
        log("   🔄 Gestion robuste des erreurs activée")
        log("   📊 Métadonnées enrichies disponibles")

# Fonction pour supprimer les accents d'une chaîne de caractères
def normalize_column_name(name, max_length=50):
    """
    Normalise un nom de colonne pour Grist en garantissant des identifiants valides.
    Supprime les espaces en début, fin et les espaces consécutifs.
    
    Args:
        name: Le nom original de la colonne
        max_length: Longueur maximale autorisée (défaut: 50)
        
    Returns:
        str: Nom de colonne normalisé pour Grist
    """
    if not name:
        return "column"
    
    # Supprimer les espaces en début et fin, et remplacer les espaces consécutifs par un seul espace
    import re
    name = name.strip()
    name = re.sub(r'\s+', ' ', name)
    
    # Supprimer les accents
    import unicodedata
    name = unicodedata.normalize('NFKD', name)
    name = ''.join([c for c in name if not unicodedata.combining(c)])
    
    # Convertir en minuscules et remplacer les caractères non alphanumériques par des underscores
    name = name.lower()
    name = re.sub(r'[^a-z0-9_]', '_', name)
    
    # Éliminer les underscores multiples consécutifs
    name = re.sub(r'_+', '_', name)
    
    # Éliminer les underscores en début et fin
    name = name.strip('_')
    
    # S'assurer que le nom commence par une lettre
    if not name or not name[0].isalpha():
        name = "col_" + (name or "")
    
    # Tronquer si nécessaire à max_length caractères
    if len(name) > max_length:
        # Générer un hash pour garantir l'unicité
        import hashlib
        hash_part = hashlib.md5(name.encode()).hexdigest()[:6]
        name = f"{name[:max_length-7]}_{hash_part}"
    
    return name

# 1. D'abord, ajoutez la fonction filter_record_to_existing_columns après les autres fonctions utilitaires

def filter_record_to_existing_columns(client, table_id, record):
    """
    Filtre un enregistrement pour ne garder que les colonnes existantes dans la table.
    
    Args:
        client: Instance de GristClient
        table_id: ID de la table Grist
        record: Dictionnaire de l'enregistrement à filtrer
        
    Returns:
        dict: Enregistrement filtré
    """
    # Récupérer les colonnes existantes
    try:
        url = f"{client.base_url}/docs/{client.doc_id}/tables/{table_id}/columns"
        response = requests.get(url, headers=client.headers)
        
        if response.status_code != 200:
            log_error(f"Erreur lors de la récupération des colonnes: {response.status_code}")
            return record  # Retourner l'enregistrement tel quel en cas d'erreur
        
        columns_data = response.json()
        existing_columns = set()
        
        if "columns" in columns_data:
            for col in columns_data["columns"]:
                existing_columns.add(col.get("id"))
        
        log_verbose(f"Colonnes existantes dans la table {table_id}: {len(existing_columns)}")
        
        # Filtrer l'enregistrement
        filtered_record = {}
        for key, value in record.items():
            if key in existing_columns:
                filtered_record[key] = value
            else:
                log_verbose(f"  Colonne {key} ignorée car inexistante dans la table")
        
        # Toujours garder dossier_number pour les références
        if "dossier_number" in record and "dossier_number" not in filtered_record:
            filtered_record["dossier_number"] = record["dossier_number"]
        
        return filtered_record
    
    except Exception as e:
        log_error(f"Erreur lors du filtrage de l'enregistrement: {str(e)}")
        return record  # Retourner l'enregistrement tel quel en cas d'erreur



def detect_column_types_from_multiple_dossiers(dossiers_data, problematic_ids=None):
    """
    Détecte les types de colonnes pour les tables Grist à partir des données de plusieurs dossiers.
    """
    # Colonnes fixes pour la table des dossiers
    dossier_columns = [
        {"id": "dossier_id", "type": "Text"},
        {"id": "number", "type": "Int"},
        {"id": "state", "type": "Text"},
        {"id": "date_depot", "type": "DateTime"},
        {"id": "date_derniere_modification", "type": "DateTime"},
        {"id": "date_traitement", "type": "DateTime"},
        {"id": "demandeur_type", "type": "Text"},
        {"id": "demandeur_civilite", "type": "Text"},
        {"id": "demandeur_nom", "type": "Text"},
        {"id": "demandeur_prenom", "type": "Text"},
        {"id": "demandeur_email", "type": "Text"},
        {"id": "demandeur_siret", "type": "Text"},
        {"id": "entreprise_raison_sociale", "type": "Text"},
        {"id": "usager_email", "type": "Text"},
        {"id": "groupe_instructeur_id", "type": "Text"},
        {"id": "groupe_instructeur_number", "type": "Int"},
        {"id": "groupe_instructeur_label", "type": "Text"},
        {"id": "supprime_par_usager", "type": "Bool"},
        {"id": "date_suppression", "type": "DateTime"},
        {"id": "label_names", "type": "Text"},
        {"id": "labels_json", "type": "Text"}
    ]

    # Colonnes de base pour la table des champs
    champ_columns = [
        {"id": "dossier_number", "type": "Int"},
    ]
    
    # Colonnes de base pour la table des annotations
    annotation_columns = [
        {"id": "dossier_number", "type": "Int"},
    ]

    # Dictionnaires pour suivre les types uniques
    unique_champ_columns = {}
    unique_annotation_columns = {}

    # Indicateurs de présence
    has_repetable_blocks = False
    has_carto_fields = False

    # Fonction pour déterminer le type de colonne
    def determine_column_type(value):
        if value is None:
            return "Text"
        elif isinstance(value, int):
            return "Int"
        elif isinstance(value, float):
            return "Numeric"
        elif isinstance(value, bool):
            return "Bool"
        elif isinstance(value, (datetime, str)) and (
            isinstance(value, datetime) or 
            any(fmt in value for fmt in ["-", "T", ":"])
        ):
            return "DateTime"
        else:
            return "Text"

    # Fonction récursive pour vérifier les champs
    def check_for_repetable_and_carto(champs):
        nonlocal has_repetable_blocks, has_carto_fields
        
        for champ in champs:
            # Ignorer les types HeaderSectionChamp et ExplicationChamp
            if champ["__typename"] in ["HeaderSectionChamp", "ExplicationChamp"]:
                continue
                
            if champ["__typename"] == "RepetitionChamp":
                has_repetable_blocks = True
                # Vérifier les champs à l'intérieur des blocs répétables
                for row in champ.get("rows", []):
                    if "champs" in row:
                        for field in row["champs"]:
                            # Ignorer les types HeaderSectionChamp et ExplicationChamp dans les blocs répétables
                            if field["__typename"] in ["HeaderSectionChamp", "ExplicationChamp"]:
                                continue
                                
                            if field["__typename"] == "CarteChamp":
                                has_carto_fields = True
                                return  # Sortir dès qu'on a trouvé les deux types
            elif champ["__typename"] == "CarteChamp":
                has_carto_fields = True

    # Analyser tous les dossiers pour détecter les colonnes et les types de champs
    for dossier_data in dossiers_data:
        # Utiliser dossier_to_flat_data avec exclude_repetition_champs=True
        # pour exclure les blocs répétables de la table des champs
        flat_data = dossier_to_flat_data(dossier_data, exclude_repetition_champs=True, problematic_ids=problematic_ids)
        
        # Collecter les champs
        for champ in flat_data["champs"]:
            # Ignorer les champs de type HeaderSectionChamp et ExplicationChamp
            if champ["type"] in ["HeaderSectionChamp", "ExplicationChamp"]:
                continue

            # Ignorer les champs dont l'ID est dans la liste des problématiques
            if problematic_ids and champ.get("id") in problematic_ids:
                continue
                
            champ_label = normalize_column_name(champ["label"])
            
            if champ_label not in unique_champ_columns:
                column_type = determine_column_type(champ.get("value"))
                unique_champ_columns[champ_label] = column_type

    

        # Collecter les annotations
        for annotation in flat_data["annotations"]:
            # Ignorer les annotations de type HeaderSectionChamp et ExplicationChamp
            if annotation["type"] in ["HeaderSectionChamp", "ExplicationChamp"]:
                continue
                
            # Enlever le préfixe "annotation_" pour le nom de colonne dans la table des annotations
            original_label = annotation["label"]
            if original_label.startswith("annotation_"):
                annotation_label = normalize_column_name(original_label[11:])  # enlever "annotation_"
            else:
                annotation_label = normalize_column_name(original_label)
            
            if annotation_label not in unique_annotation_columns:
                column_type = determine_column_type(annotation.get("value"))
                unique_annotation_columns[annotation_label] = column_type
        
        # Vérifier la présence de blocs répétables et de champs cartographiques
        check_for_repetable_and_carto(dossier_data.get("champs", []))
        if not (has_repetable_blocks and has_carto_fields):  # Continuer seulement si on n'a pas encore trouvé les deux
            check_for_repetable_and_carto(dossier_data.get("annotations", []))
        
        if has_repetable_blocks and has_carto_fields:
            break  # Sortir de la boucle si on a déjà trouvé les deux types

    # Ajouter les colonnes uniques détectées
    for col_name, col_type in unique_champ_columns.items():
        champ_columns.append({
            "id": col_name,
            "type": col_type
        })
        
    # Ajouter les colonnes uniques d'annotations détectées
    for col_name, col_type in unique_annotation_columns.items():
        annotation_columns.append({
            "id": col_name,
            "type": col_type
        })

    # Préparer le résultat
    result = {
        "dossier": dossier_columns,
        "champs": champ_columns,
        "annotations": annotation_columns,
        "has_repetable_blocks": has_repetable_blocks,
        "has_carto_fields": has_carto_fields
    }
    
    # Ne détecter les colonnes des blocs répétables que si nécessaire
    if has_repetable_blocks:
        try:
            import repetable_processor as rp
            repetable_columns = rp.detect_repetable_columns_from_multiple_dossiers(dossiers_data)
            result["repetable_rows"] = repetable_columns
        except Exception as e:
            log_error(f"Erreur lors de la détection des colonnes des blocs répétables: {str(e)}")
            import traceback
            traceback.print_exc()
            # Fournir au moins une structure de base en cas d'erreur
            result["repetable_rows"] = [
                {"id": "dossier_number", "type": "Int"},
                {"id": "block_label", "type": "Text"},
                {"id": "block_row_index", "type": "Int"},
                {"id": "block_row_id", "type": "Text"}
            ]

    return result

def get_problematic_descriptor_ids(demarche_number):
    """
    Récupère les IDs des descripteurs de champs problématiques (HeaderSectionChamp et ExplicationChamp)
    pour une démarche donnée.
    """
    from queries_config import API_TOKEN, API_URL
    import requests
    
    # Requête GraphQL pour récupérer les descripteurs de champs
    query = """
    query getDemarche($demarcheNumber: Int!) {
      demarche(number: $demarcheNumber) {
        activeRevision {
          champDescriptors {
            __typename
            id
            type
          }
        }
      }
    }
    """
    
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json"
    }
    
    response = requests.post(
        API_URL,
        json={"query": query, "variables": {"demarcheNumber": int(demarche_number)}},
        headers=headers
    )
    
    response.raise_for_status()
    result = response.json()
    
    problematic_ids = set()
    
    # Vérifier les erreurs
    if "errors" in result:
        log_error(f"GraphQL errors: {', '.join([error.get('message', 'Unknown error') for error in result['errors']])}")
        return problematic_ids
    
    # Extraire les IDs des champs problématiques
    if (result.get("data") and result["data"].get("demarche") and 
        result["data"]["demarche"].get("activeRevision") and 
        result["data"]["demarche"]["activeRevision"].get("champDescriptors")):
        
        descriptors = result["data"]["demarche"]["activeRevision"]["champDescriptors"]
        
        for descriptor in descriptors:
            if descriptor.get("type") in ["header_section", "explication"] or descriptor.get("__typename") in ["HeaderSectionChampDescriptor", "ExplicationChampDescriptor"]:
                problematic_ids.add(descriptor.get("id"))
    
    log(f"Nombre de descripteurs problématiques identifiés: {len(problematic_ids)}")
    return problematic_ids

def format_value_for_grist(value, value_type):
    if value is None:
        return None

    if value_type == "DateTime":
        if isinstance(value, str):
            if value:
                for fmt in ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]:
                    try:
                        dt = datetime.strptime(value, fmt)
                        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                    except ValueError:
                        continue
            return value
        return value

    if value_type == "Text":
        if isinstance(value, str) and len(value) > 1000:
            return value[:1000] + "..."
        return str(value)

    if value_type in ["Int", "Numeric"]:
        try:
            if value_type == "Int":
                return int(float(value)) if value else None
            return float(value) if value else None
        except (ValueError, TypeError):
            return None

    if value_type == "Bool":
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ["true", "1", "yes", "oui", "vrai"]
        return bool(value)

    return value

class ColumnCache:
    """
    Classe pour mettre en cache les informations sur les colonnes de tables Grist,
    évitant ainsi des requêtes répétées pour obtenir la structure des tables.
    """
    def __init__(self, client):
        self.client = client
        self.columns_cache = {}  # {table_id: {column_id: column_type}}
    
    def get_columns(self, table_id, force_refresh=False):
        """
        Récupère les colonnes d'une table, en utilisant le cache si disponible.
        
        Args:
            table_id: ID de la table Grist
            force_refresh: Force la récupération depuis l'API même si en cache
            
        Returns:
            set: Ensemble des IDs de colonnes
        """
        if table_id not in self.columns_cache or force_refresh:
            log_verbose(f"Récupération des colonnes pour la table {table_id}")
            url = f"{self.client.base_url}/docs/{self.client.doc_id}/tables/{table_id}/columns"
            response = requests.get(url, headers=self.client.headers)
            
            if response.status_code == 200:
                columns_data = response.json()
                column_ids = set()
                column_types = {}
                
                if "columns" in columns_data:
                    for col in columns_data["columns"]:
                        col_id = col.get("id")
                        col_type = col.get("type", "Text")
                        if col_id:
                            column_ids.add(col_id)
                            column_types[col_id] = col_type
                
                self.columns_cache[table_id] = {"ids": column_ids, "types": column_types}
                log_verbose(f"  {len(column_ids)} colonnes en cache pour {table_id}")
            else:
                log_error(f"Erreur lors de la récupération des colonnes: {response.status_code}")
                self.columns_cache[table_id] = {"ids": set(), "types": {}}
        
        return self.columns_cache[table_id]["ids"]
    
    def get_column_type(self, table_id, column_id):
        """
        Récupère le type d'une colonne spécifique.
        
        Args:
            table_id: ID de la table Grist
            column_id: ID de la colonne
            
        Returns:
            str: Type de la colonne ou "Text" par défaut
        """
        if table_id not in self.columns_cache:
            self.get_columns(table_id)
        
        return self.columns_cache[table_id]["types"].get(column_id, "Text")
    
    def add_missing_columns(self, table_id, missing_columns, column_types=None):
        """
        Ajoute les colonnes manquantes et met à jour le cache.
        
        Args:
            table_id: ID de la table
            missing_columns: Liste des noms de colonnes manquantes
            column_types: Dictionnaire des types de colonnes
            
        Returns:
            tuple: (bool succès, dict mapping des noms de colonnes)
        """
        if not missing_columns:
            return True, {}
        
        # Obtenir les colonnes existantes
        existing_columns = self.get_columns(table_id)
        
        # Ne garder que les colonnes réellement manquantes
        columns_to_add = []
        column_mapping = {}
        
        for col_name in missing_columns:
            normalized_col_name = normalize_column_name(col_name)
            column_mapping[col_name] = normalized_col_name
            
            if normalized_col_name not in existing_columns:
                # Déterminer le type
                col_type = "Text"
                if column_types and "champs" in column_types:
                    champ_column_types = {col["id"]: col["type"] for col in column_types["champs"]}
                    if col_name in champ_column_types:
                        col_type = champ_column_types[col_name]
                
                columns_to_add.append({"id": normalized_col_name, "type": col_type})
        
        if not columns_to_add:
            return True, column_mapping
        
        # Ajouter les colonnes
        url = f"{self.client.base_url}/docs/{self.client.doc_id}/tables/{table_id}/columns"
        payload = {"columns": columns_to_add}
        
        log(f"  Ajout de {len(columns_to_add)} colonnes à la table {table_id}")
        response = requests.post(url, headers=self.client.headers, json=payload)
        
        if response.status_code == 200:
            log(f"  {len(columns_to_add)} colonnes ajoutées avec succès à la table {table_id}")
            
            # Mettre à jour le cache
            if table_id in self.columns_cache:
                for col in columns_to_add:
                    self.columns_cache[table_id]["ids"].add(col["id"])
                    self.columns_cache[table_id]["types"][col["id"]] = col["type"]
            
            return True, column_mapping
        else:
            log_error(f"  Erreur lors de l'ajout des colonnes: {response.status_code} - {response.text}")
            return False, column_mapping

import concurrent.futures
import time

def fetch_dossiers_in_parallel(dossier_numbers, max_workers=2, timeout=120):
    """
    Récupère plusieurs dossiers en parallèle.
    
    Args:
        dossier_numbers: Liste des numéros de dossiers à récupérer
        max_workers: Nombre maximum de threads à utiliser
        timeout: Délai d'attente maximum par dossier en secondes
        
    Returns:
        dict: Dictionnaire {dossier_number: dossier_data}
    """
    results = {}
    errors = []
    
    def fetch_dossier(dossier_number):
        try:
            start_time = time.time()
            dossier_data = get_dossier(dossier_number)
            elapsed = time.time() - start_time
            log_verbose(f"Dossier {dossier_number} récupéré en {elapsed:.2f}s")
            return dossier_number, dossier_data
        except Exception as e:
            log_error(f"Erreur lors de la récupération du dossier {dossier_number}: {str(e)}")
            return dossier_number, None
    
    log(f"Récupération en parallèle de {len(dossier_numbers)} dossiers avec {max_workers} workers...")
    
    # Utiliser ThreadPoolExecutor pour le parallélisme
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Soumettre les tâches
        future_to_dossier = {
            executor.submit(fetch_dossier, dossier_num): dossier_num 
            for dossier_num in dossier_numbers
        }
        
        # Traiter les résultats au fur et à mesure
        for future in concurrent.futures.as_completed(future_to_dossier, timeout=timeout):
            dossier_num = future_to_dossier[future]
            try:
                dossier_num, dossier_data = future.result()
                if dossier_data:
                    results[dossier_num] = dossier_data
                else:
                    errors.append(dossier_num)
            except Exception as e:
                log_error(f"Exception pour le dossier {dossier_num}: {str(e)}")
                errors.append(dossier_num)
    
    success_rate = len(results) / len(dossier_numbers) * 100 if dossier_numbers else 0
    log(f"Récupération parallèle terminée: {len(results)}/{len(dossier_numbers)} dossiers récupérés ({success_rate:.1f}%)")
    
    if errors:
        log(f"Échecs: {len(errors)} dossiers n'ont pas pu être récupérés")
    
    return results

def process_dossiers_in_parallel(client, dossier_data_dict, table_ids, column_types, problematic_ids=None, max_workers=3):
    """
    Traite plusieurs dossiers en parallèle pour Grist.
    
    Args:
        client: Instance de GristClient (doit être thread-safe)
        dossier_data_dict: Dictionnaire {dossier_number: dossier_data}
        table_ids: IDs des tables Grist
        column_types: Types de colonnes
        problematic_ids: IDs des descripteurs à filtrer
        max_workers: Nombre maximum de threads
        
    Returns:
        tuple: (success_count, error_count)
    """
    results = {}
    
    def process_single_dossier(dossier_number, dossier_data):
        try:
            success = process_dossier_for_grist(client, dossier_data, table_ids, column_types, problematic_ids)
            return dossier_number, success
        except Exception as e:
            log_error(f"Exception lors du traitement du dossier {dossier_number}: {str(e)}")
            import traceback
            traceback.print_exc()
            return dossier_number, False
    
    log(f"Traitement en parallèle de {len(dossier_data_dict)} dossiers avec {max_workers} workers...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Soumettre les tâches
        future_to_dossier = {
            executor.submit(process_single_dossier, dossier_num, dossier_data): dossier_num
            for dossier_num, dossier_data in dossier_data_dict.items()
        }
        
        # Traiter les résultats au fur et à mesure
        for i, future in enumerate(concurrent.futures.as_completed(future_to_dossier)):
            dossier_num = future_to_dossier[future]
            try:
                dossier_num, success = future.result()
                results[dossier_num] = success
                
                # Afficher la progression
                if (i+1) % 10 == 0 or i+1 == len(dossier_data_dict):
                    success_so_far = sum(1 for result in results.values() if result)
                    log(f"Progression: {i+1}/{len(dossier_data_dict)} dossiers traités, {success_so_far} succès")
            except Exception as e:
                log_error(f"Exception pour le dossier {dossier_num}: {str(e)}")
                results[dossier_num] = False
    
    success_count = sum(1 for result in results.values() if result)
    error_count = len(results) - success_count
    
    return success_count, error_count


# Fonction pour récupérer les labels d'un dossier spécifique
def get_dossier_labels(dossier_number):
    """Récupère uniquement les labels d'un dossier spécifique"""
    from queries_config import API_TOKEN, API_URL
    
    query = """
    query GetDossierLabels($dossierNumber: Int!) {
        dossier(number: $dossierNumber) {
            id
            number
            labels {
                id
                name
                color
            }
        }
    }
    """
    
    variables = {"dossierNumber": int(dossier_number)}
    
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json"
    }
    
    response = requests.post(
        API_URL,
        json={"query": query, "variables": variables},
        headers=headers
    )
    
    if response.status_code != 200:
        log_error(f"Erreur HTTP lors de la récupération des labels: {response.status_code}")
        return None
    
    result = response.json()
    
    if "errors" in result:
        log_error(f"Erreurs GraphQL lors de la récupération des labels")
        return None
    
    return result.get("data", {}).get("dossier", {}).get("labels", [])

# Fonction pour ajouter des colonnes manquantes à une table Grist
def add_missing_columns_to_table(client, table_id, missing_columns, column_types=None):
    """
    Ajoute les colonnes manquantes à une table Grist existante.
    Vérifie que l'ajout a bien fonctionné avant de continuer.
    
    Args:
        client: Instance de GristClient
        table_id: ID de la table
        missing_columns: Liste des noms de colonnes manquantes
        column_types: Dictionnaire des types de colonnes (optionnel)
    
    Returns:
        tuple: (bool succès, dict mapping des noms de colonnes)
    """
    try:
        if not missing_columns:
            return True, {}  # Rien à ajouter
            
        # Mapping des noms originaux vers les noms normalisés
        column_mapping = {}
        columns_to_add = []
        
        for col_name in missing_columns:
            # Normaliser le nom de colonne
            normalized_col_name = normalize_column_name(col_name)
            column_mapping[col_name] = normalized_col_name
            
            # Déterminer le type de colonne (Text par défaut)
            col_type = "Text"
            
            # Si column_types est fourni, essayer de trouver le type
            if column_types and "champs" in column_types:
                champ_column_types = {col["id"]: col["type"] for col in column_types["champs"]}
                if col_name in champ_column_types:
                    col_type = champ_column_types[col_name]
            
            # Ajouter la définition de colonne
            columns_to_add.append({"id": normalized_col_name, "type": col_type})
        
        if not columns_to_add:
            return True, column_mapping
            
        # Ajouter les colonnes à la table
        url = f"{client.base_url}/docs/{client.doc_id}/tables/{table_id}/columns"
        payload = {"columns": columns_to_add}
        
        log(f"  Ajout de {len(columns_to_add)} colonnes à la table {table_id}")
        for col in columns_to_add:
            log_verbose(f"  - Ajout de la colonne '{col['id']}' (type: {col['type']})")
            
        response = requests.post(url, headers=client.headers, json=payload)
        
        if response.status_code == 200:
            log(f"  {len(columns_to_add)} colonnes ajoutées avec succès à la table {table_id}")
            
            # Vérifier que les colonnes ont bien été ajoutées
            verify_url = f"{client.base_url}/docs/{client.doc_id}/tables/{table_id}/columns"
            verify_response = requests.get(verify_url, headers=client.headers)
            
            if verify_response.status_code == 200:
                columns_data = verify_response.json()
                existing_column_ids = set()
                
                if "columns" in columns_data:
                    for col in columns_data["columns"]:
                        existing_column_ids.add(col.get("id"))
                
                # Vérifier quelles colonnes ont bien été ajoutées
                all_added = True
                for col in columns_to_add:
                    if col["id"] not in existing_column_ids:
                        log_error(f"  Colonne '{col['id']}' n'a pas été ajoutée")
                        all_added = False
                
                return all_added, column_mapping
            else:
                log_error(f"  Erreur lors de la vérification des colonnes: {verify_response.status_code} - {verify_response.text}")
                return False, column_mapping
        else:
            log_error(f"  Erreur lors de l'ajout des colonnes: {response.status_code} - {response.text}")
            log_error(f"  Détails: {response.text}")
            return False, column_mapping
            
    except Exception as e:
        log_error(f"  Erreur lors de l'ajout des colonnes: {str(e)}")
        import traceback
        traceback.print_exc()
        return False, column_mapping

def add_id_columns_based_on_annotations(client, table_id, annotations):
    """
    Ajoute des colonnes pour les IDs des annotations basées sur leur label
    """
    columns_to_add = []
    
    for annotation in annotations:
        if "label" not in annotation or "id" not in annotation:
            continue
            
        original_label = annotation["label"]
        if original_label.startswith("annotation_"):
            normalized_label = normalize_column_name(original_label[11:])
        else:
            normalized_label = normalize_column_name(original_label)
            
        id_column = f"{normalized_label}_id"
        columns_to_add.append({"id": id_column, "type": "Text"})
    
    if columns_to_add:
        # Vérifier les colonnes existantes pour éviter des doublons
        url = f"{client.base_url}/docs/{client.doc_id}/tables/{table_id}/columns"
        response = requests.get(url, headers=client.headers)
        
        if response.status_code == 200:
            columns_data = response.json()
            existing_column_ids = set()
            
            if "columns" in columns_data:
                for col in columns_data["columns"]:
                    existing_column_ids.add(col.get("id"))
            
            # Filtrer pour n'ajouter que les colonnes manquantes
            columns_to_add = [col for col in columns_to_add if col["id"] not in existing_column_ids]
        
        if columns_to_add:
            url = f"{client.base_url}/docs/{client.doc_id}/tables/{table_id}/columns"
            payload = {"columns": columns_to_add}
            response = requests.post(url, headers=client.headers, json=payload)
            
            if response.status_code != 200:
                log_error(f"Erreur lors de l'ajout des colonnes d'ID: {response.text}")
            else:
                log(f"Colonnes d'ID ajoutées avec succès: {', '.join(col['id'] for col in columns_to_add)}")
    
    return [col["id"] for col in columns_to_add]


# Classe pour gérer les opérations avec l'API Grist
class GristClient:
    def __init__(self, base_url, api_key, doc_id=None):
        self.base_url = base_url.rstrip('/')  # Enlever le / final s'il y en a un
        self.api_key = api_key
        self.doc_id = doc_id
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        log(f"Initialisation du client Grist avec l'URL de base: {self.base_url}")

    def set_doc_id(self, doc_id):
        self.doc_id = doc_id

    def table_exists(self, table_id):
        """
        Vérifie si une table existe dans le document Grist.
        """
        try:
            tables_data = self.list_tables()

            # Vérification de la structure de tables_data
            if isinstance(tables_data, dict) and 'tables' in tables_data:
                tables = tables_data['tables']
            elif isinstance(tables_data, list):
                tables = tables_data
            else:
                log_verbose(f"Structure inattendue de données de tables: {type(tables_data)}")
                return None

            # Recherche case-insensitive
            for table in tables:
                if isinstance(table, dict) and table.get('id', '').lower() == table_id.lower():
                    log_verbose(f"Table {table_id} trouvée avec l'ID {table.get('id')}")
                    return table

            log_verbose(f"Table {table_id} non trouvée")
            return None

        except Exception as e:
            log_error(f"Erreur lors de la recherche de la table {table_id}: {e}")
            return None

    def get_existing_dossier_numbers(self, table_id):
        if not self.doc_id:
            raise ValueError("Document ID is required")

        url = f"{self.base_url}/docs/{self.doc_id}/tables/{table_id}/records"
        log_verbose(f"Récupération des enregistrements existants depuis {url}")

        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            data = response.json()
            
            log_verbose(f"Nombre total d'enregistrements récupérés: {len(data.get('records', []))}")

            # Chercher les enregistrements avec dossier_number ou number
            dossier_dict = {}
            if 'records' in data and isinstance(data['records'], list):
                for record in data['records']:
                    if isinstance(record, dict) and 'fields' in record and isinstance(record['fields'], dict):
                        record_id = record.get('id')
                        fields = record.get('fields', {})
                        
                        # Vérifier si dossier_number ou number est présent
                        dossier_num = None
                        if 'dossier_number' in fields and fields['dossier_number']:
                            dossier_num = fields['dossier_number']
                            dossier_dict[str(dossier_num)] = record_id
                        elif 'number' in fields and fields['number']:
                            dossier_num = fields['number']
                            dossier_dict[str(dossier_num)] = record_id

            log(f"Nombre de dossiers existants identifiés: {len(dossier_dict)}")
            return dossier_dict
        else:
            log_error(f"Erreur lors de la récupération des enregistrements existants: {response.status_code} - {response.text}")
            return {}

    def upsert_dossier_in_grist(self, table_id, row_dict):
        """
        Insère ou met à jour un dossier dans une table Grist, en filtrant les champs problématiques.
        """
        # Log des champs avant filtrage
        log_verbose(f"Champs dans row_dict avant filtrage: {list(row_dict.keys())}")
        log_verbose(f"Présence de 'label_names': {'label_names' in row_dict}")
        log_verbose(f"Présence de 'labels_json': {'labels_json' in row_dict}")
    
        if 'label_names' in row_dict:
            log_verbose(f"Valeur de 'label_names': {row_dict['label_names']}")
        if 'labels_json' in row_dict:
            log_verbose(f"Valeur de 'labels_json': {row_dict['labels_json']}")
            if not self.doc_id:
                raise ValueError("Document ID is required")

        
        # Vérifier si nous avons le numéro de dossier
        dossier_number = row_dict.get("dossier_number") or row_dict.get("number")

        if not dossier_number:
            log_error("dossier_number ou number manquant dans les données")
            log_verbose(f"Données disponibles: {row_dict.keys()}")
            return False

        # Convertir le numéro de dossier en chaîne pour les comparaisons
        dossier_number_str = str(dossier_number)

        # Récupération des dossiers existants pour vérifier si on doit faire un update ou un insert
        log_verbose(f"Récupération des dossiers existants pour la table {table_id}...")
        existing_records = self.get_existing_dossier_numbers(table_id)
        log_verbose(f"Dossiers existants trouvés: {len(existing_records)}")

        url = f"{self.base_url}/docs/{self.doc_id}/tables/{table_id}/records"

        # S'assurer que le dictionnaire est formaté correctement pour l'API Grist
        # Grist attend des champs sous la forme {"fields": {...}}
        formatted_row = {"fields": row_dict} if "fields" not in row_dict else row_dict

        log_verbose(f"Recherche du dossier {dossier_number_str} dans les enregistrements existants...")
        if dossier_number_str in existing_records:
            # Mise à jour de l'enregistrement existant
            record_id = existing_records[dossier_number_str]
            log_verbose(f"Dossier {dossier_number_str} trouvé avec ID {record_id}, mise à jour...")
            update_payload = {"records": [{"id": record_id, "fields": formatted_row["fields"]}]}
            response = requests.patch(url, headers=self.headers, json=update_payload)
        else:
            # Création d'un nouvel enregistrement
            log_verbose(f"Dossier {dossier_number_str} non trouvé, création d'un nouvel enregistrement...")
            create_payload = {"records": [formatted_row]}
            response = requests.post(url, headers=self.headers, json=create_payload)

        if response.status_code in [200, 201]:
            return True
        else:
            log_error(f"Erreur UPSERT pour {dossier_number_str}: {response.status_code} - {response.text}")
            return False

    def list_documents(self):
        url = f"{self.base_url}/docs"
        log_verbose(f"GET {url}")
        response = requests.get(url, headers=self.headers)
        if response.status_code != 200:
            log_error(f"Erreur {response.status_code}: {response.text}")
            response.raise_for_status()

        data = response.json()
        return data

    def get_document_info(self):
        if not self.doc_id:
            raise ValueError("Document ID is required")
        url = f"{self.base_url}/docs/{self.doc_id}"
        log_verbose(f"GET {url}")
        response = requests.get(url, headers=self.headers)
        if response.status_code != 200:
            log_error(f"Erreur {response.status_code}: {response.text}")
            response.raise_for_status()

        data = response.json()
        return data
    
    def list_tables(self):
        if not self.doc_id:
            raise ValueError("Document ID is required")

        url = f"{self.base_url}/docs/{self.doc_id}/tables"
        log_verbose(f"GET {url}")
        response = requests.get(url, headers=self.headers)
        if response.status_code != 200:
            log_error(f"Erreur {response.status_code}: {response.text}")
            response.raise_for_status()

        data = response.json()
        return data
    
    def create_table(self, table_id, columns):
        if not self.doc_id:
            raise ValueError("Document ID is required")

        url = f"{self.base_url}/docs/{self.doc_id}/tables"
        data = {
            "tables": [
                {
                    "id": table_id,
                    "columns": columns
                }
            ]
        }
        log(f"Création de la table {table_id}")

        for col in columns:
            if 'id' not in col or not col['id']:
                raise ValueError(f"Column missing id: {col}")
            if 'type' not in col or not col['type']:
                raise ValueError(f"Invalid column id '{col['id']}'. Must start with a letter and contain only letters, numbers, and underscores.")

        response = requests.post(url, headers=self.headers, json=data)
        if response.status_code != 200:
            log_error(f"Erreur {response.status_code}: {response.text}")
            response.raise_for_status()

        result = response.json()
        return result

    def create_or_clear_grist_tables(self, demarche_number, column_types):
        """
        Crée ou met à jour les tables Grist pour une démarche.
        """
        try:
            # Récupérer les indicateurs de présence
            has_repetable_blocks = column_types.get("has_repetable_blocks", False)
            has_carto_fields = column_types.get("has_carto_fields", False)
            
            # FILTRAGE EXPLICITE DES COLONNES PROBLÉMATIQUES
            # Retirer toutes les colonnes qui pourraient correspondre à HeaderSectionChamp et ExplicationChamp
            filtered_champ_columns = column_types.get("champs", [])
            for col in column_types.get("champs", []):
                col_id = col.get("id", "").lower()
            # Remplacer les colonnes originales par les colonnes filtrées
            column_types["champs"] = filtered_champ_columns
            
            # Filtrage similaire pour les colonnes d'annotations
            filtered_annotation_columns = column_types.get("annotations", [])
            for col in column_types.get("annotations", []):
                col_id = col.get("id", "").lower()
            # Remplacer les colonnes originales par les colonnes filtrées
            column_types["annotations"] = filtered_annotation_columns
            
            # Définir les IDs de tables
            dossier_table_id = f"Demarche_{demarche_number}_dossiers"
            champ_table_id = f"Demarche_{demarche_number}_champs"
            annotation_table_id = f"Demarche_{demarche_number}_annotations"
            repetable_table_id = f"Demarche_{demarche_number}_repetable_rows" if has_repetable_blocks else None

            # Récupérer les tables existantes
            existing_tables_response = self.list_tables()
            existing_tables = existing_tables_response.get('tables', [])

            # Rechercher les tables existantes (dossiers, champs, annotations, répétables)
            dossier_table = None
            champ_table = None
            annotation_table = None
            repetable_table = None

            for table in existing_tables:
                if isinstance(table, dict):
                    table_id = table.get('id', '').lower()
                    if table_id == dossier_table_id.lower():
                        dossier_table = table
                        dossier_table_id = table.get('id')
                        log(f"Table dossiers existante trouvée avec l'ID {dossier_table_id}")
                    elif table_id == champ_table_id.lower():
                        champ_table = table
                        champ_table_id = table.get('id')
                        log(f"Table champs existante trouvée avec l'ID {champ_table_id}")
                    elif table_id == annotation_table_id.lower():
                        annotation_table = table
                        annotation_table_id = table.get('id')
                        log(f"Table annotations existante trouvée avec l'ID {annotation_table_id}")
                    elif repetable_table_id and table_id == repetable_table_id.lower():
                        repetable_table = table
                        repetable_table_id = table.get('id')
                        log(f"Table répétables existante trouvée avec l'ID {repetable_table_id}")

            # Créer la table des dossiers si elle n'existe pas
            if not dossier_table:
                log(f"Création de la table {dossier_table_id}")
                dossier_table_result = self.create_table(dossier_table_id, column_types["dossier"])
                dossier_table = dossier_table_result['tables'][0]
                dossier_table_id = dossier_table.get('id')

            # Créer la table des champs si elle n'existe pas
            if not champ_table:
                log(f"Création de la table {champ_table_id}")
                champ_table_result = self.create_table(champ_table_id, column_types["champs"])
                champ_table = champ_table_result['tables'][0]
                champ_table_id = champ_table.get('id')
                
            # Créer la table des annotations si elle n'existe pas
            if not annotation_table:
                log(f"Création de la table {annotation_table_id}")
                annotation_table_result = self.create_table(annotation_table_id, column_types["annotations"])
                annotation_table = annotation_table_result['tables'][0]
                annotation_table_id = annotation_table.get('id')

            # Créer la table des blocs répétables seulement si nécessaire
            if has_repetable_blocks and repetable_table_id and not repetable_table and "repetable_rows" in column_types:
                log(f"Création de la table {repetable_table_id}")
                # Commencer avec seulement les colonnes de base pour éviter des erreurs
                base_columns = [
                    {"id": "dossier_number", "type": "Int"},
                    {"id": "block_label", "type": "Text"},
                    {"id": "block_row_index", "type": "Int"},
                    {"id": "block_row_id", "type": "Text"}
                ]
                repetable_table_result = self.create_table(repetable_table_id, base_columns)
                repetable_table = repetable_table_result['tables'][0]
                repetable_table_id = repetable_table.get('id')
                
                # Ajouter les colonnes supplémentaires une par une
                remaining_columns = []
                for col in column_types["repetable_rows"]:
                    if col["id"] not in ["dossier_number", "block_label", "block_row_index", "block_row_id"]:
                        
                        remaining_columns.append(col)
                
                # Ajouter des colonnes cartographiques spécifiques seulement si des champs carto sont présents
                if has_carto_fields:
                    geo_columns = [
                        {"id": "geo_id", "type": "Text"},
                        {"id": "geo_source", "type": "Text"},
                        {"id": "geo_description", "type": "Text"},
                        {"id": "geo_type", "type": "Text"},
                        {"id": "geo_coordinates", "type": "Text"},
                        {"id": "geo_wkt", "type": "Text"},
                        {"id": "geo_commune", "type": "Text"},
                        {"id": "geo_numero", "type": "Text"},
                        {"id": "geo_section", "type": "Text"},
                        {"id": "geo_prefixe", "type": "Text"},
                        {"id": "geo_surface", "type": "Numeric"}
                    ]
                    
                    # Ajouter chaque colonne géographique si elle n'est pas déjà dans remaining_columns
                    for geo_col in geo_columns:
                        if not any(col["id"] == geo_col["id"] for col in remaining_columns):
                            remaining_columns.append(geo_col)
                
                if remaining_columns:
                    log(f"Ajout de {len(remaining_columns)} colonnes supplémentaires à la table des blocs répétables...")
                    try:
                        url = f"{self.base_url}/docs/{self.doc_id}/tables/{repetable_table_id}/columns"
                        add_columns_payload = {"columns": remaining_columns}
                        response = requests.post(url, headers=self.headers, json=add_columns_payload)
                        
                        if response.status_code != 200:
                            log_error(f"Erreur lors de l'ajout des colonnes: {response.text}")
                        else:
                            log("Colonnes ajoutées avec succès")
                    except Exception as e:
                        log_error(f"Erreur lors de l'ajout des colonnes: {str(e)}")
                        import traceback
                        traceback.print_exc()
                        
            elif has_repetable_blocks and repetable_table and repetable_table_id and "repetable_rows" in column_types:
                # La table existe déjà, vérifier que toutes les colonnes sont présentes
                try:
                    url = f"{self.base_url}/docs/{self.doc_id}/tables/{repetable_table_id}/columns"
                    response = requests.get(url, headers=self.headers)
                    
                    if response.status_code == 200:
                        columns_data = response.json()
                        existing_column_ids = set()
                        
                        if "columns" in columns_data:
                            for col in columns_data["columns"]:
                                existing_column_ids.add(col.get("id"))
                        
                        # Trouver les colonnes manquantes
                        missing_columns = []
                        for col in column_types["repetable_rows"]:  
                            if col["id"] not in existing_column_ids:
                                missing_columns.append(col)
                        
                        # Ajouter des colonnes cartographiques spécifiques seulement si des champs carto sont présents
                        if has_carto_fields:
                            geo_columns = [
                                {"id": "geo_id", "type": "Text"},
                                {"id": "geo_source", "type": "Text"},
                                {"id": "geo_description", "type": "Text"},
                                {"id": "geo_type", "type": "Text"},
                                {"id": "geo_coordinates", "type": "Text"},
                                {"id": "geo_wkt", "type": "Text"},
                                {"id": "geo_commune", "type": "Text"},
                                {"id": "geo_numero", "type": "Text"},
                                {"id": "geo_section", "type": "Text"},
                                {"id": "geo_prefixe", "type": "Text"},
                                {"id": "geo_surface", "type": "Numeric"}
                            ]
                            
                            # Ajouter chaque colonne géographique si elle n'est pas déjà présente
                            for geo_col in geo_columns:
                                if geo_col["id"] not in existing_column_ids and not any(col["id"] == geo_col["id"] for col in missing_columns):
                                    missing_columns.append(geo_col)
                        
                        if missing_columns:
                            log(f"Ajout de {len(missing_columns)} colonnes manquantes à la table des blocs répétables...")
                            add_columns_url = f"{self.base_url}/docs/{self.doc_id}/tables/{repetable_table_id}/columns"
                            add_columns_payload = {"columns": missing_columns}
                            add_response = requests.post(add_columns_url, headers=self.headers, json=add_columns_payload)
                            
                            if add_response.status_code != 200:
                                log_error(f"Erreur lors de l'ajout des colonnes: {add_response.text}")
                            else:
                                log("Colonnes ajoutées avec succès")
                    else:
                        log_error(f"Erreur lors de la récupération des colonnes: {response.text}")
                except Exception as e:
                    log_error(f"Erreur lors de la vérification des colonnes: {str(e)}")
                    import traceback
                    traceback.print_exc()

            # Retourner les IDs des tables
            return {
                "dossier_table_id": dossier_table_id,
                "champ_table_id": champ_table_id,
                "annotation_table_id": annotation_table_id,
                "repetable_table_id": repetable_table_id
            }

        except Exception as e:
            log_error(f"Erreur lors de la gestion des tables Grist: {e}")
            import traceback
            traceback.print_exc()
            raise

    # 2. Ensuite, modifiez la méthode upsert_multiple_dossiers_in_grist de la classe GristClient

    def upsert_multiple_dossiers_in_grist(self, table_id, dossiers_list):
        """
        Insère ou met à jour plusieurs dossiers en une seule requête.
        Version corrigée avec gestion appropriée des succès/échecs.
        """
        if not self.doc_id:
            raise ValueError("Document ID is required")
        
        # Récupérer tous les enregistrements existants en une seule requête
        existing_records = self.get_existing_dossier_numbers(table_id)
        log_verbose(f"Récupération de {len(existing_records)} enregistrements existants pour traitement par lot")
        
        # Récupérer les colonnes existantes une seule fois
        existing_columns = set()
        try:
            url = f"{self.base_url}/docs/{self.doc_id}/tables/{table_id}/columns"
            response = requests.get(url, headers=self.headers)
            
            if response.status_code == 200:
                columns_data = response.json()
                if "columns" in columns_data:
                    for col in columns_data["columns"]:
                        existing_columns.add(col.get("id"))
                
                log_verbose(f"Colonnes existantes dans la table {table_id}: {len(existing_columns)}")
        except Exception as e:
            log_error(f"Erreur lors de la récupération des colonnes: {str(e)}")
        
        # Préparer les listes pour les opérations de création et de mise à jour
        to_create = []
        to_update = []
        
        for row_dict in dossiers_list:
            # Filtrer les colonnes qui existent dans la table
            filtered_row_dict = {}
            for key, value in row_dict.items():
                if not existing_columns or key in existing_columns or key == "dossier_number":
                    filtered_row_dict[key] = value
            
            # Obtenir le numéro de dossier
            dossier_number = filtered_row_dict.get("dossier_number") or filtered_row_dict.get("number")
            if not dossier_number:
                log_error("dossier_number ou number manquant dans les données")
                continue
            
            dossier_number_str = str(dossier_number)
            
            if dossier_number_str in existing_records:
                # Mise à jour d'un enregistrement existant
                record_id = existing_records[dossier_number_str]
                to_update.append({"id": record_id, "fields": filtered_row_dict})
            else:
                # Création d'un nouvel enregistrement
                to_create.append({"fields": filtered_row_dict})
        
        # Variables pour suivre les succès
        total_success = 0
        total_errors = 0
        
        # Traitement des mises à jour
        if to_update:
            # Normaliser tous les enregistrements pour qu'ils aient les mêmes champs
            all_update_keys = set()
            for record in to_update:
                all_update_keys.update(record["fields"].keys())
            
            normalized_updates = []
            for record in to_update:
                normalized_fields = {}
                for key in all_update_keys:
                    normalized_fields[key] = record["fields"].get(key, None)
                normalized_updates.append({"id": record["id"], "fields": normalized_fields})
            
            # Traitement spécial pour la table des champs
            if "champ" in table_id.lower():
                log(f"Table des champs détectée - traitement individuel de {len(normalized_updates)} mises à jour...")
                
                update_success = 0
                update_errors = 0
                
                for record in normalized_updates:
                    record_id = record["id"]
                    fields = record["fields"]
                    
                    update_url = f"{self.base_url}/docs/{self.doc_id}/tables/{table_id}/records"
                    update_payload = {"records": [{"id": record_id, "fields": fields}]}
                    update_response = requests.patch(update_url, headers=self.headers, json=update_payload)
                    
                    if update_response.status_code in [200, 201]:
                        update_success += 1
                    else:
                        update_errors += 1
                        log_error(f"Erreur lors de la mise à jour de l'enregistrement {record_id}: {update_response.status_code}")
                
                log(f"Mises à jour individuelles pour la table des champs: {update_success} succès, {update_errors} échecs")
                total_success += update_success
                total_errors += update_errors
            else:
                # Mise à jour par lot pour les autres tables
                update_url = f"{self.base_url}/docs/{self.doc_id}/tables/{table_id}/records"
                update_payload = {"records": normalized_updates}
                update_response = requests.patch(update_url, headers=self.headers, json=update_payload)
                
                if update_response.status_code in [200, 201]:
                    log(f"Mise à jour par lot: {len(normalized_updates)} enregistrements mis à jour avec succès")
                    total_success += len(normalized_updates)
                else:
                    log_error(f"Erreur lors de la mise à jour par lot: {update_response.status_code} - {update_response.text}")
                    
                    # Fallback: essayer individuellement
                    log("Tentative de mise à jour individuelle...")
                    update_success = 0
                    for individual_record in normalized_updates:
                        individual_payload = {"records": [individual_record]}
                        individual_response = requests.patch(update_url, headers=self.headers, json=individual_payload)
                        
                        if individual_response.status_code in [200, 201]:
                            update_success += 1
                        else:
                            total_errors += 1
                            log_error(f"Échec individuel pour {individual_record['id']}")
                    
                    total_success += update_success
                    log(f"Mise à jour individuelle: {update_success}/{len(normalized_updates)} succès")
        
        # Traitement des créations
        if to_create:
            # Normaliser tous les enregistrements de création
            all_create_keys = set()
            for record in to_create:
                all_create_keys.update(record["fields"].keys())
            
            normalized_creations = []
            for record in to_create:
                normalized_fields = {}
                for key in all_create_keys:
                    normalized_fields[key] = record["fields"].get(key, None)
                normalized_creations.append({"fields": normalized_fields})
            
            create_url = f"{self.base_url}/docs/{self.doc_id}/tables/{table_id}/records"
            create_payload = {"records": normalized_creations}
            create_response = requests.post(create_url, headers=self.headers, json=create_payload)
            
            if create_response.status_code in [200, 201]:
                log(f"Création par lot: {len(normalized_creations)} enregistrements créés avec succès")
                total_success += len(normalized_creations)
            else:
                log_error(f"Erreur lors de la création par lot: {create_response.status_code} - {create_response.text}")
                total_errors += len(normalized_creations)
        
        # Retourner le succès global
        success = total_success > 0 and total_errors == 0
        
        # Log du résumé
        if total_success > 0 or total_errors > 0:
            log(f"Résumé upsert table {table_id}: {total_success} succès, {total_errors} échecs")
        
        return success
    
def process_dossier_for_grist(client, dossier_data, table_ids, column_types, problematic_ids=None):
    """
    Traite un dossier pour l'insérer dans les tables Grist.
    """
    try:
        # Vérifier si des blocs répétables doivent être traités
        has_repetable_blocks = column_types.get("has_repetable_blocks", False)
        
        # Extraire les données à plat du dossier, en excluant les blocs répétables
        # pour éviter la duplication avec la table des blocs répétables
        exclude_repetition = has_repetable_blocks  # N'exclure que si on va les traiter séparément
        flat_data = dossier_to_flat_data(dossier_data, exclude_repetition_champs=exclude_repetition, problematic_ids=problematic_ids)
        dossier_info = flat_data["dossier"]
        dossier_number = dossier_info["dossier_number"]
        
        # Convertir le numéro de dossier en chaîne pour toutes les comparaisons
        dossier_number_str = str(dossier_number)

        # Vérifier si le dossier a été supprimé par l'usager
        if "dateSuppressionParUsager" in dossier_data and dossier_data["dateSuppressionParUsager"]:
            # Option 1: Marquer le dossier comme supprimé dans Grist
            log(f"Dossier {dossier_number} marqué comme supprimé par l'usager le {dossier_data['dateSuppressionParUsager']}")
            dossier_info["supprime_par_usager"] = True
            dossier_info["date_suppression"] = dossier_data["dateSuppressionParUsager"]

        # --- PARTIE 1: TRAITEMENT DE LA TABLE DOSSIERS ---
        
        # Préparer l'enregistrement pour la table des dossiers
        dossier_record = {}
        for column in column_types["dossier"]:
            field_id = column["id"]
            field_type = column["type"]

            if field_id in dossier_info:
                value = dossier_info[field_id]
            elif "dossier_" + field_id in dossier_info:
                value = dossier_info["dossier_" + field_id]
            else:
                continue

            dossier_record[field_id] = format_value_for_grist(value, field_type)

        # Ajouter explicitement l'ID du dossier si disponible
        if "dossier_id" in dossier_info:
            dossier_record["dossier_id"] = dossier_info["dossier_id"]

        # Vérifier que dossier_number est présent dans l'enregistrement
        if "number" not in dossier_record:
            dossier_record["number"] = dossier_number
        
        # Traitement des labels
        # Vérifier si les labels sont présents dans dossier_data
        if "labels" not in dossier_data or not dossier_data.get("labels"):
        # Si les labels ne sont pas présents, essayer de les récupérer séparément
            log_verbose(f"Labels non trouvés dans dossier_data, tentative de récupération séparée...")
            labels = get_dossier_labels(dossier_number)

            if labels:
                log_verbose(f"{len(labels)} labels récupérés séparément")

                # Créer label_names
                label_names = [label.get("name", "") for label in labels if label.get("name")]
                dossier_record["label_names"] = ", ".join(label_names) if label_names else ""

                # Créer labels_json
                labels_with_colors = [
                    {
                        "id": label.get("id", ""),
                        "name": label.get("name", ""),
                        "color": label.get("color", "")
                    }
                    for label in labels if label.get("name") and label.get("color")
                ]
                if labels_with_colors:
                    import json
                    dossier_record["labels_json"] = json_module.dumps(labels_with_colors, ensure_ascii=False)
                    log_verbose(f"label_names ajouté: {dossier_record['label_names']}")
                    log_verbose(f"labels_json ajouté: {dossier_record['labels_json']}")
                else:
                    dossier_record["labels_json"] = ""

        # Utiliser upsert pour la table des dossiers
        dossier_table_id = table_ids["dossier_table_id"]
        success_dossier = client.upsert_dossier_in_grist(dossier_table_id, dossier_record)
        if not success_dossier:
            log_error(f"Échec de mise à jour/insertion du dossier {dossier_number} dans la table {dossier_table_id}")
            return False
        
        # --- PARTIE 2: GESTION DES COLONNES MANQUANTES POUR LA TABLE CHAMPS ---
        
        # Récupérer les colonnes existantes dans la table des champs
        champ_table_id = table_ids["champ_table_id"]
        url = f"{client.base_url}/docs/{client.doc_id}/tables/{champ_table_id}/columns"
        response = requests.get(url, headers=client.headers)
        existing_columns = set()
        if response.status_code == 200:
            columns_data = response.json()
            if "columns" in columns_data:
                existing_columns = {col["id"] for col in columns_data["columns"]}

        # Collecter les colonnes manquantes
        missing_columns = []

        for champ in flat_data["champs"]:
            # Ignorer les champs problématiques
            if champ["type"] in ["HeaderSectionChamp", "ExplicationChamp"]:
                continue
        
        champ_label = normalize_column_name(champ["label"])

        # Vérifier si la colonne existe déjà
        if champ_label not in existing_columns:
            log(f"  Colonne {champ_label} non trouvée dans le schéma, sera ajoutée")
            missing_columns.append(champ_label)

            # Mettre à jour l'ensemble des colonnes existantes
            existing_columns.update(missing_columns)

        # --- PARTIE 3: TRAITEMENT DE LA TABLE CHAMPS ---

        # Récupérer les colonnes existantes dans la table des champs
        champ_table_id = table_ids["champ_table_id"]
        url = f"{client.base_url}/docs/{client.doc_id}/tables/{champ_table_id}/columns"
        response = requests.get(url, headers=client.headers)
        existing_columns = set()
        if response.status_code == 200:
            columns_data = response.json()
            if "columns" in columns_data:
                existing_columns = {col["id"] for col in columns_data["columns"]}

        # Collecter les colonnes manquantes
        missing_columns = []

        # Parcourir tous les champs pour détecter les colonnes manquantes
        for champ in flat_data["champs"]:
            # Ignorer les champs problématiques
            if champ["type"] in ["HeaderSectionChamp", "ExplicationChamp"]:
                continue
            
            champ_label = normalize_column_name(champ["label"])

            # Vérifier si la colonne existe déjà
            if champ_label not in existing_columns:
                log(f"  Colonne {champ_label} non trouvée dans le schéma, sera ajoutée")
                missing_columns.append(champ["label"])  # Utiliser le label original ici

        # Ajouter les colonnes manquantes si nécessaire
        if missing_columns:
            success, column_mapping = add_missing_columns_to_table(client, champ_table_id, missing_columns, column_types)
            
            if success:
                # Mettre à jour l'ensemble des colonnes existantes avec les noms normalisés
                existing_columns.update(column_mapping.values())
            else:
                log_error("Échec de l'ajout des colonnes, certaines données pourraient être omises")

        # Préparer un enregistrement unique pour la table des champs
        champs_record = {"dossier_number": dossier_number}
        champ_column_types = {col["id"]: col["type"] for col in column_types["champs"]}

        # Collecter les IDs des champs pour les stocker
        champ_ids = []
        for champ in flat_data["champs"]:
            if champ.get("id"):
                champ_ids.append(str(champ["id"]))
        if champ_ids:
            champs_record["champ_id"] = "_".join(champ_ids)


        # Agréger tous les champs dans un seul enregistrement
        for champ in flat_data["champs"]:
            # Ignorer les champs de type HeaderSectionChamp et ExplicationChamp
            if champ["type"] in ["HeaderSectionChamp", "ExplicationChamp"]:
                continue
                
            # Normaliser le label pour obtenir le nom de colonne
            original_label = champ["label"]
            champ_label = normalize_column_name(original_label)
            
            # Vérifier si la colonne existe dans le schéma Grist
            if champ_label not in existing_columns:
                # Même si on a ajouté les colonnes manquantes, vérifier à nouveau
                log_verbose(f"  Colonne {champ_label} toujours non disponible, ignorée")
                continue

            value = champ.get("value", "")
            # Pour les types complexes, utiliser la représentation JSON si disponible
            if champ["type"] in ["CarteChamp", "AddressChamp", "SiretChamp"] and champ.get("json_value"):
                try:
                    value = json_module.dumps(champ["json_value"], ensure_ascii=False)
                except (TypeError, ValueError):
                    value = str(champ["json_value"])
            
            # Déterminer le type de colonne (par défaut Text)
            column_type = champ_column_types.get(champ_label, "Text")
            champs_record[champ_label] = format_value_for_grist(value, column_type)

        # Traitement de la table des champs
        existing_champs = client.get_existing_dossier_numbers(champ_table_id)

        success_champs = False
        if dossier_number_str in existing_champs:
            # Mise à jour d'un enregistrement existant
            champ_record_id = existing_champs[dossier_number_str]
            update_payload = {"records": [{"id": champ_record_id, "fields": champs_record}]}
            url = f"{client.base_url}/docs/{client.doc_id}/tables/{champ_table_id}/records"
            response = requests.patch(url, headers=client.headers, json=update_payload)
            success_champs = response.status_code in [200, 201]
        else:
            # Création d'un nouvel enregistrement
            create_payload = {"records": [{"fields": champs_record}]}
            url = f"{client.base_url}/docs/{client.doc_id}/tables/{champ_table_id}/records"
            response = requests.post(url, headers=client.headers, json=create_payload)
            success_champs = response.status_code in [200, 201]

        if not success_champs:
            log_error(f"Erreur lors du traitement des champs pour {dossier_number_str}: {response.text}")
            return False
        
        # --- PARTIE 4: GESTION DES COLONNES MANQUANTES POUR LA TABLE ANNOTATIONS ---

        # Récupérer les colonnes existantes dans la table des annotations
        annotation_table_id = table_ids["annotation_table_id"]

        url = f"{client.base_url}/docs/{client.doc_id}/tables/{annotation_table_id}/columns"
        response = requests.get(url, headers=client.headers)

        existing_annotation_columns = set()
        if response.status_code == 200:
            columns_data = response.json()
            if "columns" in columns_data:
                existing_annotation_columns = {col["id"] for col in columns_data["columns"]}

        # Collecter les colonnes d'annotations manquantes
        missing_annotation_columns = []

        for annotation in flat_data["annotations"]:
            # Ignorer les champs problématiques
            if annotation["type"] in ["HeaderSectionChamp", "ExplicationChamp"]:
                continue
                
            # Pour la table des annotations, enlever le préfixe "annotation_"
            original_label = annotation["label"]
            if original_label.startswith("annotation_"):
                annotation_label = normalize_column_name(original_label[11:])  # enlever "annotation_"
            else:
                annotation_label = normalize_column_name(original_label)
            
            # Vérifier si la colonne existe déjà
            if annotation_label not in existing_annotation_columns:
                log(f"  Colonne d'annotation {annotation_label} non trouvée dans le schéma, sera ajoutée")
                missing_annotation_columns.append(annotation_label)

        # Ajouter les colonnes d'annotations manquantes si nécessaire
        if missing_annotation_columns:
            log(f"  Ajout de {len(missing_annotation_columns)} colonnes manquantes à la table {annotation_table_id}")
            add_missing_columns_to_table(client, annotation_table_id, missing_annotation_columns, column_types)
            
            # Mettre à jour l'ensemble des colonnes existantes
            existing_annotation_columns.update(missing_annotation_columns)

        # --- PARTIE 5: TRAITEMENT DE LA TABLE ANNOTATIONS ---

        # Récupérer les colonnes existantes dans la table des annotations
        annotation_table_id = table_ids["annotation_table_id"]

        # Ajouter dynamiquement les colonnes pour les IDs des annotations
        add_id_columns_based_on_annotations(client, annotation_table_id, flat_data["annotations"])

        # Récupérer à nouveau les colonnes après l'ajout des colonnes d'ID
        url = f"{client.base_url}/docs/{client.doc_id}/tables/{annotation_table_id}/columns"
        response = requests.get(url, headers=client.headers)

        existing_annotation_columns = set()
        if response.status_code == 200:
            columns_data = response.json()
            if "columns" in columns_data:
                for col in columns_data["columns"]:
                    existing_annotation_columns.add(col.get("id"))

        # Préparer un enregistrement unique pour la table des annotations
        annotations_record = {"dossier_number": dossier_number}
        annotation_column_types = {col["id"]: col["type"] for col in column_types["annotations"]}

        # Collecter les IDs des annotations pour les stocker
        annotation_ids = []
        for annotation in flat_data["annotations"]:
            if annotation.get("id"):
                annotation_ids.append(str(annotation["id"]))
        if annotation_ids:
            annotations_record["annotation_id"] = "_".join(annotation_ids)
        
        
        # Agréger toutes les annotations dans un seul enregistrement
        for annotation in flat_data["annotations"]:
            # Ignorer les annotations problématiques
            if annotation["type"] in ["HeaderSectionChamp", "ExplicationChamp"]:
                continue
                
            # Pour la table des annotations, enlever le préfixe "annotation_"
            original_label = annotation["label"]
            if original_label.startswith("annotation_"):
                annotation_label = normalize_column_name(original_label[11:])  # enlever "annotation_"
            else:
                annotation_label = normalize_column_name(original_label)
            
            # Vérifier si la colonne existe dans le schéma (même après ajout)
            if annotation_label not in existing_annotation_columns:
                log_verbose(f"  Colonne d'annotation {annotation_label} toujours non disponible, ignorée")
                continue
            
            value = annotation.get("value", "")
            # Pour les types complexes, utiliser la représentation JSON si disponible
            if annotation["type"] in ["CarteChamp", "AddressChamp", "SiretChamp"] and annotation.get("json_value"):
                try:
                    value = json_module.dumps(annotation["json_value"], ensure_ascii=False)
                except (TypeError, ValueError):
                    value = str(annotation["json_value"])
            
            # Déterminer le type de colonne (par défaut Text)
            column_type = annotation_column_types.get(annotation_label, "Text")
            annotations_record[annotation_label] = format_value_for_grist(value, column_type)

            

        # Traitement de la table des annotations
        existing_annotations = client.get_existing_dossier_numbers(annotation_table_id)

        success_annotations = False
        if dossier_number_str in existing_annotations:
            # Mise à jour d'un enregistrement existant
            annotation_record_id = existing_annotations[dossier_number_str]
            update_payload = {"records": [{"id": annotation_record_id, "fields": annotations_record}]}
            url = f"{client.base_url}/docs/{client.doc_id}/tables/{annotation_table_id}/records"
            response = requests.patch(url, headers=client.headers, json=update_payload)
            success_annotations = response.status_code in [200, 201]
        else:
            # Création d'un nouvel enregistrement
            create_payload = {"records": [{"fields": annotations_record}]}
            url = f"{client.base_url}/docs/{client.doc_id}/tables/{annotation_table_id}/records"
            response = requests.post(url, headers=client.headers, json=create_payload)
            success_annotations = response.status_code in [200, 201]

        if not success_annotations:
            log_error(f"Erreur lors du traitement des annotations pour {dossier_number_str}: {response.text}")
            # Ne pas retourner d'erreur si l'enregistrement des annotations échoue, continuer le traitement
            log_error("Continuation du traitement malgré l'échec des annotations")

        # --- PARTIE 6: TRAITEMENT DES BLOCS RÉPÉTABLES ---
        
        # Ne traiter les blocs répétables que si:
        # 1. Des blocs répétables ont été détectés
        # 2. La table des blocs répétables existe
        # 3. Les définitions de colonnes pour les blocs répétables existent
        if has_repetable_blocks and table_ids.get("repetable_table_id") and "repetable_rows" in column_types:
            repetable_table_id = table_ids["repetable_table_id"]
            repetable_column_types = column_types.get("repetable_rows", [])
            
            # Appeler le processeur de blocs répétables
            try:
                import repetable_processor as rp
                success_count, error_count = rp.process_repetables_for_grist(
                    client, 
                    dossier_data,  # Passer les données brutes de l'API, pas les données aplaties
                    repetable_table_id, 
                    repetable_column_types
                )
                log(f"  Blocs répétables traités: {success_count} réussis, {error_count} en échec")
            except Exception as e:
                log_error(f"  Erreur lors du traitement des blocs répétables: {str(e)}")
                import traceback
                traceback.print_exc()
        elif has_repetable_blocks:
            log_verbose(f"  Blocs répétables détectés mais pas de table correspondante configurée")
        
        # Considérer l'opération comme réussie
        return True

    except Exception as e:
        log_error(f"Erreur lors du traitement du dossier {dossier_number} pour Grist: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def process_demarche_for_grist(client, demarche_number):
    """
    Traite une démarche pour l'insérer dans Grist.
    """
    try:
        # Initialiser des ensembles pour suivre les dossiers traités avec succès/échec
        successful_dossiers = set()
        failed_dossiers = set()
        
        # Récupérer les IDs des champs problématiques
        problematic_descriptor_ids = get_problematic_descriptor_ids(demarche_number)
        log(f"Filtrage de {len(problematic_descriptor_ids)} descripteurs de type HeaderSectionChamp et ExplicationChamp")

        # Vérifier que le document Grist existe
        try:
            doc_info = client.get_document_info()
            doc_name = doc_info.get("name", client.doc_id) if isinstance(doc_info, dict) else "Document ID " + client.doc_id
            log(f"Document Grist trouvé: {doc_name}")
        except Exception as e:
            log_error(f"Erreur lors de la vérification du document Grist: {e}")
            return False

        # Récupérer les données de la démarche
        log(f"Récupération des données de la démarche {demarche_number}...")
        demarche_data = get_demarche(demarche_number)
        if not demarche_data:
            log_error(f"Aucune donnée trouvée pour la démarche {demarche_number}")
            return False

        log(f"Démarche trouvée: {demarche_data['title']}")

        # Vérifier que des dossiers sont présents
        if "dossiers" not in demarche_data or "nodes" not in demarche_data["dossiers"]:
            log_error("Aucun dossier trouvé dans la démarche")
            return False

        dossiers = demarche_data["dossiers"]["nodes"]
        total_dossiers = len(dossiers)
        if total_dossiers == 0:
            log_error("La démarche ne contient aucun dossier")
            return False

        log(f"Nombre de dossiers trouvés: {total_dossiers}")

        # Récupérer quelques dossiers pour analyse du schéma
        sample_dossier_details = []
        max_sample_dossiers = min(3, total_dossiers)
        
        for i in range(max_sample_dossiers):
            sample_dossier_number = dossiers[i]["number"]
            log(f"Récupération du dossier {sample_dossier_number} pour détecter les types de colonnes... ({i+1}/{max_sample_dossiers})")
            sample_dossier = get_dossier(sample_dossier_number)
            if sample_dossier:
                sample_dossier_details.append(sample_dossier)
            else:
                log_error(f"Impossible de récupérer le dossier {sample_dossier_number}")
                
        if not sample_dossier_details:
            log_error("Aucun dossier n'a pu être récupéré pour l'analyse du schéma")
            return False

        # Détecter les types de colonnes
        column_types = detect_column_types_from_multiple_dossiers(sample_dossier_details, problematic_ids=problematic_descriptor_ids)
        
        # Récupérer les indicateurs de présence
        has_repetable_blocks = column_types.get("has_repetable_blocks", False)
        has_carto_fields = column_types.get("has_carto_fields", False)
        
        log(f"Types de colonnes détectés:")
        log(f"  - Colonnes dossiers: {len(column_types['dossier'])}")
        log(f"  - Colonnes champs: {len(column_types['champs'])}")
        log(f"  - Colonnes annotations: {len(column_types['annotations'])}")
        log(f"  - Blocs répétables détectés: {'Oui' if has_repetable_blocks else 'Non'}")
        log(f"  - Champs cartographiques détectés: {'Oui' if has_carto_fields else 'Non'}")
        
        if has_repetable_blocks and "repetable_rows" in column_types:
            log_verbose(f"  - Colonnes blocs répétables: {len(column_types['repetable_rows'])}")

        # Créer ou récupérer les tables Grist pour la démarche
        table_ids = client.create_or_clear_grist_tables(demarche_number, column_types)
        
        # Log des table IDs
        log(f"Tables utilisées pour l'importation:")
        log(f"  Table dossiers: {table_ids['dossier_table_id']}")
        log(f"  Table champs: {table_ids['champ_table_id']}")
        log(f"  Table annotations: {table_ids['annotation_table_id']}")
        if table_ids.get('repetable_table_id'):
            log(f"  Table blocs répétables: {table_ids['repetable_table_id']}")
        else:
            log_verbose(f"  Table blocs répétables: Non créée (aucun bloc répétable détecté)")

        # Nouvelle logique de traitement par lots
        batch_size = 100  # Ajustez selon les performances
        success_count = 0
        error_count = 0
        
        # Organiser les dossiers en lots
        dossier_batches = []
        for i in range(0, total_dossiers, batch_size):
            batch = dossiers[i:min(i+batch_size, total_dossiers)]
            dossier_batches.append(batch)
        
        log(f"Dossiers organisés en {len(dossier_batches)} lots")
        
        # Traiter chaque lot
        for batch_idx, batch in enumerate(dossier_batches):
            log(f"Traitement du lot {batch_idx+1}/{len(dossier_batches)} ({len(batch)} dossiers)...")
            
            # Préparer les données pour les tables
            dossier_records = []
            champ_records = []
            annotation_records = []
            
            batch_success = 0
            batch_errors = 0
            dossier_batch_data = []  # Pour stocker les données complètes des dossiers pour les blocs répétables
            
            # Récupérer les données détaillées pour chaque dossier du lot
            for dossier_brief in batch:
                dossier_number = dossier_brief["number"]
                try:
                    # Récupérer les données complètes du dossier
                    dossier_data = get_dossier(dossier_number)
                    
                    # Vérifier si le dictionnaire est vide (dossier inaccessible)
                    if not dossier_data:
                        log_error(f"Dossier {dossier_number} inaccessible en raison de restrictions de permission, ignoré")
                        batch_errors += 1
                        continue
                    
                    # Garder les données complètes pour le traitement des blocs répétables
                    if has_repetable_blocks:
                        dossier_batch_data.append(dossier_data)
                    
                    # Extraire les données pour les 3 tables principales
                    exclude_repetition = has_repetable_blocks  # N'exclure que si on va les traiter séparément
                    flat_data = dossier_to_flat_data(dossier_data, exclude_repetition_champs=exclude_repetition, problematic_ids=problematic_descriptor_ids)
                    
                    # Préparer les données pour la table des dossiers
                    dossier_info = flat_data["dossier"]
                    dossier_record = {}
                    
                    for column in column_types["dossier"]:
                        field_id = column["id"]
                        field_type = column["type"]
                        
                        if field_id in dossier_info:
                            value = dossier_info[field_id]
                        elif "dossier_" + field_id in dossier_info:
                            value = dossier_info["dossier_" + field_id]
                        else:
                            continue
                        
                        dossier_record[field_id] = format_value_for_grist(value, field_type)

                    # Ajouter explicitement l'ID du dossier
                    if "dossier_id" in dossier_info:
                        dossier_record["dossier_id"] = dossier_info["dossier_id"]
                    
                    # Vérifier que "number" est présent
                    if "number" not in dossier_record:
                        dossier_record["number"] = dossier_number
                    
                    # Traitement des labels
                    if "labels" not in dossier_data or not dossier_data.get("labels"):
                        # Si les labels ne sont pas présents, essayer de les récupérer séparément
                        labels = get_dossier_labels(dossier_number)
                        
                        if labels:
                            # Créer label_names
                            label_names = [label.get("name", "") for label in labels if label.get("name")]
                            dossier_record["label_names"] = ", ".join(label_names) if label_names else ""
                            
                            # Créer labels_json
                            labels_with_colors = [
                                {
                                    "id": label.get("id", ""),
                                    "name": label.get("name", ""),
                                    "color": label.get("color", "")
                                }
                                for label in labels if label.get("name") and label.get("color")
                            ]
                            
                            if labels_with_colors:
                                import json
                                dossier_record["labels_json"] = json_module.dumps(labels_with_colors, ensure_ascii=False)
                            else:
                                dossier_record["labels_json"] = ""
                    
                    # Ajouter à la liste des dossiers à traiter
                    dossier_records.append(dossier_record)
                    
                    # Préparer les données pour la table des champs
                    champ_record = {"dossier_number": dossier_number}
                    champ_column_types = {col["id"]: col["type"] for col in column_types["champs"]}
                    
                    for champ in flat_data["champs"]:
                        # Ignorer les champs problématiques
                        if champ["type"] in ["HeaderSectionChamp", "ExplicationChamp"]:
                            continue
                        
                        champ_label = normalize_column_name(champ["label"])
                        value = champ.get("value", "")
                        
                        # Pour les types complexes, utiliser la représentation JSON
                        if champ["type"] in ["CarteChamp", "AddressChamp", "SiretChamp"] and champ.get("json_value"):
                            try:
                                value = json_module.dumps(champ["json_value"], ensure_ascii=False)
                            except (TypeError, ValueError):
                                value = str(champ["json_value"])
                        
                        column_type = champ_column_types.get(champ_label, "Text")
                        champ_record[champ_label] = format_value_for_grist(value, column_type)
                    
                    champ_records.append(champ_record)
                    
                    # Préparer les données pour la table des annotations
                    annotation_record = {"dossier_number": dossier_number}
                    annotation_column_types = {col["id"]: col["type"] for col in column_types["annotations"]}
                    
                    for annotation in flat_data["annotations"]:
                        # Ignorer les annotations problématiques
                        if annotation["type"] in ["HeaderSectionChamp", "ExplicationChamp"]:
                            continue
                        
                        # Pour la table des annotations, enlever le préfixe "annotation_"
                        original_label = annotation["label"]
                        if original_label.startswith("annotation_"):
                            annotation_label = normalize_column_name(original_label[11:])
                        else:
                            annotation_label = normalize_column_name(original_label)
                        
                        value = annotation.get("value", "")
                        
                        # Pour les types complexes, utiliser la représentation JSON
                        if annotation["type"] in ["CarteChamp", "AddressChamp", "SiretChamp"] and annotation.get("json_value"):
                            try:
                                value = json_module.dumps(annotation["json_value"], ensure_ascii=False)
                            except (TypeError, ValueError):
                                value = str(annotation["json_value"])
                        
                        column_type = annotation_column_types.get(annotation_label, "Text")
                        annotation_record[annotation_label] = format_value_for_grist(value, column_type)
                    
                    annotation_records.append(annotation_record)
                    
                    batch_success += 1
                except Exception as e:
                    log_error(f"Exception lors du traitement du dossier {dossier_number}: {str(e)}")
                    import traceback
                    traceback.print_exc()
                    batch_errors += 1
            
            # Effectuer les opérations d'upsert par lot
            
            # Variables pour suivre les succès de ce lot
            batch_successful_dossiers = set()
            batch_failed_dossiers = set()
            
            # 1. Table des dossiers
            if dossier_records:
                log(f"  Upsert par lot de {len(dossier_records)} dossiers...")
                success = client.upsert_multiple_dossiers_in_grist(table_ids["dossier_table_id"], dossier_records)
                
                # Mettre à jour les ensembles de dossiers selon le résultat
                for record in dossier_records:
                    dossier_num = record.get("number") or record.get("dossier_number")
                    if dossier_num:
                        if success:
                            batch_successful_dossiers.add(str(dossier_num))
                        else:
                            batch_failed_dossiers.add(str(dossier_num))
            
            # 2. Table des champs
            if champ_records:
                log(f"  Upsert par lot de {len(champ_records)} enregistrements de champs...")
                success_champs = client.upsert_multiple_dossiers_in_grist(table_ids["champ_table_id"], champ_records)
                
                # Pour les champs, on ne compte les échecs que s'il y a vraiment eu un problème
                # car les champs sont traités individuellement et peuvent réussir partiellement
                if not success_champs:
                    log_verbose("Problème partiel lors du traitement des champs, mais les dossiers principaux restent valides")
            
            # 3. Table des annotations
            if annotation_records:
                log(f"  Upsert par lot de {len(annotation_records)} enregistrements d'annotations...")
                success_annotations = client.upsert_multiple_dossiers_in_grist(table_ids["annotation_table_id"], annotation_records)
                
                # Pour les annotations, même logique que pour les champs
                if not success_annotations:
                    log_verbose("Problème partiel lors du traitement des annotations, mais les dossiers principaux restent valides")
            
            # 4. Traiter les blocs répétables si nécessaire
            if has_repetable_blocks and table_ids.get("repetable_table_id") and dossier_batch_data:
                repetable_table_id = table_ids["repetable_table_id"]
                repetable_column_types = column_types.get("repetable_rows", [])
                
                log(f"  Traitement des blocs répétables pour {len(dossier_batch_data)} dossiers...")
                
                try:
                    import repetable_processor as rp
                    success_count_rep, error_count_rep = rp.process_repetables_batch(
                        client,
                        list(dossier_batch_data.values()),
                        repetable_table_id,
                        repetable_column_types,
                        problematic_ids=problematic_descriptor_ids,
                        batch_size=batch_size
                    )
                    log(f"  Blocs répétables traités par lot: {success_count_rep} réussis, {error_count_rep} en échec")
                except Exception as e:
                    log_error(f"  Erreur lors du traitement des blocs répétables: {str(e)}")
                    import traceback
                    traceback.print_exc()
            
            # Mettre à jour les ensembles globaux avec les résultats de ce lot
            successful_dossiers.update(batch_successful_dossiers)
            failed_dossiers.update(batch_failed_dossiers)
            
            # Log de progression avec les vrais chiffres
            current_success = len(successful_dossiers)
            current_failed = len(failed_dossiers)
            
            log(f"  Lot {batch_idx+1} terminé: {len(batch_successful_dossiers)} dossiers traités avec succès, {len(batch_failed_dossiers)} en échec")
            log(f"  Progression: {current_success}/{total_dossiers} dossiers traités ({current_success/total_dossiers*100:.1f}%)")
        
        # Afficher le résumé final
        log("\nTraitement terminé!")
        log(f"Dossiers traités avec succès: {success_count}/{total_dossiers}")
        if error_count > 0:
            log(f"Dossiers en échec: {error_count}")
        
        return True
        
    except Exception as e:
        log_error(f"Erreur lors du traitement de la démarche pour Grist: {e}")
        import traceback
        traceback.print_exc()
        return False

# Fonction optimisée pour le traitement d'une démarche pour Grist (Possibilité d'augmenter ou de diminuer batch_size et max_workers)
# Cette fonction est conçue pour être plus rapide et plus efficace, en utilisant le traitement par lots et le traitement parallèle.
def process_demarche_for_grist_optimized(client, demarche_number, parallel=True, batch_size=100, max_workers=3, api_filters=None):
    """
    Version optimisée du traitement d'une démarche pour Grist avec filtrage côté serveur.
    
    Args:
        client: Instance de GristClient
        demarche_number: Numéro de la démarche
        parallel: Utiliser le traitement parallèle si True
        batch_size: Taille des lots pour le traitement par lot
        max_workers: Nombre maximum de workers pour le traitement parallèle
        api_filters: Filtres optimisés à appliquer côté serveur (NOUVEAU)
        
    Returns:
        bool: Succès ou échec global
    """
    try:
        start_time = time.time()
        
        # Initialiser des ensembles pour suivre les dossiers traités
        successful_dossiers = set()
        failed_dossiers = set()
        
        # Initialiser le cache de colonnes
        column_cache = ColumnCache(client)
        
        # Vérifier que le document Grist existe
        try:
            doc_info = client.get_document_info()
            doc_name = doc_info.get("name", client.doc_id) if isinstance(doc_info, dict) else "Document ID " + client.doc_id
            log(f"Document Grist trouvé: {doc_name}")
        except Exception as e:
            log_error(f"Erreur lors de la vérification du document Grist: {e}")
            return False
        
        # Méthode avancée: Récupérer le schéma complet de la démarche
        problematic_descriptor_ids = set()
        column_types = None
        schema_method_successful = False
        
        # Essayer d'abord la méthode basée sur le schéma
        log(f"Récupération du schéma complet de la démarche {demarche_number}...")
        try:
            if 'get_demarche_schema' in globals() and 'create_columns_from_schema' in globals():
                demarche_schema = get_optimized_schema(demarche_number)
                log_schema_improvements(demarche_schema, demarche_number)
                log(f"Schéma récupéré avec succès pour la démarche: {demarche_schema['title']}")
                
                # Générer les définitions de colonnes à partir du schéma complet
                column_types, problematic_descriptor_ids = create_columns_from_schema(demarche_schema)
                
                # Récupérer les indicateurs de présence
                has_repetable_blocks = column_types.get("has_repetable_blocks", False)
                has_carto_fields = column_types.get("has_carto_fields", False)
                
                log(f"Identificateurs de {len(problematic_descriptor_ids)} descripteurs problématiques à filtrer")
                log(f"Types de colonnes détectés à partir du schéma:")
                log(f"  - Colonnes dossiers: {len(column_types['dossier'])}")
                log(f"  - Colonnes champs: {len(column_types['champs'])}")
                log(f"  - Colonnes annotations: {len(column_types['annotations'])}")
                log(f"  - Blocs répétables détectés: {'Oui' if has_repetable_blocks else 'Non'}")
                log(f"  - Champs cartographiques détectés: {'Oui' if has_carto_fields else 'Non'}")
                
                if has_repetable_blocks and "repetable_rows" in column_types:
                    log_verbose(f"  - Colonnes blocs répétables: {len(column_types['repetable_rows'])}")
                
                # Marquer la méthode comme réussie
                schema_method_successful = True
            else:
                log("Méthode basée sur le schéma non disponible, utilisation de la méthode alternative...")
        except Exception as e:
            log_error(f"Erreur lors de la récupération du schéma: {str(e)}")
            import traceback
            traceback.print_exc()
            log("Utilisation de la méthode alternative avec échantillons de dossiers...")
        
        # Essayer d'utiliser la méthode de mise à jour qui préserve les données existantes
        try:
            if 'update_grist_tables_from_schema' in globals():
                log("Mise à jour des tables Grist en préservant les données existantes...")
                table_result = update_grist_tables_from_schema(client, demarche_number, column_types if schema_method_successful else None, problematic_descriptor_ids)
                
                # Convertir le format de retour pour compatibilité
                table_ids = {
                    "dossier_table_id": table_result.get("dossiers"),
                    "champ_table_id": table_result.get("champs"), 
                    "annotation_table_id": table_result.get("annotations"),
                    "repetable_table_id": table_result.get("repetable_rows")
                }
            else:
                # Méthode classique qui peut effacer des données
                log("Utilisation de la méthode classique de création/modification de tables")
                table_ids = client.create_or_clear_grist_tables(demarche_number, column_types if schema_method_successful else None)
        except Exception as e:
            log_error(f"Erreur lors de la mise à jour des tables Grist: {str(e)}")
            # Fallback sur la méthode classique si pas de column_types
            if not schema_method_successful:
                # Récupérer les IDs des champs problématiques à filtrer
                problematic_descriptor_ids = get_problematic_descriptor_ids(demarche_number)
                log(f"Filtrage de {len(problematic_descriptor_ids)} descripteurs problématiques")
                
                # Récupérer quelques dossiers pour analyse du schéma
                sample_dossiers = []
                sample_dossier_numbers = []
                
                # Utiliser l'ancienne méthode pour récupérer des échantillons
                try:
                    from queries_graphql import get_demarche_dossiers
                    all_dossiers_brief = get_demarche_dossiers(demarche_number)
                    sample_size = min(3, len(all_dossiers_brief))
                    sample_dossier_numbers = [all_dossiers_brief[i]["number"] for i in range(sample_size)]
                    
                    for num in sample_dossier_numbers:
                        dossier = get_dossier(num)
                        if dossier:
                            sample_dossiers.append(dossier)
                except Exception as e:
                    log_error(f"Erreur lors de la récupération des échantillons: {e}")
                    return False
                
                if not sample_dossiers:
                    log_error("Aucun dossier n'a pu être récupéré pour l'analyse du schéma")
                    return False
                
                # Détecter les types de colonnes
                log("Détection des types de colonnes...")
                column_types = detect_column_types_from_multiple_dossiers(sample_dossiers, problematic_ids=problematic_descriptor_ids)
            
            # Fallback sur la méthode classique
            log("Fallback sur la méthode classique de création/modification de tables")
            table_ids = client.create_or_clear_grist_tables(demarche_number, column_types)
        
        # Log des table IDs
        log(f"Tables utilisées pour l'importation:")
        log(f"  Table dossiers: {table_ids['dossier_table_id']}")
        log(f"  Table champs: {table_ids['champ_table_id']}")
        log(f"  Table annotations: {table_ids['annotation_table_id']}")
        if table_ids.get('repetable_table_id'):
            log(f"  Table blocs répétables: {table_ids['repetable_table_id']}")
        
        # ========================================
        # NOUVELLE SECTION : RÉCUPÉRATION OPTIMISÉE DES DOSSIERS
        # ========================================
        
        if api_filters and api_filters:
            # Utiliser la récupération optimisée avec filtres côté serveur
            log(f"[FILTRAGE] Récupération optimisée des dossiers avec filtres côté serveur...")
        # Vérifier les filtres passés
        if api_filters.get('groupes_instructeurs'):
            log(f"Filtre par groupes instructeurs (numéros): {', '.join(map(str, api_filters['groupes_instructeurs']))}")
        if api_filters.get('statuts'):
            log(f"Filtre par statuts: {', '.join(api_filters['statuts'])}")
        if api_filters.get('date_debut'):
            log(f"Filtre par date de début: {api_filters['date_debut']}")
        if api_filters.get('date_fin'):
            log(f"Filtre par date de fin: {api_filters['date_fin']}")
            
            all_dossiers = get_demarche_dossiers_filtered(
                demarche_number,
                date_debut=api_filters.get('date_debut'),
                date_fin=api_filters.get('date_fin'),
                groupes_instructeurs=api_filters.get('groupes_instructeurs'),
                statuts=api_filters.get('statuts')
            )
            
            total_dossiers = len(all_dossiers)
            log(f"[OK] Dossiers récupérés avec filtres optimisés: {total_dossiers}")
            
            # Pas besoin de filtrage côté client car déjà fait côté serveur
            filtered_dossiers = all_dossiers
            
        else:
            # Utiliser l'ancienne méthode avec filtrage côté client
            log(f"[ATTENTION] Récupération classique de tous les dossiers (pas de filtres optimisés)")
            
            # Récupérer les filtres depuis les variables d'environnement pour compatibilité
            date_debut_str = os.getenv("DATE_DEPOT_DEBUT", "")
            date_fin_str = os.getenv("DATE_DEPOT_FIN", "")
            statuts_filter = os.getenv("STATUTS_DOSSIERS", "").split(",") if os.getenv("STATUTS_DOSSIERS") else []
            groupes_filter = os.getenv("GROUPES_INSTRUCTEURS", "").split(",") if os.getenv("GROUPES_INSTRUCTEURS") else []
            
            # Nettoyer les filtres
            if date_debut_str.strip() == "":
                date_debut_str = None
            if date_fin_str.strip() == "":
                date_fin_str = None
            statuts_filter = [s for s in statuts_filter if s.strip()]
            groupes_filter = [g for g in groupes_filter if g.strip()]
            
            # Convertir les dates
            date_debut = None
            date_fin = None
            if date_debut_str:
                try:
                    date_debut = datetime.strptime(date_debut_str, "%Y-%m-%d")
                    log(f"Filtre par date de début: {date_debut.strftime('%Y-%m-%d')}")
                except ValueError:
                    log_error(f"Format de date de début invalide: {date_debut_str}")
            
            if date_fin_str:
                try:
                    date_fin = datetime.strptime(date_fin_str, "%Y-%m-%d")  
                    log(f"Filtre par date de fin: {date_fin.strftime('%Y-%m-%d')}")
                except ValueError:
                    log_error(f"Format de date de fin invalide: {date_fin_str}")
            
            if statuts_filter:
                log(f"Filtre par statuts: {', '.join(statuts_filter)}")
            if groupes_filter:
                log(f"Filtre par groupes instructeurs: {', '.join(groupes_filter)}")
            
            # Récupérer tous les dossiers puis filtrer côté client
            from queries_graphql import get_demarche_dossiers
            log(f"Récupération de tous les dossiers avec pagination...")
            all_dossiers = get_demarche_dossiers(demarche_number)
            
            total_dossiers_brut = len(all_dossiers)
            log(f"Nombre total de dossiers trouvés: {total_dossiers_brut}")
            
            # Appliquer les filtres côté client
            filtered_dossiers = []
            for dossier in all_dossiers:
                # Filtre par statut
                if statuts_filter and dossier["state"] not in statuts_filter:
                    continue
                    
                # Filtre par groupe instructeur
                if groupes_filter and (
                    not dossier.get("groupeInstructeur") or 
                    str(dossier["groupeInstructeur"].get("number", "")) not in groupes_filter
                ):
                    continue
                    
                # Filtre par date de dépôt
                if date_debut or date_fin:
                    date_depot_str = dossier.get("dateDepot")
                    if not date_depot_str:
                        continue
                    
                    try:
                        date_depot = datetime.strptime(date_depot_str.split("T")[0], "%Y-%m-%d")
                        
                        if date_debut and date_depot < date_debut:
                            continue
                        if date_fin and date_depot > date_fin:
                            continue
                    except (ValueError, AttributeError, TypeError):
                        continue
                
                filtered_dossiers.append(dossier)
            
            total_dossiers = len(filtered_dossiers)
            log(f"Après filtrage: {total_dossiers} dossiers ({(total_dossiers/total_dossiers_brut*100) if total_dossiers_brut > 0 else 0:.1f}%)")
        
        # Si aucun dossier ne correspond aux critères, c'est quand même un succès (les tables sont créées)
        if total_dossiers == 0:
            log("Aucun dossier ne correspond aux critères de filtrage")
            elapsed_time = time.time() - start_time
            minutes = int(elapsed_time // 60)
            seconds = elapsed_time % 60
            log("\nTraitement terminé!")
            log(f"Durée totale: {minutes} min {seconds:.1f} sec")
            log("Tables créées avec succès, mais aucun dossier à traiter.")
            return True

        # Organiser les dossiers en lots
        dossier_batches = []
        batch_count = (total_dossiers + batch_size - 1) // batch_size
        
        for i in range(0, total_dossiers, batch_size):
            batch_dossier_numbers = [filtered_dossiers[j]["number"] for j in range(i, min(i+batch_size, total_dossiers))]
            dossier_batches.append(batch_dossier_numbers)
        
        log(f"Dossiers organisés en {batch_count} lots de {batch_size} maximum")
        
        # Le reste du traitement reste identique...
        # [COPIER LE RESTE DE LA FONCTION EXISTANTE DEPUIS "# Traiter les lots de dossiers"]
        
        # Traiter les lots de dossiers
        total_success = 0
        total_errors = 0
        
        for batch_idx, batch in enumerate(dossier_batches):
            log(f"Traitement du lot {batch_idx+1}/{batch_count} ({len(batch)} dossiers)...")
            
            # Récupérer les dossiers complets
            if parallel:
                batch_dossiers_dict = fetch_dossiers_in_parallel(batch, max_workers=max_workers)
            else:
                batch_dossiers_dict = {}
                for num in batch:
                    dossier = get_dossier(num)
                    if dossier:
                        batch_dossiers_dict[num] = dossier
                    else:
                        log_error(f"Dossier {num} inaccessible en raison de restrictions de permission, ignoré")
            
            if not batch_dossiers_dict:
                log_error(f"Aucun dossier n'a pu être récupéré pour le lot {batch_idx+1}")
                continue
            
            # Préparer les dossiers pour upsert par lot
            dossier_records = []
            champ_records = []
            annotation_records = []
            
            for dossier_num, dossier_data in batch_dossiers_dict.items():
                try:
                    # Extraire les données à plat
                    exclude_repetition = column_types.get("has_repetable_blocks", False)
                    flat_data = dossier_to_flat_data(dossier_data, exclude_repetition_champs=exclude_repetition, problematic_ids=problematic_descriptor_ids)
                    
                    # Préparer l'enregistrement pour la table des dossiers
                    dossier_info = flat_data["dossier"]
                    dossier_record = {}
                    for column in column_types["dossier"]:
                        field_id = column["id"]
                        field_type = column["type"]
                        
                        if field_id in dossier_info:
                            value = dossier_info[field_id]
                        elif "dossier_" + field_id in dossier_info:
                            value = dossier_info["dossier_" + field_id]
                        else:
                            continue
                        
                        dossier_record[field_id] = format_value_for_grist(value, field_type)
                    
                    # Vérifier que number est présent
                    if "number" not in dossier_record:
                        dossier_record["number"] = dossier_num
                        
                    # Traitement des labels
                    if "labels" not in dossier_data or not dossier_data.get("labels"):
                        labels = get_dossier_labels(dossier_num)
                        
                        if labels:
                            # Créer label_names
                            label_names = [label.get("name", "") for label in labels if label.get("name")]
                            dossier_record["label_names"] = ", ".join(label_names) if label_names else ""
                            
                            # Créer labels_json
                            labels_with_colors = [
                                {
                                    "id": label.get("id", ""),
                                    "name": label.get("name", ""),
                                    "color": label.get("color", "")
                                }
                                for label in labels if label.get("name") and label.get("color")
                            ]
                            
                            if labels_with_colors:
                                dossier_record["labels_json"] = json_module.dumps(labels_with_colors, ensure_ascii=False)
                            else:
                                dossier_record["labels_json"] = ""
                    
                    dossier_records.append(dossier_record)
                    
                    # Préparer l'enregistrement pour la table des champs
                    champ_record = {"dossier_number": dossier_num}
                    champ_column_types = {col["id"]: col["type"] for col in column_types["champs"]}
                    
                    # Collecter les IDs des champs pour les stocker
                    champ_ids = []
                    for champ in flat_data["champs"]:
                        if champ.get("id"):
                            champ_ids.append(str(champ["id"]))
                    if champ_ids:
                        champ_record["champ_id"] = "_".join(champ_ids)
                    
                    for champ in flat_data["champs"]:
                        if champ["type"] in ["HeaderSectionChamp", "ExplicationChamp"]:
                            continue
                            
                        normalized_label = normalize_column_name(champ["label"])
                        
                        value = champ.get("value", "")
                        if champ["type"] in ["CarteChamp", "AddressChamp", "SiretChamp"] and champ.get("json_value"):
                            try:
                                value = json_module.dumps(champ["json_value"], ensure_ascii=False)
                            except:
                                value = str(champ["json_value"])
                        
                        column_type = champ_column_types.get(normalized_label, "Text")
                        champ_record[normalized_label] = format_value_for_grist(value, column_type)
                    
                    champ_records.append(champ_record)
                    
                    # Préparer l'enregistrement pour la table des annotations
                    annotation_record = {"dossier_number": dossier_num}
                    annotation_column_types = {col["id"]: col["type"] for col in column_types["annotations"]}
                    
                    # Créer dynamiquement les colonnes pour les IDs des annotations
                    add_id_columns_based_on_annotations(client, table_ids["annotation_table_id"], flat_data["annotations"])
                    
                    # Collecter les IDs des annotations pour les stocker
                    annotation_ids = []
                    for annotation in flat_data["annotations"]:
                        if annotation.get("id"):
                            annotation_ids.append(str(annotation["id"]))
                    if annotation_ids:
                        annotation_record["annotation_id"] = "_".join(annotation_ids)
                    
                    for annotation in flat_data["annotations"]:
                        if annotation["type"] in ["HeaderSectionChamp", "ExplicationChamp"]:
                            continue
                            
                        original_label = annotation["label"]
                        if original_label.startswith("annotation_"):
                            normalized_label = normalize_column_name(original_label[11:])
                        else:
                            normalized_label = normalize_column_name(original_label)
                        
                        value = annotation.get("value", "")
                        if annotation["type"] in ["CarteChamp", "AddressChamp", "SiretChamp"] and annotation.get("json_value"):
                            try:
                                value = json_module.dumps(annotation["json_value"], ensure_ascii=False)
                            except:
                                value = str(annotation["json_value"])
                        
                        column_type = annotation_column_types.get(normalized_label, "Text")
                        annotation_record[normalized_label] = format_value_for_grist(value, column_type)
                        
                        # Ajouter l'ID de l'annotation
                        if "id" in annotation:
                            id_column = f"{normalized_label}_id"
                            annotation_record[id_column] = annotation["id"]
                    
                    annotation_records.append(annotation_record)
                    
                except Exception as e:
                    log_error(f"Erreur lors de la préparation du dossier {dossier_num}: {str(e)}")
                    import traceback
                    traceback.print_exc()
            
            # Effectuer les opérations d'upsert par lot
            if dossier_records:
                log(f"  Upsert par lot de {len(dossier_records)} dossiers...")
                success = client.upsert_multiple_dossiers_in_grist(table_ids["dossier_table_id"], dossier_records)

                # Mettre à jour les ensembles de dossiers
                for record in dossier_records:
                    dossier_num = record.get("number") or record.get("dossier_number")
                    if dossier_num:
                        if success:
                            successful_dossiers.add(str(dossier_num))
                        else:
                            failed_dossiers.add(str(dossier_num))
            
            if champ_records:
                log(f"  Upsert par lot de {len(champ_records)} enregistrements de champs...")
                success = client.upsert_multiple_dossiers_in_grist(table_ids["champ_table_id"], champ_records)
                if success:
                    total_success += len(champ_records)
                else:
                    total_errors += len(champ_records)
            
            if annotation_records:
                log(f"  Upsert par lot de {len(annotation_records)} enregistrements d'annotations...")
                success = client.upsert_multiple_dossiers_in_grist(table_ids["annotation_table_id"], annotation_records)
            
            # Traiter les blocs répétables si nécessaire
            if column_types.get("has_repetable_blocks", False) and table_ids.get("repetable_table_id") and "repetable_rows" in column_types:
                repetable_table_id = table_ids["repetable_table_id"]
                repetable_column_types = column_types.get("repetable_rows", [])
                
                try:
                    import repetable_processor as rp
                    success_count, error_count = rp.process_repetables_batch(
                        client,
                        list(batch_dossiers_dict.values()),
                        repetable_table_id,
                        repetable_column_types,
                        problematic_ids=problematic_descriptor_ids,
                        batch_size=batch_size
                    )
                    log(f"  Blocs répétables traités par lot: {success_count} réussis, {error_count} en échec")
                except Exception as e:
                    log_error(f"  Erreur lors du traitement des blocs répétables: {str(e)}")
                    import traceback
                    traceback.print_exc()
        
        # Calculer les statistiques finales
        elapsed_time = time.time() - start_time
        minutes = int(elapsed_time // 60)
        seconds = elapsed_time % 60

        # Calculer les nombres à partir des ensembles
        total_success = len(successful_dossiers)
        total_errors = len(failed_dossiers)

        log("\nTraitement terminé!")
        log(f"Durée totale: {minutes} min {seconds:.1f} sec")
        log(f"Dossiers traités avec succès: {total_success}")
        if total_errors > 0:
            log(f"Dossiers en échec: {total_errors}")
                
        return total_success > 0 or schema_method_successful
        
    except Exception as e:
        log_error(f"Erreur lors du traitement de la démarche pour Grist: {e}")
        import traceback
        traceback.print_exc()
        return False
    
def main():
    load_dotenv()

    grist_base_url = os.getenv("GRIST_BASE_URL")
    grist_api_key = os.getenv("GRIST_API_KEY")
    grist_doc_id = os.getenv("GRIST_DOC_ID")

    if not all([grist_base_url, grist_api_key, grist_doc_id]):
        log_error("Configuration Grist incomplète dans le fichier .env")
        log("Assurez-vous d'avoir défini GRIST_BASE_URL, GRIST_API_KEY et GRIST_DOC_ID")
        return 1

    # Masquer partiellement la clé API par sécurité
    api_key_masked = grist_api_key[:4] + "..." + grist_api_key[-4:] if len(grist_api_key) > 8 else "***"
    log(f"Configuration Grist:")
    log(f"  URL de base: {grist_base_url}")
    log(f"  Clé API: {api_key_masked}")
    log(f"  ID du document: {grist_doc_id}")

    # Récupérer le numéro de démarche
    demarche_number = os.getenv("DEMARCHE_NUMBER")
    if not demarche_number:
        log_error("DEMARCHE_NUMBER non défini dans le fichier .env")
        return 1

    try:
        # Convertir le numéro de démarche en entier
        demarche_number = int(demarche_number)
        log(f"Traitement de la démarche: {demarche_number}")
    except ValueError:
        log_error("DEMARCHE_NUMBER doit être un nombre entier")
        return 1

    # Initialiser le client Grist
    client = GristClient(grist_base_url, grist_api_key, grist_doc_id)

    # NOUVEAU : Récupérer les filtres optimisés depuis l'environnement
    api_filters_json = os.getenv('API_FILTERS_JSON', '{}')
    try:
        api_filters = json_module.loads(api_filters_json)
        if api_filters:
            log(f"[FILTRAGE] Filtres optimisés détectés: {list(api_filters.keys())}")
    except:
        api_filters = {}
        log("⚠️ Aucun filtre optimisé détecté, utilisation de l'ancienne méthode")

    # Récupérer les autres paramètres
    parallel = os.getenv('PARALLEL', 'true').lower() == 'true'
    batch_size = int(os.getenv('BATCH_SIZE', '50'))
    max_workers = int(os.getenv('MAX_WORKERS', '3'))

    # Traiter la démarche avec la fonction optimisée
    if process_demarche_for_grist_optimized(
        client, 
        demarche_number, 
        parallel=parallel, 
        batch_size=batch_size, 
        max_workers=max_workers,
        api_filters=api_filters  # Passer les filtres optimisés
    ):
        log(f"Traitement de la démarche {demarche_number} terminé avec succès")
        return 0
    else:
        log_error(f"Échec du traitement de la démarche {demarche_number}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
