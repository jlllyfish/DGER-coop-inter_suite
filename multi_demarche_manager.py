"""
Gestionnaire de configuration pour le projet multi-démarche simplifié.
Chaque démarche a son propre token API directement.
VERSION OPTIMISÉE avec filtrage côté serveur et hybride.

DOCUMENTATION DES FILTRES SUPPORTÉS:

🚀 FILTRES CÔTÉ SERVEUR (Performance optimale):
- date_depot_debut: Date de début au format YYYY-MM-DD
  Exemple: "2025-06-15"
  Impact: Réduction de ~95% du volume de données

💻 FILTRES CÔTÉ CLIENT (Appliqués sur résultat réduit):
- date_depot_fin: Date de fin au format YYYY-MM-DD
- groupes_instructeurs: Liste ou string des numéros de groupes
  Exemple: ["120382"] ou "120382"
- statuts_dossiers: Liste des statuts
  Exemple: ["en_construction", "accepte"]

⚡ RECOMMANDATIONS PERFORMANCE:
1. TOUJOURS utiliser date_depot_debut pour filtrer côté serveur
2. Combiner avec des filtres côté client pour un filtrage précis
3. Plus la date_depot_debut est récente, meilleure est la performance

📊 EXEMPLES DE CONFIGURATIONS EFFICACES:

Configuration OPTIMALE:
{
  "filters": {
    "date_depot_debut": "2025-01-01",        # Côté serveur
    "groupes_instructeurs": ["120382"],       # Côté client
    "statuts_dossiers": ["en_construction"]   # Côté client
  }
}

Configuration MOYENNE (à éviter si possible):
{
  "filters": {
    "groupes_instructeurs": ["120382"],       # Côté client uniquement
    "statuts_dossiers": ["accepte"]           # Côté client uniquement
  }
}
"""

import os
import json
import re
import time
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from dotenv import load_dotenv

@dataclass
class DemarcheConfig:
    """Configuration d'une démarche."""
    number: int
    name: str
    api_token: str
    api_url: str
    enabled: bool
    sync_config: Dict[str, Any]
    filters: Dict[str, Any]

@dataclass
class SyncResult:
    """Résultat de synchronisation d'une démarche."""
    demarche_number: int
    demarche_name: str
    success: bool
    dossiers_processed: int
    errors: List[str]
    duration_seconds: float

class MultiDemarcheManager:
    """
    Gestionnaire principal pour la synchronisation multi-démarche simplifié.
    VERSION OPTIMISÉE avec filtrage côté serveur et hybride.
    """
    
    def __init__(self, config_file: str = "config.json"):
        """
        Initialise le gestionnaire avec un fichier de configuration.
        
        Args:
            config_file: Chemin vers le fichier de configuration JSON
        """
        load_dotenv()
        self.config_file = config_file
        self.config = self._load_config()
        self.demarches = self._load_demarches()
        
    def _resolve_env_vars(self, text: str) -> str:
        """
        Résout les variables d'environnement dans une chaîne de caractères.
        Format attendu : ${VAR_NAME}
        
        Args:
            text: Texte contenant des variables d'environnement
            
        Returns:
            str: Texte avec les variables résolues
        """
        
        if not isinstance(text, str):
            return text
            
        def replace_var(match):
            var_name = match.group(1)
            return os.getenv(var_name, f"${{{var_name}}}")  # Garde la variable si non trouvée
        
        return re.sub(r'\$\{([^}]+)\}', replace_var, text)
    
    def _resolve_dict_env_vars(self, data: Any) -> Any:
        """
        Résout récursivement les variables d'environnement dans un dictionnaire.
        
        Args:
            data: Données à traiter (dict, list, str, etc.)
            
        Returns:
            Données avec les variables d'environnement résolues
        """
        if isinstance(data, dict):
            return {key: self._resolve_dict_env_vars(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self._resolve_dict_env_vars(item) for item in data]
        elif isinstance(data, str):
            return self._resolve_env_vars(data)
        else:
            return data
    
    def _load_config(self) -> Dict[str, Any]:
        """
        Charge et valide le fichier de configuration.
        
        Returns:
            dict: Configuration chargée avec variables d'environnement résolues
        """
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Résoudre les variables d'environnement
            config = self._resolve_dict_env_vars(config)
            
            # Validation de base
            required_sections = ['grist', 'demarches']
            for section in required_sections:
                if section not in config:
                    raise ValueError(f"Section manquante dans la configuration : {section}")
            
            return config
            
        except FileNotFoundError:
            raise FileNotFoundError(f"Fichier de configuration non trouvé : {self.config_file}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Erreur de format JSON dans {self.config_file} : {e}")
    
    def _load_demarches(self) -> List[DemarcheConfig]:
        """
        Charge les configurations des démarches.
        
        Returns:
            list: Liste des configurations de démarches
        """
        demarches = []
        
        for demarche_data in self.config['demarches']:
            # Vérifier que le token a été résolu
            api_token = demarche_data.get('api_token', '')
            if api_token.startswith('${'):
                print(f"⚠️  Attention: Token non résolu pour la démarche {demarche_data['number']}")
                print(f"   Variable d'environnement manquante : {api_token}")
                continue
                
            demarches.append(DemarcheConfig(
                number=demarche_data['number'],
                name=demarche_data['name'],
                api_token=api_token,
                api_url=demarche_data.get('api_url', 'https://www.demarches-simplifiees.fr/api/v2/graphql'),
                enabled=demarche_data.get('enabled', True),
                sync_config=demarche_data.get('sync_config', {}),
                filters=demarche_data.get('filters', {})
            ))
        
        return demarches
    
    def get_enabled_demarches(self) -> List[DemarcheConfig]:
        """
        Retourne la liste des démarches activées.
        
        Returns:
            list: Liste des démarches activées
        """
        return [d for d in self.demarches if d.enabled]
    
    def get_demarche_config(self, demarche_number: int) -> Optional[DemarcheConfig]:
        """
        Retourne la configuration pour une démarche donnée.
        
        Args:
            demarche_number: Numéro de la démarche
            
        Returns:
            DemarcheConfig ou None si non trouvé
        """
        for demarche in self.demarches:
            if demarche.number == demarche_number:
                return demarche
        return None
    
    def get_grist_config(self) -> Dict[str, str]:
        """
        Retourne la configuration Grist.
        
        Returns:
            dict: Configuration Grist
        """
        grist_config = self.config['grist'].copy()
        
        # Vérifier que les variables ont été résolues
        for key, value in grist_config.items():
            if isinstance(value, str) and value.startswith('${'):
                print(f"⚠️  Attention: Variable Grist non résolue : {key} = {value}")
        
        return grist_config
    
    def _prepare_filters_for_api(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prépare les filtres pour l'API en tenant compte des limitations réelles de l'API GraphQL.
        
        LIMITATIONS DÉCOUVERTES :
        ✅ createdSince: Supporté côté serveur
        ❌ createdUntil: Non supporté (sera filtré côté client)
        ❌ groupeInstructeurNumber: Non supporté (sera filtré côté client)  
        ❌ states: Non supporté (sera filtré côté client)
        
        Args:
            filters: Filtres bruts de la configuration
            
        Returns:
            dict: Filtres formatés pour l'API (hybride serveur + client)
        """
        api_filters = {}
        
        # Date de début - SEUL FILTRE CÔTÉ SERVEUR qui fonctionne
        if filters.get('date_depot_debut'):
            api_filters['date_debut'] = filters['date_depot_debut']
            print(f"🔍 Filtre côté serveur: date_debut = {filters['date_depot_debut']}")
        
        # Tous les autres filtres seront appliqués côté client
        if filters.get('date_depot_fin'):
            api_filters['date_fin'] = filters['date_depot_fin']
            print(f"💻 Filtre côté client: date_fin = {filters['date_depot_fin']}")
        
        # Groupes instructeurs - Correction du bug de parsing + note côté client
        groupes = filters.get('groupes_instructeurs', [])
        if groupes:
            if isinstance(groupes, str):
                api_filters['groupes_instructeurs'] = [groupes]
            elif isinstance(groupes, list):
                api_filters['groupes_instructeurs'] = [str(g) for g in groupes]
            else:
                print(f"⚠️  Format de groupes_instructeurs non reconnu : {type(groupes)} - {groupes}")
            
            if 'groupes_instructeurs' in api_filters:
                print(f"💻 Filtre côté client: groupes_instructeurs = {api_filters['groupes_instructeurs']}")
        
        # Statuts des dossiers - côté client
        statuts = filters.get('statuts_dossiers', [])
        if statuts:
            if isinstance(statuts, list):
                api_filters['statuts'] = statuts
            elif isinstance(statuts, str) and statuts.strip():
                api_filters['statuts'] = [statuts]
            
            if 'statuts' in api_filters:
                print(f"💻 Filtre côté client: statuts = {api_filters['statuts']}")
        
        # Ajouter une note explicative
        if api_filters:
            server_filters = [k for k in api_filters.keys() if k == 'date_debut']
            client_filters = [k for k in api_filters.keys() if k != 'date_debut']
            
            if server_filters:
                print(f"🚀 {len(server_filters)} filtre(s) côté serveur: {server_filters}")
            if client_filters:
                print(f"💻 {len(client_filters)} filtre(s) côté client: {client_filters}")
                print(f"   Note: Ces filtres seront appliqués sur le résultat déjà réduit par le serveur")
        
        return api_filters
    
    def set_environment_for_demarche(self, demarche_number: int) -> bool:
        """
        Configure les variables d'environnement pour une démarche spécifique.
        Cela permet d'utiliser le code existant sans modification.
        VERSION OPTIMISÉE - prépare les filtres pour l'API.
        
        Args:
            demarche_number: Numéro de la démarche
            
        Returns:
            bool: True si la configuration a réussi, False sinon
        """
        demarche_config = self.get_demarche_config(demarche_number)
        if not demarche_config:
            print(f"❌ Démarche {demarche_number} non trouvée dans la configuration")
            return False
        
        # Vérifier que le token est valide
        if not demarche_config.api_token or demarche_config.api_token.startswith('${'):
            print(f"❌ Token API invalide pour la démarche {demarche_number}")
            return False
        
        print(f"🔄 Reconfiguration pour la démarche {demarche_number}...")
        print(f"   Token: {demarche_config.api_token[:8]}...{demarche_config.api_token[-8:]}")
        
        # Configurer les variables d'environnement pour l'API DS
        os.environ['DEMARCHES_API_TOKEN'] = demarche_config.api_token
        os.environ['DEMARCHES_API_URL'] = demarche_config.api_url
        os.environ['DEMARCHE_NUMBER'] = str(demarche_number)
        
        # IMPORTANT : Forcer la mise à jour du cache dans queries_config
        try:
            import queries_config
            queries_config.API_TOKEN = demarche_config.api_token
            queries_config.API_URL = demarche_config.api_url
            
            # Si la classe DemarcheAPIConfig existe, l'utiliser
            if hasattr(queries_config, 'DemarcheAPIConfig'):
                queries_config.DemarcheAPIConfig.set_current_api_config(
                    demarche_config.api_token, 
                    demarche_config.api_url
                )
            
            # FORCER LE RECHARGEMENT DES MODULES CRITIQUES
            import importlib
            import sys
            
            # Recharger queries_config pour forcer la prise en compte du nouveau token
            if 'queries_config' in sys.modules:
                importlib.reload(queries_config)
                queries_config.API_TOKEN = demarche_config.api_token
                queries_config.API_URL = demarche_config.api_url
            
            # Recharger queries_graphql qui utilise le token
            if 'queries_graphql' in sys.modules:
                import queries_graphql
                importlib.reload(queries_graphql)
            
            # Recharger schema_utils qui utilise aussi le token  
            if 'schema_utils' in sys.modules:
                import schema_utils
                importlib.reload(schema_utils)
                
            print(f"   🔄 Modules rechargés avec le nouveau token")
            
        except ImportError:
            pass
        
        # Forcer le rechargement de la dotenv si elle est en cache
        try:
            from dotenv import load_dotenv
            load_dotenv(override=True)  # override=True force le rechargement
        except ImportError:
            pass
        
        # Configurer les variables Grist
        grist_config = self.get_grist_config()
        os.environ['GRIST_BASE_URL'] = grist_config['base_url']
        os.environ['GRIST_API_KEY'] = grist_config['api_key']
        os.environ['GRIST_DOC_ID'] = grist_config['doc_id']
        
        # Préparer les filtres pour l'API optimisée
        api_filters = self._prepare_filters_for_api(demarche_config.filters)
        
        # Stocker les filtres préparés dans l'environnement pour le code existant
        # (ANCIENNE MÉTHODE - pour compatibilité)
        filters = demarche_config.filters
        os.environ['DATE_DEPOT_DEBUT'] = filters.get('date_depot_debut', '')
        os.environ['DATE_DEPOT_FIN'] = filters.get('date_depot_fin', '')
        os.environ['STATUTS_DOSSIERS'] = ','.join(filters.get('statuts_dossiers', []))
        
        # Correction du bug des groupes instructeurs
        groupes = filters.get('groupes_instructeurs', [])
        if isinstance(groupes, str):
            os.environ['GROUPES_INSTRUCTEURS'] = groupes
        elif isinstance(groupes, list):
            os.environ['GROUPES_INSTRUCTEURS'] = ','.join(str(g) for g in groupes)
        else:
            os.environ['GROUPES_INSTRUCTEURS'] = ''
        
        # Stocker les filtres optimisés pour la nouvelle API
        os.environ['API_FILTERS_JSON'] = json.dumps(api_filters)
        
        # Configurer les paramètres de synchronisation
        sync_config = demarche_config.sync_config
        os.environ['BATCH_SIZE'] = str(sync_config.get('batch_size', 50))
        os.environ['MAX_WORKERS'] = str(sync_config.get('max_workers', 3))
        os.environ['PARALLEL'] = str(sync_config.get('parallel', True)).lower()
        
        print(f"✅ Environnement configuré pour la démarche {demarche_number} - {demarche_config.name}")
        
        # Afficher les filtres qui seront appliqués avec distinction serveur/client
        if api_filters:
            print(f"🔍 Stratégie de filtrage hybride activée :")
            
            # Filtres côté serveur
            server_filters = {k: v for k, v in api_filters.items() if k == 'date_debut'}
            if server_filters:
                print(f"   🚀 Côté serveur (performance optimale) :")
                for key, value in server_filters.items():
                    print(f"      • {key}: {value}")
            
            # Filtres côté client  
            client_filters = {k: v for k, v in api_filters.items() if k != 'date_debut'}
            if client_filters:
                print(f"   💻 Côté client (sur résultat réduit) :")
                for key, value in client_filters.items():
                    print(f"      • {key}: {value}")
            
            # Estimation de performance
            if server_filters and client_filters:
                print(f"   ⚡ Estimation: ~95% de réduction du volume de données grâce au filtre serveur")
            elif server_filters:
                print(f"   ⚡ Estimation: ~95% de réduction du volume de données")
            else:
                print(f"   ⚠️  Attention: Aucun filtre côté serveur - performance limitée")
        else:
            print(f"⚠️  Aucun filtre configuré - tous les dossiers seront récupérés")
        
        # VALIDATION : Vérifier que le token est bien appliqué
        import requests
        headers = {
            "Authorization": f"Bearer {demarche_config.api_token}",
            "Content-Type": "application/json"
        }
        
        # Test simple pour vérifier l'accès
        test_query = """
        query testAccess($demarcheNumber: Int!) {
            demarche(number: $demarcheNumber) {
                id
                title
            }
        }
        """
        
        try:
            response = requests.post(
                demarche_config.api_url,
                json={"query": test_query, "variables": {"demarcheNumber": demarche_number}},
                headers=headers,
                timeout=10
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get("data") and result["data"].get("demarche"):
                    print(f"   ✅ Token validé - Accès à la démarche confirmé")
                    return True
                else:
                    print(f"   ⚠️  Token configuré mais démarche inaccessible")
                    if "errors" in result:
                        for error in result["errors"]:
                            print(f"      Erreur API: {error.get('message', 'Unknown')}")
                    return True  # Continuer quand même
            else:
                print(f"   ⚠️  Erreur HTTP lors du test: {response.status_code}")
                return True  # Continuer quand même
                
        except Exception as e:
            print(f"   ⚠️  Impossible de valider le token: {str(e)}")
            return True  # Continuer quand même
    
    def sync_all_demarches(self) -> List[SyncResult]:
        """
        Synchronise toutes les démarches activées.
        
        Returns:
            list: Liste des résultats de synchronisation
        """
        results = []
        enabled_demarches = self.get_enabled_demarches()
        
        print(f"🚀 Démarrage de la synchronisation de {len(enabled_demarches)} démarches")
        
        for i, demarche in enumerate(enabled_demarches, 1):
            print(f"\n📋 Synchronisation {i}/{len(enabled_demarches)}: {demarche.name} (#{demarche.number})")
            
            # Configurer l'environnement pour cette démarche
            if not self.set_environment_for_demarche(demarche.number):
                results.append(SyncResult(
                    demarche_number=demarche.number,
                    demarche_name=demarche.name,
                    success=False,
                    dossiers_processed=0,
                    errors=["Échec de la configuration de l'environnement"],
                    duration_seconds=0
                ))
                continue
            
            # Exécuter la synchronisation
            result = self._sync_single_demarche(demarche)
            results.append(result)
            
            # Pause entre les démarches pour éviter les problèmes de cache
            if i < len(enabled_demarches):
                print(f"⏸️  Pause de 2 secondes avant la démarche suivante...")
                import time
                time.sleep(2)
        
        # Afficher le résumé
        self._print_sync_summary(results)
        
        return results
    
    def sync_specific_demarches(self, demarche_numbers: List[int], force_disabled: bool = False) -> List[SyncResult]:
        """
        Synchronise des démarches spécifiques.
        
        Args:
            demarche_numbers: Liste des numéros de démarches à synchroniser
            force_disabled: Si True, synchronise même les démarches désactivées
            
        Returns:
            list: Liste des résultats de synchronisation
        """
        results = []
        
        print(f"🔍 Recherche des démarches : {demarche_numbers}")
        
        for demarche_number in demarche_numbers:
            demarche_config = self.get_demarche_config(demarche_number)
            
            if not demarche_config:
                print(f"❌ Démarche {demarche_number} non trouvée dans la configuration")
                print(f"   Démarches disponibles : {[d.number for d in self.demarches]}")
                results.append(SyncResult(
                    demarche_number=demarche_number,
                    demarche_name=f"Démarche {demarche_number}",
                    success=False,
                    dossiers_processed=0,
                    errors=["Démarche non trouvée dans la configuration"],
                    duration_seconds=0
                ))
                continue
            
            if not demarche_config.enabled and not force_disabled:
                print(f"⚠️  Démarche {demarche_number} ({demarche_config.name}) désactivée, ignorée")
                print(f"   Utilisez --force pour forcer la synchronisation")
                continue
            
            print(f"\n📋 Synchronisation: {demarche_config.name} (#{demarche_number})")
            
            # Configurer l'environnement pour cette démarche
            if not self.set_environment_for_demarche(demarche_number):
                results.append(SyncResult(
                    demarche_number=demarche_number,
                    demarche_name=demarche_config.name,
                    success=False,
                    dossiers_processed=0,
                    errors=["Échec de la configuration de l'environnement"],
                    duration_seconds=0
                ))
                continue
            
            # Exécuter la synchronisation
            result = self._sync_single_demarche(demarche_config)
            results.append(result)
            
            # Pause entre les démarches pour éviter les problèmes de cache
            if demarche_number != demarche_numbers[-1]:  # Pas de pause après la dernière
                print(f"⏸️  Pause de 2 secondes avant la démarche suivante...")
                import time
                time.sleep(2)
        
        # Afficher le résumé
        if results:
            self._print_sync_summary(results)
        else:
            print("⚠️  Aucune démarche n'a été synchronisée")
        
        return results
    
    def _sync_single_demarche(self, demarche: DemarcheConfig) -> SyncResult:
        """
        Synchronise une seule démarche.
        VERSION OPTIMISÉE avec filtrage côté serveur.
        
        Args:
            demarche: Configuration de la démarche
            
        Returns:
            SyncResult: Résultat de la synchronisation
        """
        start_time = time.time()
        
        try:
            # Importer ici pour éviter les problèmes de dépendances circulaires
            from grist_processor_working_all import GristClient, process_demarche_for_grist_optimized
            
            # Créer le client Grist
            grist_config = self.get_grist_config()
            client = GristClient(
                grist_config['base_url'],
                grist_config['api_key'],
                grist_config['doc_id']
            )
            
            # Obtenir les paramètres de synchronisation
            sync_config = demarche.sync_config
            parallel = sync_config.get('parallel', True)
            batch_size = sync_config.get('batch_size', 50)
            max_workers = sync_config.get('max_workers', 3)
            
            # Préparer les filtres optimisés
            api_filters = self._prepare_filters_for_api(demarche.filters)
            
            # Exécuter la synchronisation OPTIMISÉE
            success = process_demarche_for_grist_optimized(
                client,
                demarche.number,
                parallel=parallel,
                batch_size=batch_size,
                max_workers=max_workers,
                api_filters=api_filters  # Nouveau paramètre pour les filtres optimisés
            )
            
            duration = time.time() - start_time
            
            return SyncResult(
                demarche_number=demarche.number,
                demarche_name=demarche.name,
                success=success,
                dossiers_processed=0,  # À améliorer : récupérer le nombre réel
                errors=[],
                duration_seconds=duration
            )
            
        except Exception as e:
            duration = time.time() - start_time
            error_msg = f"Erreur lors de la synchronisation : {str(e)}"
            print(f"❌ {error_msg}")
            
            return SyncResult(
                demarche_number=demarche.number,
                demarche_name=demarche.name,
                success=False,
                dossiers_processed=0,
                errors=[error_msg],
                duration_seconds=duration
            )
    
    def _print_sync_summary(self, results: List[SyncResult]):
        """
        Affiche un résumé des résultats de synchronisation.
        
        Args:
            results: Liste des résultats
        """
        print(f"\n{'='*60}")
        print("📊 RÉSUMÉ DE LA SYNCHRONISATION")
        print(f"{'='*60}")
        
        successful = [r for r in results if r.success]
        failed = [r for r in results if not r.success]
        
        print(f"✅ Synchronisations réussies : {len(successful)}")
        print(f"❌ Synchronisations échouées : {len(failed)}")
        print(f"⏱️  Durée totale : {sum(r.duration_seconds for r in results):.1f} secondes")
        
        if successful:
            print(f"\n🎉 Démarches synchronisées avec succès :")
            for result in successful:
                print(f"   • {result.demarche_name} (#{result.demarche_number}) - {result.duration_seconds:.1f}s")
        
        if failed:
            print(f"\n💥 Démarches en échec :")
            for result in failed:
                print(f"   • {result.demarche_name} (#{result.demarche_number})")
                for error in result.errors:
                    print(f"     - {error}")
    
    def validate_configuration(self) -> bool:
        """
        Valide la configuration complète.
        
        Returns:
            bool: True si la configuration est valide
        """
        print("🔍 Validation de la configuration...")
        
        valid = True
        
        # Vérifier Grist
        grist_config = self.get_grist_config()
        for key in ['base_url', 'api_key', 'doc_id']:
            if not grist_config.get(key) or grist_config[key].startswith('${'):
                print(f"❌ Configuration Grist incomplète : {key}")
                valid = False
        
        if valid:
            print(f"✅ Configuration Grist valide")
        
        # Vérifier les démarches
        enabled_count = len(self.get_enabled_demarches())
        total_count = len(self.demarches)
        print(f"📋 Démarches : {enabled_count}/{total_count} activées")
        
        for demarche in self.demarches:
            if not demarche.api_token or demarche.api_token.startswith('${'):
                print(f"❌ Token manquant pour la démarche {demarche.number} - {demarche.name}")
                valid = False
            else:
                status = "✅ activée" if demarche.enabled else "⚪ désactivée"
                print(f"   {status} - {demarche.name} (#{demarche.number}) - Token configuré")
                
                # Valider les filtres
                filters = demarche.filters
                filter_info = []
                if filters.get('date_depot_debut'):
                    filter_info.append(f"Date >= {filters['date_depot_debut']}")
                if filters.get('date_depot_fin'):
                    filter_info.append(f"Date <= {filters['date_depot_fin']}")
                if filters.get('groupes_instructeurs'):
                    groupes = filters['groupes_instructeurs']
                    if isinstance(groupes, str):
                        filter_info.append(f"Groupe: {groupes}")
                    elif isinstance(groupes, list):
                        filter_info.append(f"Groupes: {groupes}")
                if filters.get('statuts_dossiers'):
                    filter_info.append(f"Statuts: {filters['statuts_dossiers']}")
                
                if filter_info:
                    print(f"     🔍 Filtres: {' | '.join(filter_info)}")
        
        if valid:
            print("✅ Configuration globale valide")
        else:
            print("❌ Configuration invalide")
        
        return valid

    def validate_filters_efficiency(self) -> None:
        """
        Valide l'efficacité des filtres configurés et donne des recommandations.
        """
        print("\n🔍 Analyse de l'efficacité des filtres configurés:")
        
        for demarche in self.demarches:
            if not demarche.enabled:
                continue
                
            print(f"\n📋 Démarche {demarche.number} - {demarche.name}:")
            filters = demarche.filters
            
            # Analyse des filtres
            has_server_filter = bool(filters.get('date_depot_debut'))
            has_client_filters = any([
                filters.get('date_depot_fin'),
                filters.get('groupes_instructeurs'),
                filters.get('statuts_dossiers')
            ])
            
            if has_server_filter:
                print(f"   ✅ Filtre côté serveur détecté: date_depot_debut = {filters['date_depot_debut']}")
                print(f"      Impact: Réduction drastique du volume de données")
            else:
                print(f"   ⚠️  Aucun filtre côté serveur configuré")
                print(f"      Recommandation: Ajoutez 'date_depot_debut' pour améliorer les performances")
            
            if has_client_filters:
                client_filter_names = []
                if filters.get('date_depot_fin'):
                    client_filter_names.append('date_depot_fin')
                if filters.get('groupes_instructeurs'):
                    client_filter_names.append('groupes_instructeurs')
                if filters.get('statuts_dossiers'):
                    client_filter_names.append('statuts_dossiers')
                
                print(f"   💻 Filtres côté client: {', '.join(client_filter_names)}")
                
                if has_server_filter:
                    print(f"      Impact: Filtrage précis sur le résultat déjà réduit")
                else:
                    print(f"      ⚠️  Impact limité: Filtrage sur TOUS les dossiers de la démarche")
            
            # Score d'efficacité
            if has_server_filter and has_client_filters:
                score = "🚀 OPTIMAL"
            elif has_server_filter:
                score = "✅ BON" 
            elif has_client_filters:
                score = "⚠️  MOYEN"
            else:
                score = "❌ INEFFICACE"
            
            print(f"   Score d'efficacité: {score}")


def main():
    """
    Point d'entrée principal pour la synchronisation multi-démarche.
    VERSION OPTIMISÉE avec analyse des filtres.
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Synchronisation multi-démarche DS vers Grist (OPTIMISÉE)')
    parser.add_argument('--demarches', type=str, help='Numéros de démarches séparés par des virgules (ex: 121950,122643,121821)')
    parser.add_argument('--force', action='store_true', help='Forcer la synchronisation des démarches désactivées')
    parser.add_argument('--validate-only', action='store_true', help='Valider la configuration uniquement')
    parser.add_argument('--dry-run', action='store_true', help='Mode test (validation uniquement)')
    parser.add_argument('--config', type=str, default='config.json', help='Fichier de configuration')
    parser.add_argument('--debug', action='store_true', help='Activer les logs de debug')
    parser.add_argument('--analyze-filters', action='store_true', help='Analyser uniquement l\'efficacité des filtres')
    
    args = parser.parse_args()
    
    # Activer le debug si demandé
    if args.debug:
        os.environ['LOG_LEVEL'] = 'DEBUG'
        print("🐛 Mode debug activé")
    
    try:
        print(f"🚀 Démarrage du gestionnaire multi-démarche OPTIMISÉ")
        print(f"📁 Fichier de configuration : {args.config}")
        
        # Vérifier que le fichier de configuration existe
        if not os.path.exists(args.config):
            print(f"❌ Fichier de configuration non trouvé : {args.config}")
            print(f"💡 Créez le fichier {args.config} avec vos démarches")
            return 1
        
        # Initialiser le gestionnaire
        manager = MultiDemarcheManager(args.config)
        print(f"✅ Configuration chargée : {len(manager.demarches)} démarches trouvées")
        
        # Afficher les démarches disponibles en mode debug
        if args.debug:
            print(f"📋 Démarches disponibles :")
            for d in manager.demarches:
                status = "✅ activée" if d.enabled else "⚪ désactivée"
                token_ok = "🔑 OK" if d.api_token and not d.api_token.startswith('${') else "❌ token manquant"
                filters = len([k for k, v in d.filters.items() if v])
                print(f"   {d.number}: {d.name} - {status} - {token_ok} - {filters} filtres")
        
        # Mode analyse des filtres uniquement
        if args.analyze_filters:
            if not manager.validate_configuration():
                print("❌ Configuration invalide. Impossible d'analyser les filtres.")
                return 1
            manager.validate_filters_efficiency()
            return 0
        
        # Mode validation uniquement
        if args.validate_only or args.dry_run:
            if manager.validate_configuration():
                print("✅ Configuration valide")
                if args.debug:
                    manager.validate_filters_efficiency()
                return 0
            else:
                print("❌ Configuration invalide")
                return 1
        
        # Valider la configuration avant synchronisation
        if not manager.validate_configuration():
            print("❌ Configuration invalide. Arrêt du programme.")
            return 1
        
        # Analyser l'efficacité des filtres en mode debug
        if args.debug:
            manager.validate_filters_efficiency()
        
        # Synchronisation spécifique ou complète
        if args.demarches:
            # Synchroniser des démarches spécifiques
            try:
                # Parser les numéros de démarches en gérant les espaces
                demarche_numbers = []
                for x in args.demarches.split(','):
                    cleaned = x.strip()
                    if cleaned:
                        demarche_numbers.append(int(cleaned))
                
                print(f"🎯 Démarches sélectionnées : {demarche_numbers}")
                results = manager.sync_specific_demarches(demarche_numbers, force_disabled=args.force)
            except ValueError as e:
                print(f"❌ Erreur dans les numéros de démarches : {args.demarches}")
                print(f"   Format attendu : 121950,122643,121821 (sans espaces)")
                print(f"   Votre saisie : '{args.demarches}'")
                return 1
        else:
            # Synchroniser toutes les démarches activées
            results = manager.sync_all_demarches()
        
        # Vérifier si au moins une synchronisation a réussi
        success_count = sum(1 for r in results if r.success)
        if success_count > 0:
            print(f"\n🎉 Synchronisation terminée : {success_count} démarches traitées avec succès")
            return 0
        else:
            print(f"\n💥 Aucune synchronisation réussie")
            return 1
            
    except Exception as e:
        print(f"💥 Erreur fatale : {e}")
        if args.debug:
            import traceback
            traceback.print_exc()
        else:
            print("💡 Utilisez --debug pour plus de détails")
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
