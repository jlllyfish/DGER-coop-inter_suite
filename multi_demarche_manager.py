#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gestionnaire multidémarche optimisé intégrant les améliorations des scripts fournis.

AMÉLIORATIONS PRINCIPALES :
1. Récupération de schéma optimisée avec cache
2. Filtrage côté serveur pour la récupération des dossiers
3. Gestion d'erreur robuste avec fallback automatique
4. Cache intelligent des colonnes Grist
5. Parallélisation optimisée par démarche
"""

import os
import sys
import time
import json
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from datetime import datetime

# Import des modules optimisés
from schema_utils import (
    get_demarche_schema_enhanced,
    smart_schema_update,
    detect_schema_changes
)
from queries_graphql import get_demarche_dossiers_filtered
from grist_processor_working_all import (
    GristClient, 
    ColumnCache,
    process_demarche_for_grist_optimized
)

@dataclass
class OptimizedSyncConfig:
    """Configuration optimisée pour la synchronisation"""
    # Paramètres de performance
    use_robust_schema: bool = True
    enable_server_side_filtering: bool = True
    enable_column_cache: bool = True
    enable_parallel_processing: bool = True
    
    # Paramètres de traitement
    batch_size: int = 100
    max_workers: int = 3
    schema_cache_duration: int = 3600  # 1 heure en secondes
    
    # Filtres par défaut
    default_filters: Dict[str, Any] = None

@dataclass 
class DemarcheProcessingResult:
    """Résultat du traitement d'une démarche"""
    demarche_number: int
    demarche_name: str
    success: bool
    dossiers_processed: int
    duration_seconds: float
    schema_optimized: bool = False
    filtering_optimized: bool = False
    errors: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []

class OptimizedMultiDemarcheManager:
    """
    Gestionnaire multidémarche optimisé avec les améliorations des scripts fournis.
    """
    
    def __init__(self, config_path: str = "multi_demarche_config.json"):
        self.config_path = config_path
        self.config = self._load_config()
        self.schema_cache = {}  # Cache des schémas par démarche
        self.column_caches = {}  # Cache des colonnes Grist par document
        self.sync_config = OptimizedSyncConfig()
        
    def _load_config(self) -> Dict[str, Any]:
        """Charge la configuration avec gestion d'erreur robuste"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"❌ Fichier de configuration non trouvé : {self.config_path}")
            return self._create_default_config()
        except json.JSONDecodeError as e:
            print(f"❌ Erreur dans le fichier de configuration : {e}")
            return self._create_default_config()
    
    def _create_default_config(self) -> Dict[str, Any]:
        """Crée une configuration par défaut"""
        return {
            "grist": {
                "base_url": os.getenv("GRIST_BASE_URL", ""),
                "api_key": os.getenv("GRIST_API_KEY", ""),
                "doc_id": os.getenv("GRIST_DOC_ID", "")
            },
            "demarches": []
        }
    
    def get_schema_optimized(self, demarche_number: int, force_refresh: bool = False) -> Dict[str, Any]:
        """
        Récupération optimisée du schéma avec cache intelligent.
        
        AMÉLIORATION CLÉE : Cache des schémas pour éviter les requêtes répétées
        """
        cache_key = str(demarche_number)
        current_time = time.time()
        
        # Vérifier le cache si pas de rafraîchissement forcé
        if not force_refresh and cache_key in self.schema_cache:
            cached_data = self.schema_cache[cache_key]
            if current_time - cached_data['timestamp'] < self.sync_config.schema_cache_duration:
                print(f"📋 Utilisation du schéma en cache pour la démarche {demarche_number}")
                return cached_data['schema']
        
        # Récupération optimisée avec la fonction enhanced
        print(f"🔄 Récupération optimisée du schéma pour la démarche {demarche_number}")
        try:
            schema = get_demarche_schema_enhanced(
                demarche_number, 
                prefer_robust=self.sync_config.use_robust_schema
            )
            
            # Mise en cache
            self.schema_cache[cache_key] = {
                'schema': schema,
                'timestamp': current_time
            }
            
            print(f"✅ Schéma récupéré et mis en cache")
            return schema
            
        except Exception as e:
            print(f"❌ Erreur lors de la récupération du schéma : {e}")
            # Tentative avec le cache expiré si disponible
            if cache_key in self.schema_cache:
                print("🔄 Utilisation du cache expiré comme fallback")
                return self.schema_cache[cache_key]['schema']
            raise
    
    def get_dossiers_optimized(self, demarche_number: int, filters: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        Récupération optimisée des dossiers avec filtrage côté serveur.
        
        AMÉLIORATION CLÉE : Utilise le filtrage côté serveur pour réduire la charge
        """
        if not self.sync_config.enable_server_side_filtering:
            # Fallback vers la méthode classique
            from queries_graphql import get_demarche_dossiers
            return get_demarche_dossiers(demarche_number)
        
        # Fusion des filtres par défaut et spécifiques
        final_filters = {}
        if self.sync_config.default_filters:
            final_filters.update(self.sync_config.default_filters)
        if filters:
            final_filters.update(filters)
        
        print(f"🎯 Récupération optimisée des dossiers avec filtres côté serveur")
        if final_filters:
            print(f"   Filtres appliqués : {list(final_filters.keys())}")
        
        try:
            dossiers = get_demarche_dossiers_filtered(
                demarche_number,
                date_debut=final_filters.get('date_debut'),
                date_fin=final_filters.get('date_fin'),
                groupes_instructeurs=final_filters.get('groupes_instructeurs'),
                statuts=final_filters.get('statuts')
            )
            
            print(f"✅ {len(dossiers)} dossiers récupérés avec filtrage optimisé")
            return dossiers
            
        except Exception as e:
            print(f"❌ Erreur filtrage optimisé : {e}")
            print("🔄 Fallback vers la méthode classique")
            from queries_graphql import get_demarche_dossiers
            return get_demarche_dossiers(demarche_number)
    
    def get_column_cache(self, grist_doc_id: str) -> ColumnCache:
        """
        Récupère ou crée un cache de colonnes pour un document Grist.
        
        AMÉLIORATION CLÉE : Cache partagé des colonnes pour éviter les requêtes répétées
        """
        if grist_doc_id not in self.column_caches:
            grist_config = self.config['grist']
            client = GristClient(
                grist_config['base_url'],
                grist_config['api_key'],
                grist_doc_id
            )
            self.column_caches[grist_doc_id] = ColumnCache(client)
        
        return self.column_caches[grist_doc_id]
    
    def sync_demarche_optimized(self, demarche_config: Dict[str, Any]) -> DemarcheProcessingResult:
        """
        Synchronise une démarche avec toutes les optimisations activées.
        
        INTÉGRATION COMPLÈTE des améliorations des scripts fournis.
        """
        demarche_number = demarche_config['number']
        demarche_name = demarche_config.get('name', f"Démarche {demarche_number}")
        start_time = time.time()
        
        print(f"\n🚀 Synchronisation optimisée : {demarche_name} (#{demarche_number})")
        
        try:
            # Configuration de l'environnement pour cette démarche
            self._configure_environment_for_demarche(demarche_config)
            
            # 1. RÉCUPÉRATION OPTIMISÉE DU SCHÉMA
            schema_optimized = False
            try:
                schema = self.get_schema_optimized(demarche_number)
                schema_optimized = schema.get('metadata', {}).get('optimized', False)
                print(f"   📋 Schéma : {'Optimisé' if schema_optimized else 'Classique'}")
            except Exception as e:
                print(f"   ❌ Erreur schéma : {e}")
                raise
            
            # 2. MISE À JOUR INTELLIGENTE DES TABLES GRIST
            grist_config = self.config['grist']
            client = GristClient(
                grist_config['base_url'],
                grist_config['api_key'],
                grist_config['doc_id']
            )
            
            # Utiliser le cache de colonnes
            if self.sync_config.enable_column_cache:
                column_cache = self.get_column_cache(grist_config['doc_id'])
                client._column_cache = column_cache
            
            # Mise à jour intelligente des tables
            update_result = smart_schema_update(
                client, 
                demarche_number, 
                use_robust_version=self.sync_config.use_robust_schema
            )
            
            if not update_result['success']:
                raise Exception(f"Échec mise à jour tables : {update_result.get('error')}")
            
            # 3. RÉCUPÉRATION OPTIMISÉE DES DOSSIERS AVEC FILTRES
            filtering_optimized = False
            try:
                filters = self._build_filters_for_demarche(demarche_config)
                api_filters = self._convert_to_api_filters(filters) if filters else {}
                
                if api_filters:
                    filtering_optimized = True
                    print(f"   🎯 Filtrage côté serveur activé")
                
            except Exception as e:
                print(f"   ⚠️ Erreur construction filtres : {e}")
                api_filters = {}
            
            # 4. TRAITEMENT PARALLÈLE OPTIMISÉ
            success = process_demarche_for_grist_optimized(
                client,
                demarche_number,
                parallel=self.sync_config.enable_parallel_processing,
                batch_size=self.sync_config.batch_size,
                max_workers=self.sync_config.max_workers,
                api_filters=api_filters  # Passer les filtres optimisés
            )
            
            duration = time.time() - start_time
            
            if success:
                print(f"   ✅ Synchronisation réussie en {duration:.1f}s")
            else:
                print(f"   ❌ Échec de la synchronisation")
            
            return DemarcheProcessingResult(
                demarche_number=demarche_number,
                demarche_name=demarche_name,
                success=success,
                dossiers_processed=0,  # À améliorer
                duration_seconds=duration,
                schema_optimized=schema_optimized,
                filtering_optimized=filtering_optimized
            )
            
        except Exception as e:
            duration = time.time() - start_time
            error_msg = f"Erreur : {str(e)}"
            print(f"   ❌ {error_msg}")
            
            return DemarcheProcessingResult(
                demarche_number=demarche_number,
                demarche_name=demarche_name,
                success=False,
                dossiers_processed=0,
                duration_seconds=duration,
                errors=[error_msg]
            )
    
    def _configure_environment_for_demarche(self, demarche_config: Dict[str, Any]):
        """Configure l'environnement pour une démarche spécifique"""
        # Configuration des tokens API par démarche
        api_token = demarche_config.get('api_token')
        if api_token and not api_token.startswith('${'):
            os.environ['DEMARCHES_API_TOKEN'] = api_token
            
            # Force la mise à jour dans queries_config
            import queries_config
            queries_config.API_TOKEN = api_token
            
            if hasattr(queries_config, 'DemarcheAPIConfig'):
                queries_config.DemarcheAPIConfig.set_organization(
                    demarche_config.get('organization', 'default')
                )
    
    def _build_filters_for_demarche(self, demarche_config: Dict[str, Any]) -> Dict[str, Any]:
        """Construit les filtres pour une démarche à partir de sa configuration"""
        filters = {}
        
        # Récupérer les filtres depuis la configuration de la démarche
        if 'filters' in demarche_config:
            filters.update(demarche_config['filters'])
        
        # Ajouter les filtres par défaut si pas déjà définis
        if self.sync_config.default_filters:
            for key, value in self.sync_config.default_filters.items():
                if key not in filters:
                    filters[key] = value
        
        return filters
    
    def _convert_to_api_filters(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """Convertit les filtres de configuration vers le format API"""
        api_filters = {}
        
        # Mapping des clés de configuration vers les clés API
        key_mapping = {
            'date_debut': 'date_debut',
            'date_fin': 'date_fin', 
            'statuts': 'statuts',
            'groupes_instructeurs': 'groupes_instructeurs'
        }
        
        for config_key, api_key in key_mapping.items():
            if config_key in filters:
                api_filters[api_key] = filters[config_key]
        
        return api_filters
    
    def sync_all_optimized(self) -> List[DemarcheProcessingResult]:
        """
        Synchronise toutes les démarches activées avec optimisations.
        """
        enabled_demarches = [d for d in self.config.get('demarches', []) if d.get('enabled', True)]
        
        if not enabled_demarches:
            print("❌ Aucune démarche activée trouvée")
            return []
        
        print(f"🚀 Synchronisation optimisée de {len(enabled_demarches)} démarches")
        print(f"   📋 Schéma optimisé : {'✅' if self.sync_config.use_robust_schema else '❌'}")
        print(f"   🎯 Filtrage côté serveur : {'✅' if self.sync_config.enable_server_side_filtering else '❌'}")
        print(f"   💾 Cache de colonnes : {'✅' if self.sync_config.enable_column_cache else '❌'}")
        print(f"   ⚡ Traitement parallèle : {'✅' if self.sync_config.enable_parallel_processing else '❌'}")
        
        results = []
        
        for i, demarche_config in enumerate(enabled_demarches, 1):
            print(f"\n📋 Démarche {i}/{len(enabled_demarches)}")
            
            result = self.sync_demarche_optimized(demarche_config)
            results.append(result)
            
            # Pause entre démarches pour éviter la surcharge
            if i < len(enabled_demarches):
                print("⏸️ Pause de 2 secondes...")
                time.sleep(2)
        
        self._print_optimization_summary(results)
        return results
    
    def _print_optimization_summary(self, results: List[DemarcheProcessingResult]):
        """Affiche un résumé des optimisations appliquées"""
        if not results:
            return
        
        total_duration = sum(r.duration_seconds for r in results)
        success_count = sum(1 for r in results if r.success)
        schema_optimized_count = sum(1 for r in results if r.schema_optimized)
        filtering_optimized_count = sum(1 for r in results if r.filtering_optimized)
        
        print(f"\n🎯 RÉSUMÉ DES OPTIMISATIONS")
        print(f"   ✅ Succès : {success_count}/{len(results)}")
        print(f"   ⏱️ Durée totale : {total_duration:.1f}s")
        print(f"   📋 Schémas optimisés : {schema_optimized_count}/{len(results)}")
        print(f"   🎯 Filtrage optimisé : {filtering_optimized_count}/{len(results)}")
        
        if success_count > 0:
            avg_duration = total_duration / len(results)
            print(f"   📊 Durée moyenne : {avg_duration:.1f}s par démarche")

def main():
    """Point d'entrée principal avec support des optimisations"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Gestionnaire multidémarche optimisé")
    parser.add_argument('--config', default='multi_demarche_config.json', 
                       help='Chemin vers le fichier de configuration')
    parser.add_argument('--demarches', help='Numéros de démarches séparés par des virgules')
    parser.add_argument('--disable-schema-optimization', action='store_true',
                       help='Désactiver l\'optimisation du schéma')
    parser.add_argument('--disable-server-filtering', action='store_true',
                       help='Désactiver le filtrage côté serveur')
    parser.add_argument('--disable-column-cache', action='store_true',
                       help='Désactiver le cache des colonnes')
    parser.add_argument('--disable-parallel', action='store_true',
                       help='Désactiver le traitement parallèle')
    parser.add_argument('--batch-size', type=int, default=100,
                       help='Taille des lots (défaut: 100)')
    parser.add_argument('--max-workers', type=int, default=3,
                       help='Nombre maximum de workers (défaut: 3)')
    
    args = parser.parse_args()
    
    # Créer le gestionnaire
    manager = OptimizedMultiDemarcheManager(args.config)
    
    # Configuration des optimisations
    manager.sync_config.use_robust_schema = not args.disable_schema_optimization
    manager.sync_config.enable_server_side_filtering = not args.disable_server_filtering
    manager.sync_config.enable_column_cache = not args.disable_column_cache
    manager.sync_config.enable_parallel_processing = not args.disable_parallel
    manager.sync_config.batch_size = args.batch_size
    manager.sync_config.max_workers = args.max_workers
    
    # Activer le debug si demandé
    if args.debug:
        import os
        os.environ['LOG_LEVEL'] = 'DEBUG'
        print("🐛 Mode debug activé")
    
    try:
        # Vérifier que le fichier de configuration existe
        if not os.path.exists(args.config):
            print(f"❌ Fichier de configuration non trouvé : {args.config}")
            print(f"💡 Créez le fichier {args.config} avec vos démarches")
            return 1
        
        print(f"✅ Configuration chargée : {len(manager.config.get('demarches', []))} démarches trouvées")
        
        # Gestion des commandes compatibles avec l'ancien système
        if args.validate_only or args.dry_run:
            print("🔍 Validation de la configuration...")
            try:
                # Test de validation simple
                enabled_demarches = [d for d in manager.config.get('demarches', []) if d.get('enabled', True)]
                if enabled_demarches:
                    print(f"✅ Configuration valide : {len(enabled_demarches)} démarches activées")
                    return 0
                else:
                    print("❌ Aucune démarche activée trouvée")
                    return 1
            except Exception as e:
                print(f"❌ Configuration invalide : {e}")
                return 1
        
        elif args.sync_all:
            print("🔄 Synchronisation de toutes les démarches (optimisé)...")
            results = manager.sync_all_optimized()
            
        elif args.sync:
            # Compatibilité avec --sync NUMERO
            demarche_numbers = [int(args.sync.strip())]
            print(f"🎯 Synchronisation de la démarche : {demarche_numbers[0]}")
            
            # Filtrer les démarches à traiter
            all_demarches = manager.config.get('demarches', [])
            specific_demarches = [d for d in all_demarches if d['number'] in demarche_numbers]
            
            if not specific_demarches:
                print(f"❌ Démarche {demarche_numbers[0]} non trouvée dans la configuration")
                return 1
            
            results = []
            for demarche_config in specific_demarches:
                result = manager.sync_demarche_optimized(demarche_config)
                results.append(result)
        
        elif args.demarches:
            # Synchronisation de démarches spécifiques (nouvelle syntaxe)
            try:
                demarche_numbers = [int(x.strip()) for x in args.demarches.split(',') if x.strip()]
                print(f"🎯 Démarches sélectionnées : {demarche_numbers}")
                
                # Filtrer les démarches à traiter
                all_demarches = manager.config.get('demarches', [])
                specific_demarches = [d for d in all_demarches if d['number'] in demarche_numbers]
                
                results = []
                for demarche_config in specific_demarches:
                    result = manager.sync_demarche_optimized(demarche_config)
                    results.append(result)
            except ValueError as e:
                print(f"❌ Erreur dans les numéros de démarches : {args.demarches}")
                return 1
        else:
            # Par défaut, synchroniser toutes les démarches
            print("🚀 Synchronisation de toutes les démarches (aucune option spécifiée)")
            results = manager.sync_all_optimized()
        
        # Vérifier le succès
        if 'results' in locals():
            success_count = sum(1 for r in results if r.success)
            if success_count > 0:
                print(f"\n🎉 Synchronisation terminée : {success_count} démarches traitées avec succès")
                return 0
            else:
                print("\n💥 Aucune synchronisation réussie")
                return 1
        else:
            return 0
            
    except Exception as e:
        print(f"💥 Erreur fatale : {e}")
        if args.debug:
            import traceback
            traceback.print_exc()
        else:
            print("💡 Utilisez --debug pour plus de détails")
        return 1

if __name__ == "__main__":
    sys.exit(main())
