"""
Configuration adaptée pour le système multi-démarche.
Gère les tokens API selon l'organisation de la démarche en cours.
"""

import os
from dotenv import load_dotenv
from typing import Optional

# Charger les variables d'environnement
load_dotenv()

# Configuration pour compatibilité avec le code existant
# Ces variables sont configurées dynamiquement par le gestionnaire multi-démarche
API_TOKEN = os.getenv("DEMARCHES_API_TOKEN")
API_URL = os.getenv("DEMARCHES_API_URL", "https://www.demarches-simplifiees.fr/api/v2/graphql")

class DemarcheAPIConfig:
    """
    Gestionnaire de configuration API pour le système multi-démarche.
    Permet de récupérer les tokens selon l'organisation.
    """
    
    @staticmethod
    def get_token_for_organization(organization_key: str) -> Optional[str]:
        """
        Récupère le token API pour une organisation donnée.
        
        Args:
            organization_key: Clé de l'organisation (ex: "aura_region", "metropole_lyon")
            
        Returns:
            str: Token API ou None si non trouvé
        """
        env_var_name = f"DEMARCHES_API_TOKEN_{organization_key.upper()}"
        return os.getenv(env_var_name)
    
    @staticmethod
    def set_current_api_config(api_token: str, api_url: str = None):
        """
        Configure temporairement les variables API pour la démarche en cours.
        Utilisé par le gestionnaire multi-démarche.
        
        Args:
            api_token: Token API à utiliser
            api_url: URL de l'API (optionnel)
        """
        global API_TOKEN, API_URL
        
        os.environ["DEMARCHES_API_TOKEN"] = api_token
        API_TOKEN = api_token
        
        if api_url:
            os.environ["DEMARCHES_API_URL"] = api_url
            API_URL = api_url
    
    @staticmethod
    def validate_current_config() -> bool:
        """
        Valide que la configuration API actuelle est valide.
        
        Returns:
            bool: True si la configuration est valide
        """
        global API_TOKEN, API_URL
        
        if not API_TOKEN or API_TOKEN.startswith('${'):
            return False
        
        if not API_URL:
            return False
        
        return True
    
    @classmethod
    def get_available_organizations(cls) -> dict:
        """
        Retourne la liste des organisations configurées avec leurs tokens.
        
        Returns:
            dict: Dictionnaire {org_key: bool} indiquant si le token est configuré
        """
        organizations = {}
        
        # Scanner les variables d'environnement pour trouver les tokens
        for env_var in os.environ:
            if env_var.startswith("DEMARCHES_API_TOKEN_") and env_var != "DEMARCHES_API_TOKEN":
                org_key = env_var.replace("DEMARCHES_API_TOKEN_", "").lower()
                token = os.getenv(env_var)
                organizations[org_key] = bool(token and not token.startswith('${'))
        
        return organizations
    
    @classmethod
    def print_status(cls):
        """
        Affiche le statut de la configuration API.
        """
        global API_TOKEN, API_URL
        
        print("📡 Configuration API Démarches Simplifiées")
        print(f"   URL: {API_URL}")
        print(f"   Token actuel: {'✅ Configuré' if API_TOKEN and not API_TOKEN.startswith('${') else '❌ Non configuré'}")
        
        organizations = cls.get_available_organizations()
        if organizations:
            print(f"   Organisations disponibles :")
            for org_key, configured in organizations.items():
                status = "✅" if configured else "❌"
                print(f"     - {org_key}: {status}")
        else:
            print("   Aucune organisation configurée")

# Fonction de compatibilité pour les scripts existants
def get_api_token():
    """
    Retourne le token API actuel.
    Fonction de compatibilité avec le code existant.
    """
    return API_TOKEN

def get_api_url():
    """
    Retourne l'URL API actuelle.
    Fonction de compatibilité avec le code existant.
    """
    return API_URL

# Validation au chargement du module
if __name__ == "__main__":
    # Code de test pour vérifier la configuration
    config = DemarcheAPIConfig()
    config.print_status()
    
    # Tester la récupération des tokens par organisation
    organizations = config.get_available_organizations()
    
    if organizations:
        print(f"\n🧪 Test de récupération des tokens :")
        for org_key, configured in organizations.items():
            if configured:
                token = config.get_token_for_organization(org_key)
                print(f"   {org_key}: Token de {len(token) if token else 0} caractères")
    
    # Validation de la configuration actuelle
    if config.validate_current_config():
        print("\n✅ Configuration API actuelle valide")
    else:
        print("\n⚠️  Configuration API actuelle incomplète")
        print("   Assurez-vous que DEMARCHES_API_TOKEN est défini")

# Affichage d'informations de debug si le module est importé en mode verbose
def _debug_import():
    """Affiche des informations de debug lors de l'import du module."""
    debug_mode = os.getenv("DEBUG_CONFIG", "false").lower() == "true"
    
    if debug_mode:
        print(f"🔧 [DEBUG] queries_config.py chargé")
        print(f"   API_TOKEN: {'✅' if API_TOKEN and not API_TOKEN.startswith('${') else '❌'}")
        print(f"   API_URL: {API_URL}")
        
        organizations = DemarcheAPIConfig.get_available_organizations()
        print(f"   Organisations: {len(organizations)} configurées")

# Appeler la fonction de debug
_debug_import()