import os
import traceback
from pprint import pprint
from dotenv import load_dotenv

# Import des modules locaux
from queries_config import API_TOKEN
from queries_graphql import get_dossier, get_demarche, get_demarche_dossiers, get_dossier_geojson
from queries_util import format_complex_json_for_grist, associate_geojson_with_champs
from queries_extract import extract_champ_values, dossier_to_flat_data

# Exposer les fonctions principales pour l'importation dans d'autres scripts
__all__ = [
    'get_dossier', 
    'get_demarche', 
    'get_demarche_dossiers', 
    'get_dossier_geojson',
    'extract_champ_values', 
    'dossier_to_flat_data', 
    'associate_geojson_with_champs',
    'format_complex_json_for_grist'
]

# Code d'exemple pour tester le script
if __name__ == "__main__":
    try:
        # Charger les variables d'environnement
        load_dotenv()
        
        # Récupérer le numéro de démarche depuis le fichier .env
        demarche_number = os.getenv("DEMARCHE_NUMBER")
        if demarche_number:
            demarche_number = int(demarche_number)
            print(f"Récupération de la démarche {demarche_number}...")
            
            # Récupérer la démarche
            demarche_data = get_demarche(demarche_number)
            
            print("\nInformations de la démarche:")
            print(f"Titre: {demarche_data['title']}")
            print(f"État: {demarche_data['state']}")
        else:
            print("Aucun numéro de démarche trouvé dans le fichier .env.")
        
    except Exception as e:
        print(f"Erreur: {e}")
        traceback.print_exc()