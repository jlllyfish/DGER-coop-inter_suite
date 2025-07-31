#!/usr/bin/env python3
"""
Script de lancement pour le projet OTP multi-démarches
Permet de lancer facilement toutes les options du projet sans se soucier des chemins.
"""

import os
import sys
import subprocess
import argparse
from pathlib import Path

class OTPLauncher:
    def __init__(self):
        """Initialise le lanceur OTP"""
        # Déterminer le répertoire du script
        self.script_dir = Path(__file__).parent.absolute()
        
        # Chemins importants
        self.project_dir = self.script_dir
        self.manager_script = self.project_dir / "multi_demarche_manager.py"
        self.test_script = self.project_dir / "test-token.py"
        
        # Trouver Python
        self.python_exe = sys.executable
        
        print(f"🎯 Projet OTP - Lanceur")
        print(f"📁 Répertoire: {self.project_dir}")
        print(f"🐍 Python: {self.python_exe}")
        print()
    
    def run_command(self, args, description=""):
        """Exécute une commande"""
        if description:
            print(f"🚀 {description}")
        
        print(f"💻 Commande: {' '.join(args)}")
        print("-" * 60)
        
        try:
            # Changer vers le répertoire du projet
            os.chdir(self.project_dir)
            
            # Exécuter la commande
            result = subprocess.run(args, cwd=self.project_dir)
            
            print("-" * 60)
            if result.returncode == 0:
                print("✅ Commande terminée avec succès")
            else:
                print(f"❌ Commande terminée avec le code d'erreur {result.returncode}")
            
            return result.returncode == 0
            
        except Exception as e:
            print(f"❌ Erreur lors de l'exécution: {e}")
            return False
    
    def validate_config(self):
        """Valide la configuration"""
        args = [self.python_exe, str(self.manager_script), "--validate-only", "--debug"]
        return self.run_command(args, "Validation de la configuration")
    
    def sync_all(self):
        """Synchronise toutes les démarches"""
        args = [self.python_exe, str(self.manager_script), "--debug"]
        return self.run_command(args, "Synchronisation de toutes les démarches")
    
    def sync_specific(self, demarches):
        """Synchronise des démarches spécifiques"""
        if isinstance(demarches, list):
            demarches_str = ",".join(map(str, demarches))
        else:
            demarches_str = str(demarches)
        
        args = [self.python_exe, str(self.manager_script), "--demarches", demarches_str, "--debug"]
        return self.run_command(args, f"Synchronisation des démarches: {demarches_str}")
    
    def test_tokens(self):
        """Teste les tokens"""
        if self.test_script.exists():
            args = [self.python_exe, str(self.test_script)]
            return self.run_command(args, "Test des tokens")
        else:
            print("❌ Script test-token.py non trouvé")
            return False
    
    def interactive_menu(self):
        """Menu interactif"""
        while True:
            print("\n" + "="*60)
            print("🎯 PROJET OTP - MENU PRINCIPAL")
            print("="*60)
            print("1. 🔍 Valider la configuration")
            print("2. 🔄 Synchroniser TOUTES les démarches")
            print("3. 🎯 Synchroniser une démarche spécifique")
            print("4. 📊 Synchroniser plusieurs démarches")
            print("5. 🧪 Tester les tokens")
            print("6. ❓ Aide")
            print("0. 🚪 Quitter")
            print("-"*60)
            
            try:
                choice = input("Votre choix (0-6): ").strip()
                
                if choice == "0":
                    print("👋 Au revoir !")
                    break
                    
                elif choice == "1":
                    self.validate_config()
                    
                elif choice == "2":
                    confirm = input("⚠️  Voulez-vous vraiment synchroniser TOUTES les démarches ? (o/N): ").strip().lower()
                    if confirm in ['o', 'oui', 'y', 'yes']:
                        self.sync_all()
                    else:
                        print("❌ Synchronisation annulée")
                        
                elif choice == "3":
                    demarche = input("📋 Numéro de démarche (ex: 121950): ").strip()
                    if demarche.isdigit():
                        self.sync_specific(demarche)
                    else:
                        print("❌ Numéro invalide")
                        
                elif choice == "4":
                    demarches = input("📋 Numéros de démarches séparés par des virgules (ex: 121950,122643): ").strip()
                    if demarches:
                        try:
                            # Valider les numéros
                            nums = [int(x.strip()) for x in demarches.split(",") if x.strip().isdigit()]
                            if nums:
                                self.sync_specific(nums)
                            else:
                                print("❌ Aucun numéro valide trouvé")
                        except ValueError:
                            print("❌ Format invalide")
                    else:
                        print("❌ Aucune démarche spécifiée")
                        
                elif choice == "5":
                    self.test_tokens()
                    
                elif choice == "6":
                    self.show_help()
                    
                else:
                    print("❌ Choix invalide")
                    
            except KeyboardInterrupt:
                print("\n👋 Au revoir !")
                break
            except EOFError:
                print("\n👋 Au revoir !")
                break
            
            input("\nAppuyez sur Entrée pour continuer...")
    
    def show_help(self):
        """Affiche l'aide"""
        print("\n" + "="*60)
        print("❓ AIDE - PROJET OTP")
        print("="*60)
        print("Ce script permet de lancer facilement le projet OTP multi-démarches.")
        print()
        print("📋 Options en ligne de commande:")
        print("  python run.py                    - Menu interactif")
        print("  python run.py --validate         - Valider la configuration")
        print("  python run.py --sync-all         - Synchroniser toutes les démarches")
        print("  python run.py --sync 121950      - Synchroniser la démarche 121950")
        print("  python run.py --sync 121950,122643 - Synchroniser plusieurs démarches")
        print("  python run.py --test             - Tester les tokens")
        print()
        print("📁 Fichiers du projet:")
        print(f"  Répertoire: {self.project_dir}")
        print(f"  Script principal: {self.manager_script.name}")
        print(f"  Configuration: config.json")
        print(f"  Variables d'environnement: .env")
        print()
        print("🔧 En cas de problème:")
        print("  1. Vérifiez que tous les fichiers sont présents")
        print("  2. Vérifiez votre fichier .env avec les tokens")
        print("  3. Lancez d'abord la validation (option 1)")
        print("  4. Utilisez --debug pour plus de détails")

def main():
    """Fonction principale"""
    launcher = OTPLauncher()
    
    # Parser les arguments en ligne de commande
    parser = argparse.ArgumentParser(
        description="Lanceur pour le projet OTP multi-démarches",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples d'utilisation:
  python run.py                    - Menu interactif
  python run.py --validate         - Valider la configuration
  python run.py --sync-all         - Synchroniser toutes les démarches
  python run.py --sync 121950      - Synchroniser la démarche 121950
  python run.py --sync 121950,122643 - Synchroniser plusieurs démarches
  python run.py --test             - Tester les tokens
        """
    )
    
    parser.add_argument("--validate", action="store_true", 
                       help="Valider la configuration")
    parser.add_argument("--sync-all", action="store_true", 
                       help="Synchroniser toutes les démarches")
    parser.add_argument("--sync", type=str, 
                       help="Synchroniser des démarches spécifiques (ex: 121950 ou 121950,122643)")
    parser.add_argument("--test", action="store_true", 
                       help="Tester les tokens")
    
    args = parser.parse_args()
    
    # Exécuter selon les arguments
    if args.validate:
        launcher.validate_config()
        
    elif args.sync_all:
        launcher.sync_all()
        
    elif args.sync:
        try:
            # Parser les numéros de démarches
            demarches = [int(x.strip()) for x in args.sync.split(",") if x.strip().isdigit()]
            if demarches:
                launcher.sync_specific(demarches)
            else:
                print("❌ Aucun numéro de démarche valide trouvé")
                sys.exit(1)
        except ValueError:
            print("❌ Format invalide pour les numéros de démarches")
            sys.exit(1)
            
    elif args.test:
        launcher.test_tokens()
        
    else:
        # Mode interactif par défaut
        launcher.interactive_menu()

if __name__ == "__main__":
    main()