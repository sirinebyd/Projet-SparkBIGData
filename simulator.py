import os
import time
import json
import random
from datetime import datetime

# 1. Configuration du dossier de destination du flux (Exigence 2.1)
OUTPUT_DIR = "flux_streaming"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 2. Données de simulation conformes au cahier des charges (Exigence 2.1)
CITIES = ["Paris", "Lyon", "Marseille", "Lille", "Toulouse"]
CATEGORIES = ["Véhicules", "Immobilier", "Mode", "Électronique"]
ACTIONS = ["AIME", "VOUT", "ACHAT"]

print("[SIMULATEUR] Lancement de la génération du flux infini...")
print(f" Fichiers JSON enregistrés dans : {os.path.abspath(OUTPUT_DIR)}")
print("Ctrl+C pour arrêter le simulateur.\n")

counter = 0

try:
    while True:
        # Génération d'une interaction conforme à la structure exigée (Exigence 2.1)
        event = {
            "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),  # Format strict ISO 8601
            "user_id": f"usr_{random.randint(1000, 1050)}",  # Plage réduite pour forcer des interconnexions
            "user_city": random.choice(CITIES),
            "product_id": f"prod_{random.randint(5000, 5020)}",  # Plage réduite pour créer des nœuds centraux
            "product_cat": random.choice(CATEGORIES),
            "seller_id": f"sel_{random.randint(100, 110)}",  # Plage réduite pour lier fortement les vendeurs
            "action_type": random.choice(ACTIONS),
            "price": round(random.uniform(10.0, 1200.0), 2)
        }

        # Écriture dans un fichier JSON unique par événement (Simule le streaming par fichier pour Spark)
        file_path = os.path.join(OUTPUT_DIR, f"event_{int(time.time() * 1000)}_{counter}.json")

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(event, f, ensure_ascii=False)

        print(
            f"[EVENT #{counter}] {event['action_type']} -> {event['user_id']} sur {event['product_id']} ({event['price']}€)")

        counter += 1
        # Fréquence ajustable : 0.5 seconde pour avoir un flux dynamique sans saturer le disque
        time.sleep(0.5)

except KeyboardInterrupt:
    print("\n[SIMULATEUR] Arrêt du générateur de flux.")