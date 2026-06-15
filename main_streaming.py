import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col, lit
from graphframes import GraphFrame

# 1. INITIALISATION OPTIMISEE
# Configuration locale avec optimisation du shuffle pour eviter la surconsommation memoire
spark = SparkSession.builder \
    .appName("EngineLeBonCoin") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

# Nettoyage des logs pour ne pas polluer la console avec les avertissements JVM
spark.sparkContext.setLogLevel("ERROR")

# Dossiers d'echange de donnees
INPUT_DIR = "flux_streaming"
OUTPUT_METRICS = "metrics_output"
os.makedirs(OUTPUT_METRICS, exist_ok=True)

# 2. SCHEMA ENFORCEMENT
# Definition stricte pour eviter l'inference de schema couteuse sur le flux infini
schema_interaction = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("user_id", StringType(), True),
    StructField("user_city", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_cat", StringType(), True),
    StructField("seller_id", StringType(), True),
    StructField("action_type", StringType(), True),
    StructField("price", DoubleType(), True)
])

# 3. LECTURE DU FLUX CONTINU
df_stream = spark.readStream \
    .schema(schema_interaction) \
    .json(INPUT_DIR)

# 4. GESTION DU TEMPS : WATERMARKING
# Application d'un filigranage de 10 minutes pour gerer les donnees tardives
df_watermarked = df_stream.withWatermark("timestamp", "10 minutes")


# 5. TRAITEMENT DU GRAPHE PAR MICRO-BATCHES (Exigence GraphFrames & Algorithmes)
def process_graph_batch(batch_df, batch_id):
    try:
        # On verifie si le batch contient des lignes sans utiliser .count() qui bloque le flux
        if batch_df.head(1):
            # --- A. CREATION DES ARETES (Edges) ---
            # Relation 1 : L'utilisateur (src) effectue une action vers le produit (dst)
            edges_users = batch_df.select(
                col("user_id").alias("src"),
                col("product_id").alias("dst"),
                col("action_type").alias("relationship")
            )

            # Relation 2 : Le vendeur (src) propose le produit (dst)
            edges_sellers = batch_df.select(
                col("seller_id").alias("src"),
                col("product_id").alias("dst")
            ).withColumn("relationship", lit("PROPOSE"))

            # Union globale des liaisons et suppression des doublons du batch
            edges = edges_users.union(edges_sellers).distinct()

            # --- B. CREATION DES SOMMETS (Vertices) ---
            # Extraction et typage de chaque entite pour la coloration du Dashboard
            users = batch_df.select(col("user_id").alias("id")).withColumn("type", lit("User"))
            sellers = batch_df.select(col("seller_id").alias("id")).withColumn("type", lit("Seller"))
            products = batch_df.select(col("product_id").alias("id")).withColumn("type", lit("Product"))

            vertices = users.union(sellers).union(products).distinct()

            # --- C. INSTANCIATION DU GRAPH FRAME ---
            g = GraphFrame(vertices, edges)

            # --- D. CALCUL ALGORITHMIQUE ---
            # Calcul de la centralite de degre entrant (In-Degrees) pour mesurer la popularite des produits
            centrality_df = g.inDegrees

            # --- E. EXPORTATION VERS LE DASHBOARD ---
            # Ecriture au format CSV (via Pandas pour ecraser proprement le fichier a chaque batch)
            edges.toPandas().to_csv(os.path.join(OUTPUT_METRICS, "current_links.csv"), index=False)
            centrality_df.toPandas().to_csv(os.path.join(OUTPUT_METRICS, "centrality.csv"), index=False)

            print(f"[SPARK] Micro-batch {batch_id} traite. {vertices.count()} noeuds analyses.")

    except Exception as e:
        print(f"[ERREUR BATCH {batch_id}] : {str(e)}")


# 6. DEMARRAGE DU QUERY STREAMING
# Le mode 'update' est requis pour pousser uniquement les changements au foreachBatch
query = df_watermarked.writeStream \
    .outputMode("update") \
    .foreachBatch(process_graph_batch) \
    .start()

print("Moteur PySpark et GraphFrames pret et en attente de flux...")
query.awaitTermination()