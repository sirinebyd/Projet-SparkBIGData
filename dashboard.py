import os
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import matplotlib.patches as mpatches

CSV_LINKS = "metrics_output/current_links.csv"
CSV_CENTRALITY = "metrics_output/centrality.csv"

# Configuration d'une grande fenetre propre
fig, ax = plt.subplots(figsize=(14, 10))
fig.canvas.manager.set_window_title('LeBonCoin - Dashboard Big Data')


def update_dashboard(frame):
    if not os.path.exists(CSV_LINKS) or not os.path.exists(CSV_CENTRALITY):
        ax.clear()
        ax.text(0.5, 0.5, "En attente des calculs PySpark...", ha='center', va='center', fontsize=14, color='gray')
        ax.axis('off')
        return

    try:
        df_links = pd.read_csv(CSV_LINKS)
        df_centrality = pd.read_csv(CSV_CENTRALITY)

        if df_links.empty:
            return

        ax.clear()
        G = nx.DiGraph()

        # Palette de couleurs pastel distinctes
        color_map_dict = {'User': '#5DADE2', 'Seller': '#58D68D', 'Product': '#F39C12'}

        # On limite strictement aux 25 premieres lignes pour preserver la lisibilite
        df_links_sample = df_links.head(25)

        for _, row in df_links_sample.iterrows():
            src, dst, rel = str(row['src']), str(row['dst']), str(row['relationship'])
            src_type = 'User' if src.startswith('usr') else 'Seller'
            dst_type = 'Product'

            G.add_node(src, type=src_type)
            G.add_node(dst, type=dst_type)
            G.add_edge(src, dst, label=rel)

        node_sizes = []
        node_colors = []
        centrality_lookup = dict(zip(df_centrality['id'].astype(str), df_centrality['inDegree']))

        for node in G.nodes():
            n_type = G.nodes[node]['type']
            node_colors.append(color_map_dict.get(n_type, '#D5D8DC'))
            clicks = centrality_lookup.get(node, 0)
            # Equilibre des tailles des ronds
            node_sizes.append(500 + (clicks * 400))

        # Algorithme de repartition aere dans l'espace
        pos = nx.spring_layout(G, k=0.8, iterations=50, seed=42)

        # 1. Rendu des Noeuds avec une bordure blanche protectrice
        nx.draw_networkx_nodes(G, pos, node_color=node_colors, node_size=node_sizes,
                               edgecolors='white', linewidths=2, ax=ax)

        # 2. Rendu des Fleches droites (permet un alignement parfait du texte)
        nx.draw_networkx_edges(G, pos, edge_color='#BDC3C7', width=1.5,
                               arrowsize=18, ax=ax)

        # 3. Noms des identifiants (usr, sel, prod)
        nx.draw_networkx_labels(G, pos, font_size=8, font_weight='bold', font_family='sans-serif', ax=ax)

        # 4. Etiquettes d'actions avec boite blanche arrondie opaque pour couper la ligne grise
        edge_labels = nx.get_edge_attributes(G, 'label')
        nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_size=7,
                                     font_color='#2C3E50', font_weight='bold',
                                     bbox=dict(facecolor='white', alpha=0.9, edgecolor='#BDC3C7',
                                               boxstyle='round,pad=0.3'), ax=ax)

        # 5. Legende explicative stable en haut a droite
        legend_patches = [
            mpatches.Patch(color='#5DADE2', label='Utilisateur (User)'),
            mpatches.Patch(color='#58D68D', label='Vendeur (Seller)'),
            mpatches.Patch(color='#F39C12', label='Produit (Product)')
        ]
        ax.legend(handles=legend_patches, loc='upper right', frameon=True, facecolor='white', edgecolor='#BDC3C7',
                  fontsize=10)

        # Titre net sans emoji et masquage des axes de graphiques
        ax.set_title("Dashboard Live - Graphe Relationnel Temps Reel", fontsize=16, fontweight='bold', color='#2C3E50',
                     pad=15)
        ax.axis('off')

    except Exception as e:
        print(f"Synchro en cours... ({e})")


# Rafraichissement toutes les 3 secondes
ani = FuncAnimation(fig, update_dashboard, cache_frame_data=False, interval=3000)

# Definition stricte des marges pour empecher le rognage du titre
plt.subplots_adjust(top=0.92, bottom=0.03, left=0.03, right=0.97)
plt.show()