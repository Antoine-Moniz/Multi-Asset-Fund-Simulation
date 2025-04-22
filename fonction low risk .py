def lowrisk_strategy(current_date, portfolio, df):
    """
    Optimise dynamiquement un portefeuille selon une stratégie à faible risque.

    Cette fonction calcule l'allocation optimale des actifs financiers en maximisant les rendements attendus tout en maintenant une volatilité annuelle cible proche de 10 %. 
    L'allocation est obtenue par une optimisation basée sur un algorithme génétique (évolution différentielle). 
    La fonction génère ensuite automatiquement les ordres d'achat et de vente nécessaires pour atteindre cette nouvelle allocation, en tenant compte du portefeuille actuel.

    Paramètres
    ----------
    current_date :
        Date actuelle servant de référence pour calculer l'allocation optimale.
        
    portfolio : dict
        Portefeuille actuel sous la forme d'un dictionnaire indiquant les poids actuels des actifs. 
    df : pandas.DataFrame
        DataFrame contenant l'historique des rendements des actifs avec au minimum les colonnes suivantes :
        - 'Date' : date des rendements.
        - 'symbole' : symbole ou ticker de l'actif.
        - 'Returns' : rendement périodique de l'actif.

    Retourne
    --------
    new_portfolio_percent : dict
        Dictionnaire indiquant l'allocation optimale recommandée pour chaque actif en pourcentage.
        
    orders : list of dict
        Liste des ordres générés pour rééquilibrer le portefeuille selon la nouvelle allocation. Chaque ordre est un dictionnaire précisant la date, l'action ('buy' ou 'sell'), le symbole de l'actif et la quantité correspondante.

    Effets secondaires
    ------------------
    Insère automatiquement les ordres générés dans une base de données SQLite nommée "fund_database.db", dans les tables 'Deals' et 'Portfolio_Holdings'.

    """


    # Conversion de la date actuelle en datetime
    current_date = pd.to_datetime(current_date)
    # Réinitialise l'index et renomme la colonne en 'Date'
    df = df.reset_index()
    df.rename(columns={'index': 'Date'}, inplace=True)
    df['Date'] = pd.to_datetime(df['Date'])
    
     # Sélectionne les données historiques strictement antérieures à la date actuelle
    df = df[df['Date'] < current_date]
    
    # Transforme les données en tableau pivot (date, symbole, rendements)
    pivot_data = df.pivot_table(index='Date', columns='symbole', values='Returns')

    # Supprime les actifs n'ayant que des NaN et remplace les NaN restants par 0
    pivot_data = pivot_data.dropna(axis=1, how='all').fillna(0)
    # Calcule les rendements moyens historiques pour chaque actif
    avg_Returns = pivot_data.mean().values        
    # Calcule la matrice de covariance historique des rendements
    cov_matrix = pivot_data.cov().values       
    # Liste des symboles (actifs disponibles)     
    symboles = list(pivot_data.columns)

    num_assets = len(symboles)
    # Définit la volatilité cible annuelle du portefeuille à 10%
    target_vol = 0.10  

    def objective(x):
        # Normalisation des poids des actifs dans le portefeuille
        weights = np.array(x)
        weights = weights / np.sum(weights)
        
         # Calcule le rendement du portefeuille
        port_Returns = np.dot(weights, avg_Returns)
        # Calcule la volatilité annualisée du portefeuille
        port_vol = np.sqrt(252 * np.dot(weights, np.dot(cov_matrix, weights)))
         # Pénalisation si la volatilité du portefeuille s'écarte de la cible
        penalty = 1000 * abs(port_vol - target_vol)
         # Retourne l'opposé du rendement pénalisé (pour maximiser)
        return -(port_Returns - penalty)
    
    bounds = [(0, 1)] * num_assets   # Définition des bornes des poids (entre 0% et 100% par actif)

     # Lance l'optimisation par évolution différentielle (algorithme génétique)
    result = differential_evolution(
        objective,  # La fonction objectif à minimiser
        bounds, # Limites des valeurs possibles pour chaque variable (poids des actifs entre 0 et 1)
        strategy='best1bin', # Stratégie d'évolution différentielle utilisant le meilleur individu actuel ('best1bin')
        maxiter=10, # Nombre maximal d'itérations/générations
        popsize=10, # Taille de la population (nombre d'individus dans chaque génération)
        tol=1e-6, # Tolérance de convergence (critère d'arrêt basé sur l'amélioration minimale)
        mutation=(0.5, 1),  # Facteur de mutation (amplitude des perturbations appliquées aux solutions)
        recombination=0.7 # Probabilité de recombinaison (probabilité d'échanger des caractéristiques entre individus)

    )

     # Extraction de la solution optimale (poids optimaux des actifs)
    best_solution = result.x
    
    # Normalisation finale des poids
    best_weights_fraction = best_solution / np.sum(best_solution)

    # Création d'un dictionnaire d'allocation optimale en pourcentages
    new_portfolio_percent = {symbole: weight * 100 for symbole, weight in zip(symboles, best_weights_fraction)}
    
    print("Allocation optimale (en pourcent) :", new_portfolio_percent)

     # Connexion à la base de données SQLite
    conn = sqlite3.connect("fund_database.db")
    cursor = conn.cursor()
    
     # Préparation d'une liste d'ordres à exécuter
    orders = []
    seuil = 0.001  # Seuil minimal de rééquilibrage (0.1%)
    
    # Parcourt chaque actif pour générer les ordres nécessaires au rééquilibrage
    for symbole, target_weight_fraction in zip(symboles, best_weights_fraction):
         # Poids actuel dans le portefeuille (0 si inexistant)
        current_weight_fraction = portfolio.get(symbole, 0.0)
        # Calcul de la différence avec la cible
        diff = target_weight_fraction - current_weight_fraction
        if abs(diff) > seuil: # Si l'écart dépasse le seuil, génère un ordre
            action = "buy" if diff > 0 else "sell"
            quantity = abs(diff) * 100

             # Détail de l'ordre généré
            order = {
                "date": current_date,
                "id_portfolio": 1,
                "risk_profile": "Lowrisk",
                "action": action,
                "asset": symbole,
                "quantity": quantity
            }
            orders.append(order)
             
            # Insertion de l'ordre dans la table 'Deals' de la base de données
            date_str = current_date.strftime("%Y-%m-%d")
            cursor.execute("""
                INSERT INTO Deals (date, id_portfolio, risk_profile, action, asset, quantity)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (date_str, order['id_portfolio'], order['risk_profile'], order['action'], order['asset'], order['quantity']))
            
             # Insertion de l'allocation cible dans la table 'Portfolio_Holdings'
            target_weight_percent = target_weight_fraction * 100
            cursor.execute("""
                INSERT INTO Portfolio_Holdings (date, id_portfolio, ticker, weight)
                VALUES (?, ?, ?, ?)
            """, (date_str, order['id_portfolio'], symbole, target_weight_percent))

    # Enregistre les changements dans la base de données et ferme la connexion            
    conn.commit()
    conn.close()
    
    
    return new_portfolio_percent, orders