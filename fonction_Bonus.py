import numpy as np
import pandas as pd
import pygad  
from datetime import datetime, timedelta
import pandas as pd
from scipy.optimize import differential_evolution
import sqlite3

def strategy_high_yield_equity_optimization(current_date, portfolio, df):
    """
    Cette fonction réalise une optimisation de portefeuille axée sur les hauts rendements ("High Yield Equity")
    à l'aide d'un algorithme génétique (GA). L'objectif principal est de maximiser le rendement espéré du portefeuille,
    calculé sur la base des rendements moyens des 90 derniers jours pour chaque action présente dans le jeu de données.

    Étapes de la fonction :
    1. Prépare et filtre les données pour ne considérer que les observations antérieures à la date actuelle et les actions uniquement.
    2. Calcule les rendements espérés de chaque action en moyenne sur une fenêtre glissante de 90 jours.
    3. Utilise un algorithme génétique pour déterminer les poids optimaux à attribuer à chaque action afin de maximiser le rendement espéré.
    4. Génère des ordres d'achat ou de vente selon la différence entre l'allocation actuelle et l'allocation optimale déterminée par le GA.
    5. Insère les transactions générées ainsi que les nouvelles positions du portefeuille dans une base de données SQLite.

    Paramètres :
        current_date (datetime) : Date à laquelle l'optimisation est effectuée.
        portfolio (dict) : Dictionnaire contenant les poids actuels des actifs du portefeuille.
        df (DataFrame) : Jeu de données contenant l'historique des rendements et informations sur les actifs.

    Retourne :
        new_portfolio (dict) : Dictionnaire des allocations optimales pour chaque actif déterminées par l'algorithme génétique.
        orders (list) : Liste de dictionnaires représentant les ordres générés pour ajuster le portefeuille vers l'allocation optimale.
    """

    # Réinitialiser l'index pour manipuler la colonne Date facilement
    df = df.reset_index()
    
    # Renommer la colonne d'index en 'Date' et conversion en datetime
    df.rename(columns={'index': 'Date'}, inplace=True)
    df['Date'] = pd.to_datetime(df['Date'])

    # Filtrer les données avant la date actuelle
    df = df[df['Date'] < current_date]

    # Définition des noms de colonnes importantes
    category_col = 'Category'
    symbole_col = "symbole"
    returns_col = "Returns"

    # Sélectionner uniquement les données de type actions
    equities_data = df[df[category_col] == "Action"]

    symboles = equities_data[symbole_col].unique() # Obtenir les symboles d'actions uniques

    # Calculer les rendements espérés des 90 derniers jours pour chaque action
    expected_returns = []
    start_window = current_date - timedelta(days=90)
    for s in symboles:
        symbole_data = equities_data[equities_data[symbole_col] == s]
        symbole_window = symbole_data[symbole_data['Date'] >= start_window]
        if not symbole_window.empty:
            exp_ret = symbole_window[returns_col].mean()
        else:
            exp_ret = 0.0
        expected_returns.append(exp_ret)
    expected_returns = np.array(expected_returns)

    # Nombre d'actifs à optimiser
    num_genes = len(symboles)

    # Fonction fitness pour l'algorithme génétique (maximisation du rendement)
    def fitness_func(ga_instance, solution, solution_idx):
        # Éviter division par zéro si solution est toute à zéro
        if np.sum(solution) == 0:
            weights = np.ones_like(solution) / len(solution)
        else:
            weights = solution / np.sum(solution)
        portfolio_return = np.sum(weights * expected_returns)
        return portfolio_return

    # Configuration et exécution de l'algorithme génétique
    ga_instance = pygad.GA(num_generations=50,
                           num_parents_mating=5,
                           fitness_func=fitness_func,
                           sol_per_pop=20,
                           num_genes=num_genes,
                           gene_space=[{'low': 0, 'high': 1}] * num_genes,
                           mutation_percent_genes=10,
                           stop_criteria=["reach_0.001"])
    ga_instance.run()

    # Récupération de la meilleure solution (allocation optimale)
    solution, solution_fitness, solution_idx = ga_instance.best_solution()

    if np.sum(solution) == 0:
        optimal_weights = np.ones_like(solution) / len(solution)
    else:
        optimal_weights = solution / np.sum(solution)

     # Créer un dictionnaire d'allocation optimale
    new_portfolio = dict(zip(symboles, optimal_weights))
    print("Allocation optimale (en fraction du total) :", new_portfolio)

    orders = []
    
    # Connexion à la base de données SQLite
    conn = sqlite3.connect("fund_database.db")
    cursor = conn.cursor()

    current_date_str = current_date.strftime("%Y-%m-%d")

    # Générer les ordres d'achat/vente en fonction des poids cibles
    for i, s in enumerate(symboles):
        target_amount = optimal_weights[i] * 100
        current_amount = portfolio.get(s, 0)
        diff = target_amount - current_amount
        if abs(diff) > 1e-6: # Ignorer les différences insignifiantes
            order = {
                'date': current_date_str,
                'id_portfolio': 3,  
                'risk_profile': 'High Yield Equity Only',
                'action': 'buy' if diff > 0 else 'sell',
                'asset': s,
                'quantity': abs(diff)
            }
            orders.append(order)

            # Insertion des ordres dans la base de données
            
            cursor.execute("""
                INSERT INTO Deals (date, id_portfolio, risk_profile, action, asset, quantity)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (order['date'], order['id_portfolio'], order['risk_profile'], order['action'], order['asset'], order['quantity']))
            
            # Mise à jour des positions du portefeuille

            cursor.execute("""
                INSERT INTO Portfolio_Holdings (date, id_portfolio, ticker, weight)
                VALUES (?, ?, ?, ?)
            """, (current_date_str, 3, s, target_amount))

    # Sauvegarde des modifications dans la base et fermeture de la connexion
    conn.commit()
    conn.close()

    print("Ordres générés :")
    for order in orders:
        print(order)

    return new_portfolio, orders

import numpy as np
import pandas as pd
import sqlite3
import random
from datetime import datetime
from deap import base, creator, tools, algorithms

def lowturnover_strategy(current_date, portfolio, df):
    """
    Implémente une stratégie d'optimisation de portefeuille à faible rotation (low turnover) en utilisant un algorithme génétique.

    Paramètres:
    - current_date (str ou datetime) : Date courante pour déterminer la période d'analyse.
    - portfolio (dict) : Dictionnaire contenant les actifs et leurs poids actuels.
    - df (DataFrame) : DataFrame contenant les rendements historiques des actifs avec les colonnes 'Date', 'symbole', et 'Returns'.

    Fonctionnement:
    - Sélectionne deux actifs du portefeuille et optimise leur allocation à l'aide d'un algorithme génétique.
    - Cherche à maximiser le rendement attendu moyen du portefeuille tout en minimisant la rotation (faibles changements d'allocation).
    - Génère des ordres d'achat/vente pour refléter les nouvelles pondérations optimisées.

    Sortie:
    - Retourne les nouvelles pondérations du portefeuille (en %) et les ordres d'achat/vente générés.
    """
    
    current_date = pd.to_datetime(current_date)
    
    # Préparation des données
    df = df.reset_index()
    df.rename(columns={'index': 'Date'}, inplace=True)
    df['Date'] = pd.to_datetime(df['Date'])
    df = df[df['Date'] < current_date]
    
    pivot_data = df.pivot_table(index='Date', columns='symbole', values='Returns')
    pivot_data = pivot_data.dropna(axis=1, how='all').fillna(0)
    tickers = list(pivot_data.columns)
    num_assets = len(tickers)
    
    # S'assurer que tous les tickers du pivot existent dans le portefeuille
    for t in tickers:
        if t not in portfolio:
            portfolio[t] = 0.0

    # Calculer le rendement moyen pour chaque actif
    avg_returns = {t: pivot_data[t].mean() for t in tickers}
    
    def evalIndividual(individual):
        i = int(individual[0]) % num_assets
        j = int(individual[1]) % num_assets
        
        if i == j:
            return (-1e12,) 
        
        x = individual[2]
        ticker_i = tickers[i]
        ticker_j = tickers[j]
        p = portfolio[ticker_i] + portfolio[ticker_j]
        new_weight_i = x * p
        new_weight_j = (1 - x) * p
        
        total_return = 0.0
        for t in tickers:
            if t == ticker_i:
                total_return += new_weight_i * avg_returns[t]
            elif t == ticker_j:
                total_return += new_weight_j * avg_returns[t]
            else:
                total_return += portfolio[t] * avg_returns[t]
        
        return (total_return,)
    
    # Création des classes pour DEAP
    if not hasattr(creator, "FitnessMax"):
        creator.create("FitnessMax", base.Fitness, weights=(1.0,))
    if not hasattr(creator, "Individual"):
        creator.create("Individual", list, fitness=creator.FitnessMax)
    
    # Configuration de la toolbox DEAP
    toolbox = base.Toolbox()
    toolbox.register("attr_index", lambda: random.uniform(0, num_assets))
    toolbox.register("attr_x", random.random)
    toolbox.register("individual", tools.initCycle, creator.Individual,
                     (toolbox.attr_index, toolbox.attr_index, toolbox.attr_x), n=1)
    toolbox.register("population", tools.initRepeat, list, toolbox.individual)
    toolbox.register("evaluate", evalIndividual)
    toolbox.register("mate", tools.cxBlend, alpha=0.5)
    toolbox.register("mutate", tools.mutGaussian, mu=0, sigma=0.2, indpb=0.5)
    toolbox.register("select", tools.selTournament, tournsize=3)
    
    population = toolbox.population(n=50)
    NGEN = 100
    for gen in range(NGEN):
        offspring = algorithms.varAnd(population, toolbox, cxpb=0.5, mutpb=0.3)
        fits = list(map(toolbox.evaluate, offspring))
        for ind, fit in zip(offspring, fits):
            ind.fitness.values = fit
        population = toolbox.select(offspring, k=len(population))
    
    best_ind = tools.selBest(population, k=1)[0]
    best_i = int(best_ind[0]) % num_assets
    best_j = int(best_ind[1]) % num_assets
    best_x = max(0, min(best_ind[2], 1))
    
    ticker_i = tickers[best_i]
    ticker_j = tickers[best_j]
    p = portfolio[ticker_i] + portfolio[ticker_j]
    new_weight_i = best_x * p
    new_weight_j = (1 - best_x) * p
    
    # Mise à jour du portefeuille pour les deux actifs modifiés
    new_portfolio = portfolio.copy()
    new_portfolio[ticker_i] = new_weight_i
    new_portfolio[ticker_j] = new_weight_j

    # Normalisation pour obtenir des poids en % dont la somme est égale à 100%
    total_weight_new = sum(new_portfolio.values())
    new_portfolio_percent = {t: (weight / total_weight_new) * 100 for t, weight in new_portfolio.items()}
    
    # Normalisation du portefeuille initial (pour comparaison)
    total_weight_old = sum(portfolio.values())
    old_portfolio_percent = {t: (portfolio[t] / total_weight_old) * 100 for t in portfolio}
    
    print("Répartition initiale (en %):")
    for t, perc in old_portfolio_percent.items():
        print("  {} : {:.2f}%".format(t, perc))
    print("\nNouvelle répartition (en %):")
    for t, perc in new_portfolio_percent.items():
        print("  {} : {:.2f}%".format(t, perc))
    
    # Calcul des ordres basés sur la différence entre le portefeuille initial et le nouveau, en pourcentage
    orders = []
    conn = sqlite3.connect("fund_database.db")
    cursor = conn.cursor()
    date_str = current_date.strftime("%Y-%m-%d")
    
    threshold = 1e-4  
    for ticker in [ticker_i, ticker_j]:
        diff = new_portfolio_percent[ticker] - old_portfolio_percent.get(ticker, 0)
        if abs(diff) > threshold:
            order = {
                "date": current_date,
                "id_portfolio": 2,
                "risk_profile": "Lowturnover",
                "action": "buy" if diff > 0 else "sell",
                "asset": ticker,
                "quantity": abs(diff)  # La quantité est en % à acheter ou vendre
            }
            orders.append(order)
            cursor.execute("""
                INSERT INTO Deals (date, id_portfolio, risk_profile, action, asset, quantity)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (date_str, order['id_portfolio'], order['risk_profile'], order['action'], order['asset'], order['quantity']))
    
    for t, weight_percent in new_portfolio_percent.items():
        cursor.execute("""
            INSERT INTO Portfolio_Holdings (date, id_portfolio, ticker, weight)
            VALUES (?, ?, ?, ?)
        """, (date_str, 2, t, weight_percent))
    
    conn.commit()
    conn.close()
    
    print("\nOrdres générés pour les actifs modifiés :")
    for order in orders:
        print(order)
    
    return new_portfolio_percent, orders
