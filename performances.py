from datetime import datetime, timedelta
import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

#%% Performance Low Turnover

def performance():
    # Connexion à la base de données SQLite
    conn = sqlite3.connect("fund_database.db")
    cursor = conn.cursor()

    # Requête pour récupérer le nombre total de transactions et le volume total par profil de risque
    query_transactions = """
    SELECT risk_profile, COUNT(*) AS nb_transactions, SUM(quantity) AS volume_total
    FROM Deals
    GROUP BY risk_profile;
    """
    df_transactions = pd.read_sql(query_transactions, conn)

    # Requête pour récupérer les pondérations des actifs dans le portefeuille et leurs rendements
    query_returns = """
    SELECT ph.id_portfolio, ph.ticker, ph.weight, r.date, r.return
    FROM Portfolio_Holdings ph
    JOIN Returns r ON ph.ticker = r.ticker;
    """
    df_returns = pd.read_sql(query_returns, conn)
    
    # Calcul du rendement pondéré par portefeuille
    df_returns["weighted_return"] = df_returns["weight"] * df_returns["return"]
    portfolio_performance = df_returns.groupby(["id_portfolio", "date"])['weighted_return'].sum().reset_index()
    
    # Calcul des statistiques annuelles
    rendement_annuel = portfolio_performance.groupby("id_portfolio")["weighted_return"].mean() * 252  # Rendement annualisé
    vol_annuelle = portfolio_performance.groupby("id_portfolio")["weighted_return"].std() * np.sqrt(252)  # Volatilité annualisée
    rf = 0.02  # Taux sans risque supposé
    ratio_sharpe = (rendement_annuel - rf) / vol_annuelle  # Calcul du ratio de Sharpe
    
    # Création d'un DataFrame pour stocker les métriques de performance
    df_metrics = pd.DataFrame({
        "Rendement Annuel (%)": rendement_annuel * 100,
        "Volatilité Annuelle (%)": vol_annuelle * 100,
        "Ratio de Sharpe": ratio_sharpe
    }).round(2)
    
    # Requête pour récupérer la répartition des transactions par secteur et profil de risque
    query_products = """
    SELECT 
        d.risk_profile, 
        p.secteur AS secteur,
        COUNT(*) AS nb_transactions,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY d.risk_profile), 2) AS proportion
    FROM Deals d
    JOIN Products p ON d.asset = p.ticker
    GROUP BY d.risk_profile, p.secteur
    ORDER BY d.risk_profile, proportion DESC;
    """
    df_products = pd.read_sql(query_products, conn)
    
    # Fermeture de la connexion à la base de données
    conn.close()
    
    # Affichage des résultats
    print("Nombre de transactions par profil de risque :")
    print(df_transactions)
    print("\nIndicateurs de performance :")
    print(df_metrics)
    print("\nRépartition des transactions par produits et profil de risque :")
    print(df_products)
    
    # Visualisation des résultats sous forme de graphique à barres
    plt.figure(figsize=(10, 5))
    sns.barplot(data=df_transactions, x="risk_profile", y="nb_transactions")
    plt.title("Nombre de transactions par profil de risque")
    plt.xlabel("Profil de risque")
    plt.ylabel("Nombre de transactions")
    plt.show()
    
    return df_transactions, df_metrics, df_products