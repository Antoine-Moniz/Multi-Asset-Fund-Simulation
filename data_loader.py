# %% Packages 
import yfinance as yf
import pandas as pd
import numpy as np

# %% Fonction pour les tickers de yfinance 
ticker = [
    # Plus grandes capitalisations boursières
    'AAPL', 'MSFT', 'NVDA', 'AMZN', 'GOOGL', 'META', 'TSLA', 'BRK-B', 'LLY', 'WMT',
    'JPM', 'V', 'MA', 'UNH', 'HD', 'PG', 'XOM', 'CVX', 'PEP', 'KO',
    'ABBV', 'MRK', 'AVGO', 'COST', 'MCD', 'CSCO', 'ACN', 'ABT', 'DHR', 'NKE',
    'ORCL', 'DIS', 'TXN', 'PM', 'VZ', 'ADBE', 'NFLX', 'CRM', 'INTC', 'AMD',
    'NEE', 'LIN', 'TMUS', 'UPS', 'HON', 'UNP', 'SBUX', 'AMGN', 'MDT',
    'ASML', 'TM', 'SSNLF', 'NSRGY', 'RHHBY', 'LVMUY', 'NVO', 'TSM', 'UL', 'BHP',
    'HSBC', 'BP', 'TTE', 'SIEGY', 'SNY', 'BTI', 'GSK', 'PNGAY', 'BABA',
    'TCEHY', 'IDCBY', 'CICHY', 'FMX', 'ITUB', 'VALE', 'RY', 'TD', 'BNS', 'CM',
    'MUFG', 'SMFG', 'BBVA', 'SAN', 'DB', 'ING', 'UBS', 'GS', 'MS',
    'BAC', 'C', 'WFC', 'BLK', 'TFC', 'SCHW', 'BX', 'STT', 'NTR', 'APD',

    # Autres indices 
    '^GSPC', '^NDX', '^DJI', 

    # Valeurs refuge 
    'CL=F', 'GC=F',

    # Bon du trésor américain 
    '^FVX', 

    # ETF
    'SPY', 'QQQ', 'DIA', 'IWM', 'EFA', 'EEM', 'TLT', 'GLD', 'XLV', 'XLE'
]

def get_financial_data():

    """
    Télécharge, nettoie et prépare des données financières pour une sélection d'actifs.

    Cette fonction récupère des données historiques quotidiennes pour une liste prédéfinie de tickers provenant de Yahoo Finance via la bibliothèque yfinance. Elle traite ensuite ces données pour en faciliter l'analyse, incluant les opérations suivantes :

    - Téléchargement des données historiques 
    - Calcul des rendements quotidiens à partir du prix de clôture.
    - Classification des actifs en catégories (« Action » ou « ETF »).
    - Extraction automatique du secteur économique pour chaque actif lorsque disponible.
    - Traitement des valeurs aberrantes (méthode Z-score), avec remplacement par la médiane des rendements.
    - Gestion appropriée des valeurs manquantes.

    Période couverte : 1er janvier 2022 au 31 décembre 2024.

    Colonnes du DataFrame retourné :
    - 'Close' : Prix de clôture quotidien.
    - 'Volume' : Volume quotidien échangé.
    - 'ticker' : Ticker ou symbole financier.
    - 'Category' : Catégorie de l'actif (Action ou ETF).
    - 'Returns' : Rendement quotidien de l'actif (variation en % par rapport au jour précédent).
    - 'Secteur' : Secteur économique auquel appartient l'actif (si disponible, sinon "Non disponible").

    Retourne
    --------
    pandas.DataFrame
        DataFrame consolidé contenant l'ensemble des données traitées et prêtes pour analyse.

    ----
    ValueError
        Si aucune donnée n’a pu être correctement téléchargée et traitée.

    """
     # Définition de la période d'extraction des données financières historiques
    start_date = '2022-01-01'
    end_date = '2024-12-31'

    # Téléchargement des données historiques (cours et volumes) via Yahoo Finance pour tous les tickers spécifiés
    data = yf.download(ticker, start=start_date, end=end_date, group_by='ticker')
    # Remplit les données manquantes par propagation vers l'avant (forward-fill), afin de gérer les éventuelles absences de cotations certains jours 
    data = data.ffill() 

    dataframes = [] # Initialisation d'une liste vide pour regrouper les DataFrames individuels par ticker

    # Boucle sur chaque ticker individuel pour traiter et enrichir ses données séparément
    for t in ticker:
        try:
            # Copie les données spécifiques à un ticker pour éviter toute modification des données d'origine
            df = data[t].copy()
            # Ajoute une colonne indiquant clairement le ticker correspondant
            df['ticker'] = t  
            # Définit la catégorie : 'ETF' pour les tickers explicitement définis comme ETF, sinon 'Action'
            df['Category'] = (
                'Bon du trésor américain' if t == '^FVX' else
                'Valeurs refuge' if t in ['CL=F', 'GC=F'] else
                'ETF' if t in ['SPY', 'QQQ', 'DIA', 'IWM', 'EFA', 'EEM', 'TLT', 'GLD', 'XLV', 'XLE'] else
                'Action'
            )

            df['Returns'] = df['Close'].fillna(method='pad').pct_change()
            # Récupération du secteur d'activité du ticker via yfinance
            try:
                info = yf.Ticker(t).info
                secteur = info.get("sector", "Non disponible")   # Obtention du secteur, sinon valeur par défaut "Non disponible"
            except Exception:
                secteur = "Non disponible" # Gestion du cas où l'info secteur n'est pas disponible (actifs internationaux, indices, etc.)
            df['Secteur'] = secteur # Ajoute le secteur dans les données

            # Traitement des valeurs aberrantes 
            z_score = np.abs((df['Returns'] - df['Returns'].mean()) / df['Returns'].std())
            df.loc[z_score > 3, 'Returns'] = np.nan
            df['Returns'] = df['Returns'].fillna(df['Returns'].median())
            df = df[['Close', 'Volume', 'ticker', 'Category', 'Returns', 'Secteur']]
            dataframes.append(df)
        except Exception as e:
            print(f"Erreur lors du traitement de {t}: {e}")

    if not dataframes:  # Vérification si dataframes est vide
        raise ValueError("Aucune donnée n'a pu être traitée")

    return pd.concat(dataframes)
