import pandas as pd
import sqlite3

""" Nous avons un problème sur ce fichier, pourtant nous utilisons le même insert deals que dans 
la stratégie low turnover qui fonctionne... 
Ayant l'erreur database is locked, cela pourrait signifier qu'il y a un problème avec la connexion
c'est pour cela que j'ai ajouté (reprise d'internet) une fonctionne permettant de checker
si la connexion est ouverte ou non et en la printant on a bien qu'elle se ferme après apple 
mais qu'ensuite ça ne fonctionne pas pourtant s'il s'agissait d'un problème du code en lui même, cela
ne devrait pas aller jusqu'apple. J'ai également vérifié avec open cache.db aucun fichier cache n'est présent
donc ce code n'implémente pas dans SQL mais nous ne comprenons vraiement pas pourquoi"""

def chk_conn(conn):
     try:
        conn.cursor()
        return True
     except Exception as ex:
        return False

""" Pour récupérer la data nous faisons un Inner Join afin d'utiliser la condition Catégorie = Action
pour extraire les données dont nous avons besoin """

def load_data_equity_only(db_path):
    conn = sqlite3.connect(db_path)
    query = """
    SELECT r.*, p.category 
    FROM Returns r
    INNER JOIN Products p ON r.ticker = p.ticker
    WHERE p.category = 'Action'
    """
    df = pd.read_sql_query(query, conn, parse_dates=['date'], index_col='date')
    print(chk_conn(conn))
    conn.close()
    print(chk_conn(conn))
    df.rename(columns={'price': 'Close'}, inplace=True)
    df.sort_index(inplace=True)
    tickers = df['ticker'].unique()
    
    return df, tickers

data_equity_only, tickers = load_data_equity_only(db_path="fund_database.db")

"""Voici la fonction qui pose problème dans cette stratégie mais pas dans low turnover"""

def insert_deals(conn, date, action, asset, quantity, secteur):
    cursor = conn.cursor()
    date_str = date.strftime('%Y-%m-%d')

    id_portfolio = 3 
    risk_profile = "High Yield Only"
    
    cursor.execute(
            """ 
            INSERT INTO Deals (date, id_portfolio, risk_profile, action, asset, quantity, secteur)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (date_str, id_portfolio, risk_profile, action, asset, quantity, secteur)
        )
    print(f"Transaction insérée : {action} le {date_str} pour {asset} dans le secteur {secteur}")
    conn.commit()
    conn.close()


def strategy_equity_only(data_equity_only, tickers, db_path, date):
    date = pd.to_datetime(date, errors='coerce')
    data = data_equity_only[data_equity_only.index < date]

    """ Nous faisons une boucle sur les tickers en calculant la moyenne mobile 10j et 30j 
    si la moyenne mobile 10j est supérieure à la moyenne mobile 30j alors nous achetons
    dans le cas contraire, nous vendons """

    for tic in tickers:
        print(tic)
        conn = sqlite3.connect(db_path, timeout=10)
        
        print(chk_conn(conn))
        data_tic = data[data['ticker'] == tic].copy()
        data_tic['SMA_10'] = data_tic['Close'].rolling(window=10).mean()
        data_tic['SMA_30'] = data_tic['Close'].rolling(window=30).mean()

        last_sma_10 = data_tic['SMA_10'].iloc[-1]
        last_sma_30 = data_tic['SMA_30'].iloc[-1]
        action = 'buy' if last_sma_10 > last_sma_30 else 'sell'
        
        secteur = str(data_tic['secteur'].iloc[-1]) 
        quantity = 1

        insert_deals(conn, date, action, tic, quantity, secteur)

        print(chk_conn(conn))


date_test = pd.to_datetime('2023-01-09')
test = strategy_equity_only(data_equity_only, tickers, "fund_database.db", date_test)
