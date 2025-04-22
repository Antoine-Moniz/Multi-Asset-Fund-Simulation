import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

"""
Notre stratégie Low TurnOver consiste à déterminer si l'on investit, achat ou vente, 
si l'investissement est suffisamment intéressant afin de ne pas faire 2 deals la première
semaine de chaque mois puis plus aucun si cela n'est pas optimal. Cependant, nous ne 
pouvons pas connaître les rendements futurs afin de déterminer quels investissements faire
dans le mois. Ainsi, nous avons construit un score qui dépend de la distance à la moyenne mobile
30 jours (en %) soit (Close - SMA_30) / SMA_30 et de la volatilité. 
En effet, puisque nous ne pouvons pas investir beaucoup dans le mois, nous avons pensé que nous
devions privilégié les actifs avec un bon rendement par rapport à leur moyenne mobile mais n'ayant pas 
de volatilité. Si un actif a une forte volatilité, cela implique un fort risque et nous souhaitons 
minimiser ce risque au vu de la faible capacité de changement de positions. 
"""



class Strategie_2_Low_Turnover:
    
    def __init__(self, db_path = "fund_database.db"):
        self.db_path = db_path
        self.data = None
        self.tickers = None
        self.trading_days = None
        self.deals = []
        self.turnover_month = 0
        self.ranked_scores = None
        self.best_scores_prev_month = None
        self.date_t = None
        self.date_fin = None
        self.last_date_used = None

    def load_data(self):
        """
        load_data permet de récupérer la donnée de la base SQL qui se trouve dans la table Returns
        """
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query("SELECT * FROM Returns", conn, parse_dates=['date'], index_col='date')
        conn.close()
        """
        Nous renommons la colonne price de SQL car depuis la donnée originale nous avions Close, cela
        permet d'éviter des confusions
        """
        df.rename(columns={'price': 'Close'}, inplace=True)
        df.sort_index(inplace=True)
        self.data = df
        self.tickers = self.data['ticker'].unique()
        """
        Nous allons récupérer les jours de trading afin de tester si la date se trouve dans nos données
        ou s'il s'agit d'un jour férié par exemple et donc qu'il n'y ait pas de trading ce jour là
        """
        self.trading_days = self.data[self.data['ticker'] == self.tickers[0]].index
        self.date_t = self.trading_days[self.trading_days == '2023-01-09'][0]
        self.date_fin = self.trading_days[-1]
        """
        Nous allons également définir last_date_used qui sert à garder la dernière utilisée, que l'on 
        utilisera pour déterminer le seuil à comparer pour investir ou non
        """
        self.last_date_used = self.date_t

    def prepare_previous_month_scores(self):
        """
        Il s'agit ici d'une initialisation, comme nous allons comparer pour chaque mois
        à la moyenne des trois meilleurs scores de la dernière date (du mois précédent) utilisée
        nous avons besoin d'initialiser les scores afin de pouvoir comparer avec la première date
        """

        t_dec22 = self.trading_days[self.trading_days == '2022-12-19'][0]
        scores22 = pd.DataFrame(index=[t_dec22], columns=self.tickers)
        data_dec22 = self.data[self.data.index < t_dec22]
        for tic in self.tickers:
            tic_data = data_dec22[data_dec22['ticker'] == tic]
            scores22.loc[t_dec22, tic] = self.generate_score(tic_data)
        scores_only22 = scores22.applymap(lambda x: x[0] if isinstance(x, tuple) else x)

        """
        On classe les scores et on garde les 3 meilleurs en calculant leur moyenne 
        """
        ranked_scores_dec22 = (
            scores_only22.loc[t_dec22]
            .reset_index()
            .rename(columns={'index': 'ticker', t_dec22: 'Score'})
        )
        ranked_scores_dec22['Score'] = pd.to_numeric(ranked_scores_dec22['Score'], errors='coerce')
        ranked_scores_dec22 = ranked_scores_dec22.sort_values(by='Score', ascending=False).dropna(subset=['Score'])
        self.best_scores_prev_month = ranked_scores_dec22['Score'].head(3).mean()

    def generate_score(self, data):
        """
        Voici la fonction qui calcule les scores, on commence par copier la data afin d'éviter un FutureWarning
        """
        df = data.copy()
        """ On calcule la moyenne mobile 30 jours"""
        df['SMA_30'] = df['Close'].rolling(window=30).mean()
        """ On calcule la distance comme indiqué au début de la classe"""
        distance_close_sma = (df['Close'] - df['SMA_30']) / df['SMA_30']
        df['vol'] = df['Close'].rolling(window=252).std()
        average_daily_volatility = df['vol'].mean()
        dist_temp = distance_close_sma.dropna()
        if dist_temp.empty:
            distance_sma = np.nan
        else:
            """ On garde uniquement la dernière distance """
            distance_sma = dist_temp.iloc[-1]
        """ 
        On met la distance à la moyenne mobile en valeur absolue puisque nous avons un - pour la volatilité
        si nous avions laisser le signe de distance_sma nous ne pourrions pas déterminer si le score est mauvais 
        à cause de la volatilité ou que la distance_sma est très négative. Nous voulons également les classer par ordre
        décroissant et avons ainsi de besoin de mettre la valeur absolue.
        Cependant, le signe est nécessaire pour déterminer si on veut acheter ou vendre donc nous le gardons via la 
        direction de la distance_sma
        Au score, on enlève la moitié de la volatilité puisqu'en enlevant la totalité de la volatilité, on pénalisait 
        trop les actifs.
        """
        score = abs(distance_sma) - 0.5 * average_daily_volatility
        direction = 1 if (not np.isnan(distance_sma) and distance_sma > 0) else -1
        return score, direction

    def insert_deal(self, date, id_portfolio, risk_profile, action, asset, quantity, secteur):
        """
        Insert deals comme son nom l'indique permet d'insérer les deals dans la table SQL 
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        existing_quantity = cursor.fetchone()[0]  
        new_quantity = quantity if existing_quantity is None else existing_quantity + quantity

        cursor.execute(
            """ 
            INSERT INTO Deals (date, id_portfolio, risk_profile, action, asset, quantity, secteur)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (date, id_portfolio, risk_profile, action, asset, new_quantity, secteur)
        )

        conn.commit()
        conn.close()

    def strategy_low_turnover(self, ranked_scores, date_str):
        trades = []
        if self.turnover_month >= 2:
            """ Si le turnover max du mois est dépassé, on s'arrête"""
            return trades
        """ On garde seulement les deux meilleurs performances en fonction du classement du score
        puisque nous ne pouvons faire maximum 2 deals, ca ne sert à rien d'en prendre davantage"""
        best_performer = ranked_scores.iloc[0]
        best_2_performer = ranked_scores.iloc[1] if len(ranked_scores) > 1 else None
        """ Si le score obtenu est meilleur que la moyenne des trois meilleurs du mois précédent alors
        on fait un deal et on l'insère dans la base SQL """
        if best_performer['Score'] > self.best_scores_prev_month and self.turnover_month < 2:
            direction = best_performer['Direction']
            self.turnover_month += 1
            action = 'buy' if direction > 0 else 'sell'
            trade = f"{action} {best_performer['ticker']} on {date_str}"
            trades.append(trade)
            secteur = self.data[self.data['ticker'] == best_performer['ticker']]['secteur'].iloc[-1]
            self.insert_deal(date_str, 2, "Low Turnover", action, best_performer['ticker'], 1, secteur)
            print(trade)
            """ Si le meilleur score ne passe pas la contrainte, le second non plus donc on le mets dans la 
            condition du premier, on vérifie également qu'avec le premier deal, on ne dépasse pas le maximum de deals  """
            if best_2_performer is not None and best_2_performer['Score'] > self.best_scores_prev_month and self.turnover_month < 2:
                direction2 = best_2_performer['Direction']
                self.turnover_month += 1
                action2 = 'buy' if direction2 > 0 else 'sell'
                trade2 = f"{action2} {best_2_performer['ticker']} on {date_str}"
                trades.append(trade2)
                secteur2 = self.data[self.data['ticker'] == best_2_performer['ticker']]['secteur'].iloc[-1]
                self.insert_deal(date_str, 2, "Low Turnover", action2, best_2_performer['ticker'], 1, secteur2)
                print(trade2)
        return trades

    def run_strategy(self):
        scores = pd.DataFrame(index=self.trading_days, columns=self.tickers)
        while self.date_t <= self.date_fin:
            """ Si le mois est différent de la date précédemment utilisée pour un deal, alors on remet 
            le compteur du turnover à 0 et on calcule la moyenne des trois meilleurs scores. Nous prenons 
            les 3 meilleurs car en prenant les 5 meilleurs, la condition était trop facilement vérifiée """
            if pd.to_datetime(self.last_date_used).month != pd.to_datetime(self.date_t).month:
                self.turnover_month = 0
                if self.ranked_scores is not None and not self.ranked_scores.empty:
                    self.best_scores_prev_month = self.ranked_scores['Score'].head(3).mean()
            if self.date_t in self.trading_days:
                data_subset = self.data[self.data.index < self.date_t]
                """ Pour chaque ticker, on calcule le score """
                for tic in self.tickers:
                    tic_data = data_subset[data_subset['ticker'] == tic]
                    scores.loc[self.date_t, tic] = self.generate_score(tic_data)
                current_scores = scores.loc[self.date_t].dropna()
                if not current_scores.empty:
                    df_scores = current_scores.apply(
                        lambda x: pd.Series(x, index=['Score', 'Direction'])
                        if isinstance(x, tuple) else pd.Series({'Score': np.nan, 'Direction': np.nan})
                    )
                    df_scores.dropna(inplace=True)
                    df_scores['Score'] = pd.to_numeric(df_scores['Score'], errors='coerce')
                    self.ranked_scores = df_scores.reset_index().rename(columns={'index': 'ticker'})
                    self.ranked_scores = self.ranked_scores.sort_values(by='Score', ascending=False)
                    trades = self.strategy_low_turnover(self.ranked_scores, str(self.date_t.date()))
                    self.deals.extend(trades)
                """ Puisque nous avons fait un deal, on garde en mémoire la date """
                self.last_date_used = self.date_t
            """ On passe à la semaine suivante en rajoutant 7 jours"""
            self.date_t += pd.Timedelta(days=7)
        print("\nListe des deals :", self.deals)
        print(f"Nombre total des deals : {len(self.deals)}")

    def run(self):
        self.load_data()
        self.prepare_previous_month_scores()
        self.run_strategy()


