# Multi-Asset-Fund-Simulation

💼 Multi-Asset Fund Simulation - Data Management Project
Date : Mars 2025
Université : Paris Dauphine - PSL
Objectif : Créer un fonds multi-actifs simulé gérant différents profils de clients à l’aide de stratégies d’investissement automatisées.

📌 Objectif du Projet
Ce projet vise à modéliser un fonds d’investissement multi-asset intégrant trois profils clients :

🧘 Low Risk : portefeuille à volatilité annuelle max de 10%.

🔁 Low Turnover : max 2 transactions par mois.

📈 High Yield Equity Only : objectif de rendement maximal, uniquement en actions, sans contrainte.

Chaque lundi entre le 01/01/2023 et le 31/12/2024, nos algorithmes déterminent quels actifs acheter ou vendre, en s'appuyant sur des données financières récupérées en ligne et des stratégies dynamiques.

🛠️ Architecture du Projet
1. data_preparation.py
Préparation et nettoyage des données via Yahoo Finance.
📌 Gestion des valeurs aberrantes via z-score.

2. database_creation.py
Création des 7 tables SQL : Clients, Produits, Rendements, Portefeuilles, Managers, Deals, Holdings.
📌 Génération de données synthétiques avec Faker.

3. strategies.py
Trois stratégies implémentées :

Low Risk : Optimisation des poids via differential_evolution.

Low Turnover : Score basé sur la moyenne mobile et sélection de timing.

High Yield Equity Only : Modèle Random Forest pour prédiction de hausse journalière.

4. base_update.py
🔄 Mise à jour des tables Deals et Portfolio Holdings chaque semaine.

5. performance.py
Évaluation des performances du fonds :

Volatilité, Sharpe Ratio

Répartition sectorielle et par profil

Visualisations graphiques

6. main.py
Fichier principal qui orchestre :

Le téléchargement des données

La création des bases

Le lancement hebdomadaire des stratégies

L’évaluation de performance

7. Bonus : Optimisations par Algorithmes Génétiques
Deux fonctions avancées :

strategy_high_yield_equity_optimization()

lowturnover_strategy()
📌 Optimisation de portefeuille par méthode évolutive (Genetic Algorithm).

🔍 Environnement
Python 3.11+

Packages principaux : pandas, numpy, scipy, sklearn, yfinance, sqlalchemy, faker, matplotlib

Voir myenv.zip pour l’environnement virtuel complet
