import sqlite3
import pandas as pd
from faker import Faker
from data_loader_v2 import get_financial_data
import random

# Chargement des données financières
data = get_financial_data()
fake = Faker()

# %% Table Clients 
def clients(): 
    """
    Génère une table Clients avec des données fictives pour simuler une base client.
    """
    risk_profiles = ["Low Risk", "Low Turnover", "High Yield Equity Only"]
    n=3 # Nombre de clients à générer

     # Création d'un DataFrame de données clients fictifs
    clients = pd.DataFrame({
        "last_name": [fake.last_name() for _ in range(n)],
        "first_name": [fake.first_name() for _ in range(n)],
        "birth_date": [fake.date_of_birth(minimum_age=18, maximum_age=80).strftime("%Y-%m-%d") for _ in range(n)],
        "address": [fake.address().replace("\n", ", ") for _ in range(n)],  
        "phone_number": [fake.phone_number() for _ in range(n)],
        "email": [fake.email() for _ in range(n)],
        "registration_date": [fake.date_between(start_date="-5y", end_date="today").strftime("%Y-%m-%d") for _ in range(n)],
        "risk_profile": risk_profiles  
    })

    conn = sqlite3.connect("fund_database.db")
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS Clients") # Réinitialise la table Clients si elle existe déjà

    # Création de la table Clients avec les colonnes spécifiées

    cursor.execute("""
    CREATE TABLE Clients (
        client_id INTEGER PRIMARY KEY AUTOINCREMENT,
        last_name TEXT NOT NULL,
        first_name TEXT NOT NULL,
        birth_date DATE NOT NULL,
        address TEXT NOT NULL,
        phone_number TEXT NOT NULL,
        email TEXT UNIQUE NOT NULL,
        registration_date DATE NOT NULL,
        risk_profile TEXT CHECK(risk_profile IN ('Low Risk', 'Low Turnover', 'High Yield Equity Only')) NOT NULL
    )
    """)

    # Insertion des données dans la table
    clients.to_sql("Clients", conn, if_exists="append", index=False, method="multi")

    conn.commit()
    conn.close()

#%% Table des produits
def products():
     """
    Génère la table Products avec des produits uniques tirés des données financières.
    """
    conn = sqlite3.connect("fund_database.db")
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS Products")
    # Définition de la table Products
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Products (
        id_product INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker TEXT NOT NULL UNIQUE,
        category TEXT NOT NULL,
        secteur TEXT NOT NULL  
    )
    """)

    products = data[['ticker', 'Category', 'Secteur']].drop_duplicates()

    for _, row in products.iterrows():
        cursor.execute("""
            INSERT OR IGNORE INTO Products (ticker, category, secteur)
            VALUES (?, ?, ?)
        """, (row["ticker"], row["Category"], row["Secteur"]))

    conn.commit()
    conn.close()

# %% Création de la table Returns
def returns():
    """
    Génère une table contenant les retours quotidiens des actifs.
    """
    conn = sqlite3.connect("fund_database.db")
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS Returns")

    cursor.execute("""
    CREATE TABLE Returns (
        id_returns INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker TEXT NOT NULL,
        date TEXT NOT NULL,
        return REAL,
        price REAL,
        secteur TEXT NOT NULL,
        FOREIGN KEY (ticker) REFERENCES Products(ticker)
    )
    """)
    
    data.reset_index(inplace=True)
    data["Date"] = data["Date"].astype(str)

    for _, row in data.iterrows():
        cursor.execute("""
            INSERT INTO Returns (ticker, date, return, price, secteur) 
            VALUES (?, ?, ?, ?, ?)
        """, (row["ticker"], row["Date"], row["Returns"], row["Close"], row["Secteur"]))

    conn.commit()
    conn.close()

# %% Création de la table managers
def managers():
    """
    Génère une table Managers contenant des gestionnaires fictifs avec profils de risque associés.
    """
    conn = sqlite3.connect("fund_database.db")
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS Managers")

    cursor.execute("""CREATE TABLE Managers (
        id_manager INTEGER PRIMARY KEY AUTOINCREMENT,  
        first_name TEXT NOT NULL, 
        last_name TEXT NOT NULL,  
        email TEXT UNIQUE NOT NULL,  
        phone_number TEXT,  
        experience_years INTEGER CHECK (experience_years >= 0),  
        risk_profile TEXT CHECK (risk_profile IN ('Low Risk', 'Low Turnover', 'High Yield Equity')),  
        assigned_since TEXT DEFAULT (DATE('now'))  
    );
    """)

    risk_profiles = ['Low Risk', 'Low Turnover', 'High Yield Equity']

    for risk_profile in risk_profiles:
        cursor.execute("""
            INSERT INTO Managers (first_name, last_name, email, phone_number, experience_years, risk_profile, assigned_since)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            fake.first_name(), fake.last_name(), fake.unique.email(), fake.phone_number(),
            random.randint(5, 30), risk_profile, fake.date_between(start_date="-5y", end_date="today")
        ))

    conn.commit()
    conn.close()

# %% Création de la table des portefeuilles
def pf():
    """
    Génère une table pf contenant les portefeuilles.
    """
    conn = sqlite3.connect("fund_database.db")
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS Portfolios")

    cursor.execute("""
    CREATE TABLE Portfolios (
        id_portfolio INTEGER PRIMARY KEY AUTOINCREMENT,
        risk_profile TEXT CHECK (risk_profile IN ('Low Risk', 'Low Turnover', 'High Yield Equity')) NOT NULL,  
        manager_id INTEGER,  
        FOREIGN KEY (manager_id) REFERENCES Managers(id_manager) ON DELETE SET NULL
    );
    """)

    cursor.execute("SELECT id_manager, risk_profile FROM Managers")
    managers = cursor.fetchall()  

    for manager_id, risk_profile in managers:
        cursor.execute("""
            INSERT INTO Portfolios (risk_profile, manager_id) 
            VALUES (?, ?)
        """, (risk_profile, manager_id))

    conn.commit()
    conn.close()

# %% Création de la table Portfolio_Holdings
def pfh():
    """
    Génère une table Portfolio_Holdings contenant des produits détenu en portefeuille.
    """
    conn = sqlite3.connect("fund_database.db")
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS Portfolio_Holdings")

    cursor.execute("""
    CREATE TABLE Portfolio_Holdings (
        id_holding INTEGER PRIMARY KEY AUTOINCREMENT,  
        date TEXT NOT NULL, 
        id_portfolio INTEGER NOT NULL,  
        ticker TEXT NOT NULL,  
        weight REAL CHECK(weight >= 0 AND weight <= 1),   
        FOREIGN KEY (id_portfolio) REFERENCES Portfolios(id_portfolio),
        FOREIGN KEY (ticker) REFERENCES Products(ticker)
    );
    """)
    conn.commit()
    conn.close()

# %% Création de la table deals
def deals():
    """
    Génère une table deals contenant tous les deals effectués.
    """
    conn = sqlite3.connect("fund_database.db")
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS Deals")

    cursor.execute("""
    CREATE TABLE Deals (
        deal_id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,       
        id_portfolio INTEGER NOT NULL, 
        risk_profile TEXT NOT NULL,    
        action TEXT NOT NULL,          
        asset TEXT NOT NULL,           
        quantity REAL NOT NULL CHECK (quantity >= 0),
        secteur TEXT NOT NULL,
        FOREIGN KEY (id_portfolio) REFERENCES Portfolios(id_portfolio) ON DELETE CASCADE
    );
    """)
    conn.commit()
    conn.close()

# %% Lancement de la base de données 
def lancement_base():
    """Lance toutes les fonctions pour créer et remplir les tables de la base de données."""
    clients()
    products()
    returns()
    managers()
    pf()
    pfh()
    deals()
