import sqlite3

def insert_deals(date, id_portfolio, risk_profile, action, asset, quantity, secteur):
    conn = sqlite3.connect("fund_database.db")
    cursor = conn.cursor()
    
    cursor.execute(
        """
        INSERT INTO Deals (date, id_portfolio, risk_profile, action, asset, quantity, secteur)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (date.strftime("%Y-%m-%d"), id_portfolio, risk_profile, action, asset, quantity, secteur)
    )
    
    conn.commit()
    conn.close()

def update_pfh(date, id_portfolio, ticker, action, quantity):
    conn = sqlite3.connect("fund_database.db")
    cursor = conn.cursor()
    cursor.execute(
        "SELECT weight FROM Portfolio_Holdings WHERE id_portfolio = ? AND ticker = ?",
        (id_portfolio, ticker)
    )
    existing_holding = cursor.fetchone()
    
    if existing_holding:
        new_weight = existing_holding[0] + (quantity / 10000 if action == 'buy' else -quantity / 10000)
        new_weight = max(0, min(1, new_weight))  # S'assurer que le poids reste entre 0 et 1
        
        cursor.execute(
            """
            UPDATE Portfolio_Holdings
            SET weight = ?
            WHERE id_portfolio = ? AND ticker = ?
            """,
            (new_weight, id_portfolio, ticker)
        )
    else:
        new_weight = quantity / 10000 if action == 'buy' else 0
        cursor.execute(
            """
            INSERT INTO Portfolio_Holdings (date, id_portfolio, ticker, weight)
            VALUES (?, ?, ?, ?)
            """,
            (date.strftime("%Y-%m-%d"), id_portfolio, ticker, new_weight)
        )
    
    conn.commit()
    conn.close()
    print(f"Portfolio updated: {action} {quantity} of {ticker} on {date}.")

