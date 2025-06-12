import yfinance as yf
from datetime import datetime, timedelta

def get_closing_price(symbol: str, date_str: str):
    # date_str format: "YYYY-MM-DD"
    date = datetime.strptime(date_str, "%Y-%m-%d").date()
    next_date = date + timedelta(days=1)

    df = yf.download(symbol, start=str(date), end=str(next_date), progress=False)

    if df.empty:
        return f"No data for {symbol} on {date_str} (market may have been closed)"
    
    return df['Close'].iloc[0]


if __name__ == "__main__":
    # Example usage
    # symbol = "SCHY"
    symbol = "069500.KS"
    # symbol = "069500.KS"
    date_str = "2025-06-11"
    price = get_closing_price(symbol, date_str)
    print(f"The closing price for {symbol} on {date_str} is: {price}")
