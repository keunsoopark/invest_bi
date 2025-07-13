from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd


def main():
    asset_id = "0P00000MY5.IR"
    candidate_date = datetime.strptime("2025-04-04", "%Y-%m-%d").date()

    ticker = yf.Ticker(asset_id)
    hist = ticker.history(start=str(candidate_date), end=str(candidate_date + timedelta(days=1)))
    print(type(ticker))
    print(ticker)
    print(type(hist))
    print(hist)
    close_price = hist["Close"].iloc[0] if not hist.empty else None
    print(pd.isna(close_price))
    print(close_price)
    print(hist.index[0].date())
    print(hist.index[0].isoformat())

    # data = yf.download(asset_id,
    #                    start="2025-06-13",
    #                    end="2025-06-14",
    #                    progress=False,
    #                    threads=False)
    # print(type(data))
    # print(data)


if __name__ == "__main__":
    main()
