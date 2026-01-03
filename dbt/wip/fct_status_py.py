import bigframes.pandas as bpd
import pandas as pd
import numpy as np

# --------- Robust XNPV / XIRR (fallback, no external deps) ----------
def _xnpv(rate, cashflows, dates):
    # rate는 연간 수익률 (예: 0.1 == 10%)
    if rate <= -0.999999:
        return np.nan
    t0 = dates[0]
    years = np.array([(d - t0).days / 365.0 for d in dates], dtype=float)
    cf = np.array(cashflows, dtype=float)
    base = 1.0 + rate
    if base <= 0:
        return np.nan
    return np.sum(cf / (base ** years))

def _xirr(cashflows, dates, guess=0.10, tol=1e-7, maxiter=50):
    # 먼저 부호 변화(음수/양수 CF)가 있어야 IRR이 존재
    low, high = -0.9999, 10.0
    f_low = _xnpv(low, cashflows, dates)
    f_high = _xnpv(high, cashflows, dates)
    if (not np.isfinite(f_low)) or (not np.isfinite(f_high)) or (f_low * f_high > 0):
        return None

    # Newton 시도
    r = guess
    for _ in range(maxiter):
        if r <= -0.9999:
            r = -0.9999 + 1e-8
        t0 = dates[0]
        years = np.array([(d - t0).days / 365.0 for d in dates], dtype=float)
        cf = np.array(cashflows, dtype=float)
        base = 1.0 + r
        if base <= 0:
            break
        f = np.sum(cf / (base ** years))
        df = np.sum(-years * cf / (base ** (years + 1.0)))
        if (not np.isfinite(df)) or df == 0:
            break
        r_new = r - f / df
        if not np.isfinite(r_new):
            break
        if abs(r_new - r) < tol:
            return float(r_new)
        r = r_new

    # Bisection fallback
    lo, hi = low, high
    f_lo, f_hi = f_low, f_high
    for _ in range(100):
        mid = (lo + hi) / 2.0
        f_mid = _xnpv(mid, cashflows, dates)
        if not np.isfinite(f_mid):
            mid = max(mid, -0.9999 + 1e-8)
            f_mid = _xnpv(mid, cashflows, dates)
        if abs(f_mid) < tol:
            return float(mid)
        if f_lo * f_mid < 0:
            hi, f_hi = mid, f_mid
        else:
            lo, f_lo = mid, f_mid
    return float((lo + hi) / 2.0)

# --------- dbt Python model ----------
def model(dbt, session):
    """
    Input : marts_facts.fct_status [date, asset_name, purchase_sum, balance]
    Output: marts_facts.fct_status_irr_py
            [asset_name, start_date, end_date, years, invested, final_balance,
             irr_annual, irr_period]

    irr_period = 비연간화 IRR (해당 전체 기간 수익률)
    irr_annual = 연간화 IRR (참고용)
    """
    dbt.config(materialized="table")

    src = dbt.ref("fct_status").to_pandas()
    if src.empty:
        return bpd.DataFrame(pd.DataFrame(
            columns=[
                "asset_name","start_date","end_date","years",
                "invested","final_balance","irr_annual","irr_period"
            ]
        ))

    # 정리
    src = src.copy()
    src["date"] = pd.to_datetime(src["date"]).dt.date
    for col in ("purchase_sum", "balance"):
        if col not in src.columns:
            raise ValueError(f"Missing required column '{col}' in fct_status")
        src[col] = src[col].fillna(0.0).astype(float)

    if "asset_name" not in src.columns:
        raise ValueError("Missing required column 'asset_name' in fct_status")

    out_rows = []

    # 자산별 전체 기간 한 줄 계산
    for asset, g in src.groupby("asset_name", dropna=False):
        g = g.sort_values("date")
        if g.empty:
            continue

        # 누적 매입금액의 일별 증가분을 '현금유출(음수)'로 기록
        cashflows, dates = [], []
        prev_psum = 0.0
        for row in g.itertuples(index=False):
            d = pd.to_datetime(getattr(row, "date")).date()
            psum = float(getattr(row, "purchase_sum"))
            delta = psum - prev_psum
            prev_psum = psum
            if delta != 0:
                cashflows.append(-delta)     # 매입 = 돈 나감(음수)
                dates.append(pd.to_datetime(d))

        # 마지막 날의 평가잔액을 회수 현금흐름(양수)으로 추가
        last_date = g["date"].max()
        last_balance = float(g.loc[g["date"] == last_date, "balance"].iloc[-1])
        cashflows.append(last_balance)
        dates.append(pd.to_datetime(last_date))

        invested = -sum(cf for cf in cashflows if cf < 0)
        years = max( (dates[-1] - dates[0]).days / 365.0, 1e-9 )

        irr_ann = _xirr(cashflows, dates)  # 연간화 IRR
        irr_period = None
        if irr_ann is not None and np.isfinite(irr_ann):
            # 비연간화 IRR: 연간화 IRR을 전체기간 수익률로 변환
            irr_period = (1.0 + irr_ann) ** years - 1.0

        out_rows.append({
            "asset_name": asset,
            "start_date": dates[0].date(),
            "end_date": dates[-1].date(),
            "years": years,
            "invested": invested,
            "final_balance": last_balance,
            "irr_annual": irr_ann,
            "irr_period": irr_period,
        })

    return bpd.DataFrame(pd.DataFrame(out_rows))
