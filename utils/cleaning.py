
import re
import pandas as pd

PRICE = re.compile(r'(\d[\d\s\.,]*)', re.I)

def basic_cleaning(df_raw: pd.DataFrame, dropna_thresh: float = 0.0, drop_duplicates: bool = False) -> pd.DataFrame:
    df = df_raw.copy()
    candidates_price = [c for c in df.columns if str(c).lower() in ['price_cfa','price','prix','price_raw']]
    price_col = candidates_price[0] if candidates_price else None
    def _to_int(s):
        if s is None: return None
        s = str(s).replace(' ',' ').replace(' ',' ').replace(' ','').replace('.','').replace(',','')
        try: return int(s)
        except: return None
    def price_to_cfa(txt):
        if txt is None: return None
        m = PRICE.search(str(txt))
        return _to_int(m.group(1)) if m else None
    df['price_cfa'] = df[price_col].apply(price_to_cfa) if price_col is not None else None

    candidates_addr = [c for c in df.columns if str(c).lower() in ['address_raw','adresse','address','location','ad__card-location']]
    addr_col = candidates_addr[0] if candidates_addr else None
    def extract_city(addr):
        if addr is None: return None
        s = str(addr)
        for sep in ['•','-','|',',','/']:
            if sep in s: return s.split(sep)[0].strip()
        return s.strip()
    df['city'] = df[addr_col].apply(extract_city) if addr_col is not None else None

    candidates_title = [c for c in df.columns if str(c).lower() in ['title','nom','name','details','detail','ad__card-description']]
    title_col = candidates_title[0] if candidates_title else None
    df['title_len'] = df[title_col].apply(lambda x: len(str(x)) if x is not None else 0) if title_col is not None else 0

    if drop_duplicates:
        df = df.drop_duplicates()
    if dropna_thresh > 0:
        df = df.dropna(thresh=int(df.shape[1]*dropna_thresh))
    return df
