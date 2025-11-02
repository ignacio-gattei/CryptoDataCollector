import requests
import pandas as pd
import time
from datetime import datetime

class CryptoDataCollector:
    BASE_URL = "https://api.coingecko.com/api/v3/coins/markets"

    def __init__(self, vs_currency="usd", total=100, per_page=250, delay=65, numb_reqs_until_waiting=4):
        """
        Inicializa clase
        :param vs_currency: moneda
        :param total: cantidad total de criptomonedas a obtener
        :param per_page: cantidad por página
        :param delay: segundos de espera entre requests (para el limit request de la cuenta free)
        :numb_req_until_waiting: es la cantidad de requests que hace antes de esperar
        """
        self.vs_currency = vs_currency
        self.total = total
        self.per_page = per_page
        self.delay = delay
        self.pages = total // per_page
        self.data = []
        self.df = pd.DataFrame()
        self.numb_reqs_until_waiting = numb_reqs_until_waiting

    def get_page(self, page):
        """Request, obtiene una pagina"""
        params = {
            "vs_currency": self.vs_currency,
            "order": "market_cap_desc",
            "per_page": self.per_page,
            "page": page,
            "sparkline": "false",
            "price_change_percentage": "1h,24h,7d"
        }

        response = requests.get(self.BASE_URL, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error {response.status_code} al descargar página {page}")
            return []

    def get_all_pages(self):
        """Descarga todas las páginas de criptomonedas."""
        i = 0
        print(f"🔄 Descargando top {self.total} criptomonedas en {self.vs_currency.upper()}...")

        for page in range(1, self.pages + 1):
            print(f"📥 Página {page}/{self.pages}")
            data = self.get_page(page)
            self.data.extend(data)
            i += 1
            if i > self.numb_reqs_until_waiting:
              print(f"Esperando {self.delay} segundos...")
              time.sleep(self.delay)
              i = 0

        print(f"Descarga completa: {len(self.data)} registros.")
        return self.data

    def to_dataframe(self):
        """Convierte los datos descargados a un df de pandas."""
        if not self.data:
            print("No hay datos.")
            return None

        self.df = pd.DataFrame(self.data)

        # Renombrar columnas
        self.df = self.df.rename(columns={
            "id": "ID",
            "symbol": "Símbolo",
            "name": "Nombre",
            "current_price": f"Precio_actual",
            "market_cap": "Capitalización_de_mercado",
            "market_cap_rank": "Ranking",
            "total_volume": "Volumen_total",
            "high_24h": "Máximo_24h",
            "low_24h": "Mínimo_24h",
            "price_change_percentage_24h": "%_cambio_24h",
            "circulating_supply": "Suministro_en_circulación",
            "total_supply": "Suministro_total",
            "ath": "Máximo_historico",
            "ath_change_percentage": "%_desde_máximo_historico",
            "atl": "Mínimo_historico",
            "atl_change_percentage": "%_desde_mínimo_historico",
            "last_updated": "Última_actualización"
        })



    def print_df(self, n=None):
        """Muestra las primeras n filas."""
        if self.df.empty:
            print("El DataFrame está vacío.")
            return

        if n is None:
         n = self.total
        print(self.df.head(n))

    def save_csv(self, filename=None):
        """Guarda el DataFrame a un archivo CSV con fecha en el nombre."""
        if self.df.empty:
            print("El DataFrame está vacío.")
            return

        if not filename:
            fecha = datetime.now().strftime("%Y-%m-%d")
            filename = f"coingecko_top{self.total}_{self.vs_currency}_{fecha}.csv"

        self.df.to_csv(filename, index=False, encoding="utf-8-sig")
        print(f"Archivo guardado como: {filename}")
        return filename

    def count_rows_df(self):
        """Devuelve e imprime la cantidad de filas del DataFrame."""
        if self.df.empty:
            print("El DataFrame está vacío.")
            return 0

        count = len(self.df)
        print(f"Cantidad de registros en el DataFrame: {count}")
        return count

if __name__ == "__main__":
    cg = CryptoDataCollector(vs_currency="usd", total=500)
    cg.get_all_pages()
    cg.to_dataframe()
    cg.print_df()
    cg.count_rows_df()
