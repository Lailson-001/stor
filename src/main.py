#%%
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
import logging
import time
import json
from tqdm import tqdm

#%%
# configuração de logging
logging.basicConfig(
    level=logging.INFO,   
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


class FakerStoreEtl:
    BASE_URL = "https://fakestoreapi.com"

    def __init__(self, output_dir: str = "data"):
        """
        Inicializa o ETL.

        Args:
            output_dir: Diretório onde os arquivos Parquet serão salvos
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Python-FakeStore-ETL/1.0'
        })

    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> List[Dict]:
        """
        Faz requisição à API com tratamento de erros.
        """
        url = f"{self.BASE_URL}/{endpoint}"
        try:
            logger.info(f"Fazendo requisição para: {url}")
            response = self.session.get(url, params=params)
            response.raise_for_status()
            time.sleep(0.5)
            return response.json()
        except requests.exceptions.RequestException as e:   
            logger.error(f"Erro na requisição {url}: {e}")
            raise

    def extract_products(self) -> pd.DataFrame:
        """Extrai todos os produtos da API."""

        logger.info("Extraindo produtos...")

        # Faz requisição
        data = self._make_request("products")   

        # Normaliza JSON em DataFrame
        df = pd.json_normalize(
            data,
            sep="_",
            max_level=2
        )

        # Metadados
        df["extracted_at"] = datetime.now()
        df["source"] = "fakestore_api"

        logger.info(f"Extraídos {len(df)} produtos")

        return df   # <-- devolve dataframe

    
    def extract_categories(self) -> pd.DataFrame:
        """Extrair categorias de produtos"""
        logger.info("Extraindo categorias")
        categories = self._make_requests("products/categories")

# Criar DataFrame com informações detalhadas
        category_data = []
        for cat in categories:
            product=self._make_requests(f"products/category/{cat}")
            category_data.append({
                'category':cat,
                'product_count': len(product),
                'avg_price': sum(p['price'] for p in product) / len(product),
                'min_price': min(p['price'] for p in product),
                'max_price': max (p['price'] for p in product),
                'extracted_at': datetime.now()
})
                                                     
        df = pd.DataFrame(category_data)
        logger.info(f"extraidos{len(df)} categorias")
        return df  
    
    
    def extract_users(self) -> pd.DataFrame:
        """" Extrair todos os usuarios"""
        logger.info("Extraindo Usuarios...")
        data = self._make_request("users")
        
        #Normqlizar estrutura aninhada
        
        df = pd.json_normalize(
            data,
            sep='_',
            max_level=3)
        
        df['extracted_at'] = datetime.now()
        logger.info(f"extraidos{len(df)}usuarios")
        return df
    
    
    def extract_carts(self) -> pd.DataFrame:
        """Extrai todos os carrinhos."""
        logger.info("Extraindo carrinhos...")

        data = self._make_request("carts")

        cart_products = []
        for cart in data:
            for product in cart["products"]:
                cart_products.append({
                    "cart_id": cart["id"],
                    "user_id": cart["userId"],
                    "date": cart["date"],
                    "product_id": product["productId"],
                    "quantity": product["quantity"],
                })

        df = pd.DataFrame(cart_products)
        df["date"] = pd.to_datetime(df["date"])
        df["extracted_at"] = datetime.now()

        logger.info(f"Extraídos {len(data)} carrinhos com {len(df)} itens")
        return df
    
    def save_to_parquet()

            