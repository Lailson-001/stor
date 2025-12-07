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
    level=logging.INFO,   # <-- CORRIGIDO
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
        except requests.exceptions.RequestException as e:   # <-- CORRIGIDO
            logger.error(f"Erro na requisição {url}: {e}")
            raise

    def extract_products(self) -> pd.DataFrame:
        """Extrai todos os produtos da API."""

        logger.info("Extraindo produtos...")

        # Faz requisição
        data = self._make_request("products")   # <-- nome corrigido

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
        