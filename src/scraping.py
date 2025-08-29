import pandas as pd
import time
import logging
from typing import List, Dict, Any

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException, NoSuchElementException



SITES: List[Dict[str, Any]] = [
    {
        "nome": "Consumo de Água",
        "url": "https://aplicacoes.cidadania.gov.br/vis/data3/v.php?q[]=oNOhlMHqwGZsemeZ6au8srNekJavm7mj2ejJZmx4aWepanp%2BaGiJkWWdamOUqH1luL5ma6ptiLSYmcrGbtCen9DgiG%2BiqaGt3nSIwaya06Sc3bGYz%2Bmup1yulqfipbavqZLKgZfPXfb%2B4sKVXLiWrNpZsL2loMzOooplZB8lbffdr6qbolmyvKufvMioz7BTzeC5o1yVeY2ZYY6xrJrMzZTOrFzZ64iwuMRw&ag=m&ultdisp=1",
        "max_paginas": 20,
        "arquivo_saida": "dados_consumo.csv"
    },
    {
        "nome": "Produção de Água",
        "url": "https://aplicacoes.cidadania.gov.br/vis/data3/v.php?q[]=oNOhlMHqwGZsemeZ6au8srNekJivm7mj2ejJZmx4bWepanp%2BaGiJkWWdamOUqH1luL5ma6pviLSYmcrGbtCen9DgiG%2BiqaGt3nSIwaya06Sc3bGYz%2Bmup1yulqfipbavqZLKgZfPXfb%2B4sKVXLiWrNpZvcCmkcwk2i3gon2jf%2FbmaPjb4K6ud1eSxdWlz6Sowu5tpKG0pFrGfaBuX2661qDfqZTB6nawrIOxtvV0&ag=m&ultdisp=1",
        "max_paginas": 20,
        "arquivo_saida": "dados_producao.csv"
    },
    {
        "nome": "Dados Escolares",
        "url": "https://aplicacoes.cidadania.gov.br/vis/data3/v.php?q[]=oNOhlMHqwGZsemeZ6au8srNfh5Ovm7mj2ejJZmx5Z2epanp%2BaGiJkWWdamOUqH1luL5ma6tqiLSYmcrGbtCen9DgiG%2BiqaGt3nSIwayaetxUzayUyeDAl6FwdbCqan9%2FY12AgV6KoKK%2B57Knn61deu9qfoBnWYeKVOd4mb7nwJl3rpam7J6IiZ2Ow9SYpXim0ujJhbGpo67ina6ynE27xlOtpqbR4L%2BinbtVf%2BycvLqYn7zUU8%2Brp8%2FgtKmhu1VnmZ7Fs5qiGgj2DaxTsMSUVH%2BxqK7eq7uvqlCayqbeoqXL3MBUgbuYqeWav7OqTbzPp9yimtLgwFSsraGpmYaRoVdNf6KW36qoydyxo2XEpXX1tcmJ&ultdisp=1&ag=m&ultdisp=0&ultdisp=1",
        "max_paginas": 20,
        "arquivo_saida": "dados_escolares.csv"
    }
]


def driver_configuration() -> WebDriver:
   
    service = Service(ChromeDriverManager().install())
    
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    
    return webdriver.Chrome(service=service, options=options)

def scrape_table_from_page(driver: WebDriver) -> pd.DataFrame:
   
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, "table"))
        )
        html_content = driver.page_source
        tables = pd.read_html(html_content)
        if tables:
            return tables[0]
    except TimeoutException:
       print("Tempo de espera excedido. A tabela não foi encontrada ou não carregou.")
    except Exception as e:
        print(f"Erro ao ler a tabela com pandas: {e}")
        
    return pd.DataFrame()

def scrape_data_site(driver: WebDriver, config: Dict[str, Any], delay: int = 2) -> pd.DataFrame:
   
    base_url = config["url"]
    max_paginas = config["max_paginas"]
    
    driver.get(base_url)
    
    todos_os_dados = []
    
    for num_pagina in range(1, max_paginas + 1):
        df_pagina = scrape_table_from_page(driver)
        
        if df_pagina.empty:
            print(f"Nenhuma tabela encontrada na página {num_pagina}. Interrompendo.")
            break
        
        todos_os_dados.append(df_pagina)
        

        try:
            next_button = driver.find_element(By.CSS_SELECTOR, "a.paginate_button.next:not(.disabled)")
            driver.execute_script("arguments[0].click();", next_button)
            time.sleep(delay)
        except NoSuchElementException:
            print("Botão 'Próxima' não encontrado ou desabilitado. Fim da extração.")
            break
        except Exception as e:
            print(f"Erro inesperado ao tentar navegar para a próxima página: {e}")
            break
            
    if not todos_os_dados:
        print(f"Nenhum dado foi extraído para o site: {config['nome']}")
        return pd.DataFrame()

    
    df_final = pd.concat(todos_os_dados, ignore_index=True)
    
    

    linhas_iniciais = len(df_final)
    df_final.drop_duplicates(inplace=True)
    linhas_finais = len(df_final)
    
    if linhas_iniciais != linhas_finais:
        print(f"Removidas {linhas_iniciais - linhas_finais} linhas duplicadas.")
    
   
    return df_final

def save_csv(df: pd.DataFrame, nome_arquivo: str):
    
    if not df.empty:
        try:
            
            df.to_csv(nome_arquivo, index=False, sep=";", encoding="utf-8-sig")
            print(f"Arquivo '{nome_arquivo}' salvo")
        except Exception as e:
           print(f"Falha ao salvar o arquivo '{nome_arquivo}': {e}")
    else:
        print(f"DataFrame vazio. Nenhum arquivo foi salvo para '{nome_arquivo}'.")


def main():
    
    driver = driver_configuration()
    try:
        for config in SITES:
            df_completo = scrape_data_site(driver, config)
            save_csv(df_completo, config["arquivo_saida"])
    finally:
       
        print("webdriver encerrado.")
        driver.quit()

if __name__ == "__main__":
    main()