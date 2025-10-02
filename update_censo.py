import os
import time
import pandas as pd
import requests
import zipfile
import glob
import shutil
import gc
import subprocess, time

from bs4 import BeautifulSoup

# Importações do Selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

def baixar_censo_superior_ufrj():
    """
    Versão corrigida que busca o link de download diretamente, sem a necessidade
    de cliques para expandir seções.
    """
    # --- Parte 1: Entrada do Usuário ---
    ano_desejado = input("Digite o ano do Censo Superior que deseja baixar (ex: 2022): ")
    if not ano_desejado.isdigit() or len(ano_desejado) != 4:
        print("Entrada inválida. Por favor, digite um ano com 4 dígitos.")
        return

    print(f"Iniciando processo para o Censo da Educação Superior de {ano_desejado}...")
    
    pasta_raiz_temporaria = f"censo_superior_{ano_desejado}_temp"
    if os.path.isdir(pasta_raiz_temporaria):
        shutil.rmtree(pasta_raiz_temporaria, ignore_errors=True)
    os.makedirs(pasta_raiz_temporaria, exist_ok=True)

    pasta_csv_final = os.path.join('DADOS_ES_UFRJ', 'censo_es_ufrj')
    os.makedirs(pasta_csv_final, exist_ok=True)

    headers_download = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
    }

    # --- Parte 2: Coletar o link de download com Selenium (LÓGICA SIMPLIFICADA) ---
    print("Iniciando Selenium para encontrar o link de download...")
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_argument("--start-maximized")
    service = ChromeService(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    link_para_baixar = None

    try:
        url_base = "https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-da-educacao-superior"
        driver.get(url_base)
        wait = WebDriverWait(driver, 20)
        
        try:
            # Tratamento de cookie (mantido por segurança)
            seletor_botao_aceitar = "button.br-button.secondary.small.btn-accept"
            botao_cookies = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, seletor_botao_aceitar)))
            driver.execute_script("arguments[0].click();", botao_cookies)
            time.sleep(2)
        except TimeoutException:
            print("Banner de cookies não foi encontrado, prosseguindo.")
        
        print(f"Procurando diretamente pelo link de download para o ano de {ano_desejado}...")
        try:

            # O seletor procura por um link <a> que contenha ambos os textos no seu conteúdo.
            seletor_link = f"//a[contains(text(), 'Microdados do Censo da Educação Superior') and contains(text(), '{ano_desejado}')]"
            
            # Espera até que o link esteja presente no DOM da página
            link_element = wait.until(EC.presence_of_element_located((By.XPATH, seletor_link)))
            
            link_para_baixar = link_element.get_attribute('href')
            print(f"Link encontrado: {link_para_baixar}")
            
        except TimeoutException:
            print(f"ERRO: Não foi possível encontrar o link de download para o ano de {ano_desejado}.")
            # Se mesmo assim falhar, os arquivos de diagnóstico serão úteis novamente.
            driver.save_screenshot("debug_screenshot.png")
            with open("debug_page_source.html", "w", encoding="utf-8") as f:
                f.write(driver.page_source)

    finally:
        print("Coleta de link finalizada. Fechando o navegador.")
        driver.quit()

    # --- Parte 3: Download, Processamento e Limpeza (sem alterações) ---
    if not link_para_baixar:
        print("\nNenhum link de download foi coletado. Processo encerrado.")
        if os.path.isdir(pasta_raiz_temporaria):
            shutil.rmtree(pasta_raiz_temporaria, ignore_errors=True)
        return

    nome_arquivo_zip = os.path.basename(link_para_baixar)
    caminho_zip = os.path.join(pasta_raiz_temporaria, nome_arquivo_zip)
    
    print(f"\n--- Processando Arquivo: {nome_arquivo_zip} ---")
    
    try:
        # 1. Download
        sucesso_download = False
        for tentativa in range(3):
            try:
                with requests.get(link_para_baixar, stream=True, headers=headers_download, timeout=120) as r:
                    r.raise_for_status()
                    with open(caminho_zip, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192): f.write(chunk)
                print("1. Download concluído com sucesso!")
                sucesso_download = True
                break
            except requests.exceptions.RequestException as e:
                print(f"   FALHA no download (tentativa {tentativa + 1}/3): {e}")
                if tentativa < 2: time.sleep(10)
        
        if not sucesso_download:
            raise Exception("Não foi possível baixar o arquivo após 3 tentativas.")

        # 2. Descompactar
        print(f"2. Descompactando '{nome_arquivo_zip}'...")
        with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
            zip_ref.extractall(pasta_raiz_temporaria)

        # 3. Encontrar o arquivo de DADOS
        print("3. Procurando pelo arquivo de dados principal...")
        preferencias = [
            "CADASTRO_CURSOS",  # quero este primeiro
            "CURSOS"
        ]

        arquivos_encontrados = []
        for subfolder_name in ['dados', 'DADOS', 'microdados', 'MICRODADOS']:
            for extension in ['*.csv', '*.CSV']:
                arquivos_encontrados.extend(
                    glob.glob(os.path.join(pasta_raiz_temporaria, '**', subfolder_name, extension), recursive=True)
                )

        # ordenar pela prioridade
        caminho_dados = None
        for preferencia in preferencias:
            for arquivo in arquivos_encontrados:
                if preferencia in os.path.basename(arquivo).upper():
                    caminho_dados = arquivo
                    break
            if caminho_dados:
                break

        if not caminho_dados and arquivos_encontrados:
            caminho_dados = arquivos_encontrados[0]  # fallback
            
        if not caminho_dados:
            raise Exception("Nenhum arquivo de dados (.csv) encontrado nas subpastas esperadas.")
        
        print(f"   Arquivo de dados encontrado: {os.path.basename(caminho_dados)}")

        # 4. Ler, Filtrar e Salvar
        print("4. Lendo, filtrando dados da UFRJ (CO_IES == 586) e salvando...")
        df = pd.read_csv(caminho_dados, sep=';', encoding='latin-1', low_memory=False)
        
        coluna_ies = next((col for col in df.columns if str(col).strip().upper() == 'CO_IES'), None)
        
        if not coluna_ies:
            raise Exception(f"A coluna 'CO_IES' não foi encontrada. Colunas disponíveis: {df.columns.tolist()}")

        ufrj_df = df[df[coluna_ies] == 586].copy()

        if ufrj_df.empty:
            print("   AVISO: Nenhum dado encontrado para a UFRJ (CO_IES 586) neste arquivo.")
        else:
            nome_csv_final = f"UFRJ_CENSO_{ano_desejado}.csv"
            caminho_csv = os.path.join(pasta_csv_final, nome_csv_final)
            ufrj_df.to_csv(caminho_csv, index=False, encoding='utf-8-sig')
            print(f"   SUCESSO: {len(ufrj_df)} registros da UFRJ salvos em '{caminho_csv}'")
    
    except Exception as e:
        print(f"   ERRO GERAL no processamento do arquivo: {e}")

    finally:
        print("5. Limpando a pasta de arquivos temporários...")
        try:
            del df, ufrj_df
        except:
            pass
        gc.collect()

        # tenta remover o zip antes
        if os.path.exists(caminho_zip):
            try:
                os.remove(caminho_zip)
            except PermissionError:
                print("   Aviso: zip ainda em uso, ignorando...")

        # remove a pasta temporária inteira
        if os.path.isdir(pasta_raiz_temporaria):
            for tentativa in range(5):
                try:
                    shutil.rmtree(pasta_raiz_temporaria)
                    if not os.path.exists(pasta_raiz_temporaria):
                        print(f"   Pasta temporária '{pasta_raiz_temporaria}' removida com sucesso.")
                        break
                except Exception as e:
                    print(f"   Tentativa {tentativa+1}/5 falhou: {e}")
                    time.sleep(3)
            else:
                # fallback: usar comando nativo do Windows
                try:
                    subprocess.run(["cmd", "/c", "rmdir", "/s", "/q", pasta_raiz_temporaria], check=True)
                    if not os.path.exists(pasta_raiz_temporaria):
                        print(f"   Pasta temporária '{pasta_raiz_temporaria}' removida via rmdir.")
                    else:
                        print(f"   ERRO FINAL: pasta '{pasta_raiz_temporaria}' ainda existe mesmo após rmdir.")
                except Exception as e:
                    print(f"   ERRO FINAL: não foi possível remover '{pasta_raiz_temporaria}': {e}")

    print(f"\nProcesso concluído!")

if __name__ == "__main__":
    baixar_censo_superior_ufrj()