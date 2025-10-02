import os
import time
import pandas as pd
import requests
import zipfile
import glob
import shutil
from bs4 import BeautifulSoup

# Importações do Selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, ElementClickInterceptedException

def baixar_e_processar_dados():
    """
    Versão final que salva os CSVs na pasta correta (DADOS_ES_UFRJ/indicadores_trajetoria_ufrj)
    e limpa os arquivos temporários.
    """
    # --- Parte 1: Entrada do Usuário ---
    ano_final_desejado = input("Digite o ano FINAL de acompanhamento (ano2, ex: 2024): ")
    if not ano_final_desejado.isdigit() or len(ano_final_desejado) != 4:
        print("Entrada inválida. Por favor, digite um ano com 4 dígitos.")
        return

    print(f"Procurando por abas cujo ano final seja {ano_final_desejado}...")
    
    # Pasta para downloads temporários e extração (será criada e depois deletada)
    pasta_raiz_temporaria = f"dados_inep_{ano_final_desejado}_temp"
    os.makedirs(pasta_raiz_temporaria, exist_ok=True)

    # --- CORREÇÃO: Define a pasta final fixa para os arquivos .csv, conforme sua estrutura ---
    pasta_csv_final = os.path.join('DADOS_ES_UFRJ', 'indicadores_trajetoria_ufrj')
    os.makedirs(pasta_csv_final, exist_ok=True) # Garante que a pasta exista

    headers_download = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
    }

    # --- Parte 2: Coletar links de todas as abas correspondentes ---
    print("Iniciando Selenium para encontrar os links...")
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_argument("--start-maximized")
    service = ChromeService(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    links_para_baixar = set()
    textos_das_abas_alvo = set()

    try:
        url_base = "https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/indicadores-educacionais/indicadores-de-trajetoria-da-educacao-superior"
        driver.get(url_base)
        wait = WebDriverWait(driver, 20)
        
        try:
            seletor_botao_aceitar = "button.br-button.secondary.small.btn-accept"
            botao_cookies = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, seletor_botao_aceitar)))
            driver.execute_script("arguments[0].click();", botao_cookies)
            time.sleep(2)
        except TimeoutException:
            print("Banner de cookies não foi encontrado, prosseguindo.")

        seletor_abas = "div.govbr-tabs div.tab a"
        seletor_botao_proximo = "div.govbr-tabs div.button-next"
        
        while True:
            abas_visiveis = driver.find_elements(By.CSS_SELECTOR, seletor_abas)
            for aba in abas_visiveis:
                try:
                    texto_aba = aba.text
                    if texto_aba:
                        partes = texto_aba.split('-')
                        if len(partes) == 2 and partes[1].strip() == ano_final_desejado:
                            if texto_aba not in textos_das_abas_alvo:
                                print(f"Aba correspondente encontrada: '{texto_aba}'")
                                textos_das_abas_alvo.add(texto_aba)
                except StaleElementReferenceException:
                    continue
            
            try:
                wait_curta = WebDriverWait(driver, 2)
                botao_proximo = wait_curta.until(EC.element_to_be_clickable((By.CSS_SELECTOR, seletor_botao_proximo)))
                driver.execute_script("arguments[0].click();", botao_proximo)
                time.sleep(1)
            except (TimeoutException, ElementClickInterceptedException):
                break

        if not textos_das_abas_alvo:
            print(f"Nenhuma aba com ano final {ano_final_desejado} foi encontrada.")
        else:
            print(f"\nColetando links de {len(textos_das_abas_alvo)} aba(s) encontrada(s)...")
            for texto_da_aba in sorted(list(textos_das_abas_alvo)):
                try:
                    aba_a_clicar = driver.find_element(By.XPATH, f"//a[text()='{texto_da_aba}']")
                    print(f"Clicando na aba '{texto_da_aba}' para obter o link...")
                    driver.execute_script("arguments[0].click();", aba_a_clicar)
                    time.sleep(2)
                    soup = BeautifulSoup(driver.page_source, 'html.parser')
                    for link in soup.find_all('a', href=True):
                        if 'download.inep.gov.br' in link['href'] and link['href'].endswith('.zip'):
                            links_para_baixar.add(link['href'])
                            break
                except Exception as e:
                    print(f"  ERRO ao processar a aba '{texto_da_aba}': {e}")
    finally:
        print("Coleta de links finalizada. Fechando o navegador.")
        driver.quit()

    # --- Parte 3: Download, Processamento e Limpeza ---
    if not links_para_baixar:
        print("\nNenhum link de download foi coletado. Processo encerrado.")
        # Limpa a pasta temporária mesmo se nada for baixado
        if os.path.isdir(pasta_raiz_temporaria):
            shutil.rmtree(pasta_raiz_temporaria)
        return

    print(f"\nIniciando o download e processamento de {len(links_para_baixar)} arquivos...")
    
    for i, link_arquivo in enumerate(sorted(list(links_para_baixar))):
        nome_arquivo_zip = os.path.basename(link_arquivo)
        caminho_zip = os.path.join(pasta_raiz_temporaria, nome_arquivo_zip)
        
        nome_subpasta_temp = os.path.splitext(nome_arquivo_zip)[0]
        pasta_temporaria = os.path.join(pasta_raiz_temporaria, nome_subpasta_temp)
        os.makedirs(pasta_temporaria, exist_ok=True)
        
        print(f"\n--- Processando Arquivo [{i+1}/{len(links_para_baixar)}]: {nome_arquivo_zip} ---")
        
        try:
            # 1. Download
            sucesso_download = False
            for tentativa in range(3):
                try:
                    with requests.get(link_arquivo, stream=True, headers=headers_download, timeout=45) as r:
                        r.raise_for_status()
                        with open(caminho_zip, 'wb') as f:
                            for chunk in r.iter_content(chunk_size=8192): f.write(chunk)
                    print("1. Download concluído com sucesso!")
                    sucesso_download = True
                    break
                except requests.exceptions.RequestException as e:
                    print(f"   FALHA no download (tentativa {tentativa + 1}/3): {e}")
                    if tentativa < 2: time.sleep(5)
            
            if not sucesso_download:
                print(f"   ERRO FINAL: Não foi possível baixar o arquivo. Pulando para o próximo.")
                continue

            # 2. Descompactar
            print(f"2. Descompactando em '{pasta_temporaria}'...")
            with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
                zip_ref.extractall(pasta_temporaria)

            # 3. Encontrar .xlsx
            print("3. Procurando pelo arquivo .xlsx...")
            arquivos_xlsx = glob.glob(os.path.join(pasta_temporaria, '**', '*.xlsx'), recursive=True)
            if not arquivos_xlsx:
                print("   ERRO: Nenhum arquivo .xlsx encontrado na subpasta.")
                continue
            caminho_xlsx = arquivos_xlsx[0]
            print(f"   Arquivo encontrado: {os.path.basename(caminho_xlsx)}")

            # 4. Ler, Filtrar e Salvar
            print("4. Lendo, filtrando dados da UFRJ e salvando em .csv...")
            df = pd.read_excel(caminho_xlsx, skiprows=8)
            coluna_ies = None
            for col in df.columns:
                if str(col).strip().upper() == 'CO_IES':
                    coluna_ies = col
                    break
            
            if not coluna_ies:
                print(f"   ERRO: A coluna 'CO_IES' não foi encontrada.")
                print(f"   Colunas disponíveis: {df.columns.tolist()}")
                continue

            ufrj_df = df[df[coluna_ies] == 586].copy()

            if ufrj_df.empty:
                print("   AVISO: Nenhum dado encontrado para a UFRJ (CO_IES 586) neste arquivo.")
            else:
                nome_base = os.path.splitext(os.path.basename(caminho_xlsx))[0]
                nome_csv = f"UFRJ_{nome_base}.csv"
                # Salva o CSV na pasta final correta
                caminho_csv = os.path.join(pasta_csv_final, nome_csv)
                ufrj_df.to_csv(caminho_csv, index=False, encoding='utf-8-sig')
                print(f"   SUCESSO: {len(ufrj_df)} registros da UFRJ salvos em '{caminho_csv}'")
        
        except Exception as e:
            print(f"   ERRO GERAL no processamento do arquivo: {e}")

        # 5. Limpeza dos arquivos temporários individuais
        finally:
            print("5. Limpando arquivos temporários do processamento atual...")
            try:
                if os.path.exists(caminho_zip):
                    os.remove(caminho_zip)
                    print(f"   Arquivo .zip '{nome_arquivo_zip}' deletado.")
                if os.path.exists(pasta_temporaria):
                    shutil.rmtree(pasta_temporaria)
                    print(f"   Pasta temporária '{os.path.basename(pasta_temporaria)}' deletada.")
            except OSError as e:
                print(f"   ERRO durante a limpeza: {e}")

    # Limpeza final da pasta raiz temporária, que agora deve estar vazia
    try:
        if os.path.isdir(pasta_raiz_temporaria):
            shutil.rmtree(pasta_raiz_temporaria)
            print(f"\nPasta temporária principal '{pasta_raiz_temporaria}' removida com sucesso.")
    except OSError as e:
        print(f"   Aviso: não foi possível remover a pasta temporária principal. {e}")


    print(f"\nProcesso concluído! Arquivos CSV finais salvos em '{pasta_csv_final}'.")

if __name__ == "__main__":
    baixar_e_processar_dados()