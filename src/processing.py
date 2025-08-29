import pandas as pd

def process_data(caminho_consumo, caminho_producao, caminho_escolar, caminho_saida):
   
    try:
       
        df_consumo = pd.read_csv(caminho_consumo,delimiter=';')
        print(df_consumo.columns)
        df_producao = pd.read_csv(caminho_producao,delimiter=';')
        df_escolar = pd.read_csv(caminho_escolar,delimiter=';')

       
        df_consumo_proc = df_consumo.rename(columns={
            
            'Unidade Territorial': 'municipio',
            'UF': 'uf',
            'Referência': 'mês',
            'Cisternas familiares de água para consumo (1ª água) entregues pelo MDS (Acumulado)': 'consumo'
        })
        
        df_consumo_proc = df_consumo_proc[['uf', 'municipio', 'mês', 'consumo']]

    
        df_producao_proc = df_producao.rename(columns={
           
            'Unidade Territorial': 'municipio',
            'UF': 'uf',
            'Referência': 'mês',
            'Cisternas familiares de água para produção (2ª água) entregues pelo MDS (Acumulado)': 'producao'
        })
        df_producao_proc = df_producao_proc[['uf', 'municipio', 'mês', 'producao']]
        
        df_escolar_proc = df_escolar.rename(columns={
            
            'Unidade Territorial': 'municipio',
            'UF': 'uf',
            'Referência': 'mês',
            'Cisternas Escolares entregues pelo MDS (Acumulado)': 'escolar'
        })
        df_escolar_proc = df_escolar_proc[['uf', 'municipio', 'mês', 'escolar']]
        

        df_merged = pd.merge(df_consumo_proc, df_producao_proc, on=['uf', 'municipio', 'mês'], how='outer')
        df_final = pd.merge(df_merged, df_escolar_proc, on=['uf', 'municipio', 'mês'], how='outer')
        print("DADOS MESCLADOS:")
        print(df_final.head())

       
        colunas_ordenadas = ['uf', 'municipio', 'mês', 'consumo', 'producao', 'escolar']
        df_final = df_final[colunas_ordenadas]


        df_final.to_csv(caminho_saida, index=False,sep=';', encoding='utf-8-sig')

       
    except FileNotFoundError as e:
        print(f"Erro: Arquivo não encontrado. Verifique o caminho: {e.filename}")
    except Exception as e:
        print(f"Ocorreu um erro inesperado durante o processamento: {e}")


ARQUIVO_CONSUMO = 'dados_consumo.csv'
ARQUIVO_PRODUCAO = 'dados_producao.csv'
ARQUIVO_ESCOLAR = 'dados_escolares.csv'
ARQUIVO_SAIDA = 'dados_tratados.csv'



process_data(ARQUIVO_CONSUMO, ARQUIVO_PRODUCAO, ARQUIVO_ESCOLAR, ARQUIVO_SAIDA)