from datetime import timedelta

import numpy as np
import pandas as pd

from funcoes.utilitarios import loga_mensagem, loga_mensagem_erro
from persistencia.Oraprd import Oraprd


class SSNClasse:
    etapaProcess = f"class {__name__} - class SSNClasse."
    # loga_mensagem(etapaProcess)

    def __init__(self):
        var = None

    #  Função para buscar período de enquadramento de contribuintes no Simples Nacional  #
    def busca_contrib_ssn_periodo(self, inscrs, p_data_inicio, p_data_fim):
        etapaProcess = f"class {self.__class__.__name__} - def busca_contrib_ssn para o período de {p_data_inicio} a {p_data_fim}."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            df_ssn = db.select_contrib_ssn_periodo(inscrs, p_data_inicio, p_data_fim)
            del db

            if len(df_ssn) > 0:
                # retirando períodos conflitantes do dataframe
                df_ssn = self.retirar_periodo_conflitante(df_ssn)

                # Inclui coluna 'data_alteracao' com base na coluna 'data_inicio_opcao'
                df_ini = df_ssn[['numr_inscricao', 'data_inicio_opcao']].copy()
                df_ini['data_alteracao'] = df_ini['data_inicio_opcao'].fillna('1900-01-01')
                df_ini = df_ini.drop(columns=['data_inicio_opcao'])
                df_ini['desc_tipo_enqdto'] = 'Micro EPP/Simples Naciona'
                df_ini['data_alterada'] = False

                # DataFrame com data_alteracao = data_final_opcao (ignora nulos)
                df_fim = df_ssn[['numr_inscricao', 'data_final_opcao']].dropna(subset=['data_final_opcao']).copy()
                df_fim['data_alteracao'] = df_fim['data_final_opcao'] + timedelta(days=1)
                df_fim['data_alterada'] = True  # Marca como True para indicar data_fim + 1 dia
                df_fim = df_fim.drop(columns=['data_final_opcao'])
                df_fim['desc_tipo_enqdto'] = 'Normal'

                # Concatena os dois DataFrames
                df_result = pd.concat([df_ini, df_fim], ignore_index=True)

                # Ordena pelo id e data_alt, se necessário
                df_result['data_alteracao'] = pd.to_datetime(df_result['data_alteracao'])
                df_result = df_result.sort_values(by=['numr_inscricao', 'data_alteracao']).reset_index(drop=True)

                # Exclui linhas onde a data fim de vigênica do desc_tipo_enqdto coincide com a data de inicio do novo desc_tipo_enqdto
                df_result = df_result[~df_result.duplicated(subset=['numr_inscricao', 'data_alteracao'], keep=False) | (
                            df_result['data_alterada'] == False)].reset_index(drop=True)
                df_result.drop(columns=['data_alterada'], inplace=True)

                # Criar uma coluna para identificar mudanças no valor de 'desc_tipo_enqdto' por numr_inscricao
                df_result['houve_mudanca'] = df_result.groupby('numr_inscricao')['desc_tipo_enqdto'].shift(1) != \
                                             df_result['desc_tipo_enqdto']
                df_result['houve_mudanca'].fillna(True, inplace=True)

                # Elimina linhas para manter apenas as linhas onde houve mudança ou a primeira ocorrência
                df_result = df_result[df_result['houve_mudanca']].drop(columns=['houve_mudanca']).reset_index(drop=True)

                return df_result
            else:
                return []

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def busca_historico_ssn(self, p_inscrs):
        etapaProcess = f"class {self.__class__.__name__} - def busca_contrib_ssn."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            df_ssn = db.select_contrib_ssn(p_inscrs)
            del db

            if len(df_ssn) > 0:
                df_ssn = df_ssn.sort_values(by=['numr_inscricao', 'data_inicio_opcao', 'numr_opcao'], ascending=[True, True, False])

                # Preencher valores nulos de data_final_opcao com Timestamp.max
                df_ssn['data_final_opcao'] = df_ssn['data_final_opcao'].fillna(pd.Timestamp.max)

                # Lista para armazenar os resultados por numr_inscricao
                resultados = []

                # Processamento vetorizado para cada numr_inscricao
                unique_inscricoes = df_ssn['numr_inscricao'].unique()

                for inscricao in unique_inscricoes:
                    df_filtrado = df_ssn[df_ssn['numr_inscricao'] == inscricao].copy()

                    data_inicio = df_filtrado['data_inicio_opcao'].values
                    data_final = df_filtrado['data_final_opcao'].values

                    # Criar matriz de comparação para identificar períodos coincidentes dentro de cada numr_inscricao
                    mask = ~((data_inicio[:, None] >= data_inicio) & (data_final[:, None] <= data_final))
                    mask[np.arange(len(mask)), np.arange(len(mask))] = True  # Evita autoexclusão
                    valid_rows = mask.all(axis=1)

                    df_filtrado = df_filtrado[valid_rows]

                    # Criar estrutura de saída para data_inicio_opcao
                    df_inicio = df_filtrado.assign(nome_tabela='IPM_OPCAO_CONTRIB_SIMPL_SIMEI'
                                                   , nome_coluna='OPCAO_SIMPLES'
                                                   , tipo_transacao='I'
                                                   , valr_subst_coluna='3'
                                                   , valr_anterior_coluna='2'
                                                   , id_historico=0
                                                   , matr_func=0
                                                   , nome_usuario_transacao=None
                                                   , id_solict=0
                                                   , indi_correcao='N')

                    # Criar DataFrame para data_final_opcao (exceto valores iguais a Timestamp.max)
                    df_filtrado = df_inicio[df_inicio['data_final_opcao'] != pd.Timestamp.max].copy()

                    if len(df_filtrado) > 0:
                        # Calcula 'data_inicio_opcao' adicionando 1 dia
                        df_filtrado['data_inicio_opcao'] = df_filtrado['data_final_opcao'] + pd.Timedelta(days=1)

                        # Atualiza df_final com os dados processados
                        df_final = df_filtrado

                        # Adiciona colunas adicionais
                        df_final['valr_subst_coluna'], df_final['valr_anterior_coluna'] = '2', '3'

                        # Concatenar resultados individuais
                        resultados.append(pd.concat([df_inicio, df_final], ignore_index=True))
                    else:
                        resultados.append(df_inicio)

                # Unir todos os resultados em um único DataFrame
                obj_retorno = pd.concat(resultados, ignore_index=True).drop_duplicates(subset=['numr_inscricao', 'data_inicio_opcao'], keep=False)
                obj_retorno.drop(columns=['data_final_opcao', 'numr_opcao', ], inplace=True)
                obj_retorno.rename(columns={'data_inicio_opcao': 'data_hora_transacao'}, inplace=True)

                colunas = ['nome_tabela', 'nome_coluna', 'data_hora_transacao', 'tipo_transacao', 'valr_subst_coluna',
                           'valr_anterior_coluna', 'id_historico', 'matr_func', 'nome_usuario_transacao',
                           'numr_inscricao',
                           'id_solict', 'indi_correcao']
                return obj_retorno[colunas]
            else:
                return pd.DataFrame()

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def retirar_periodo_conflitante(self, df_ssn):

        # Ordenando por numr_inscricao, data_inicio_opcao e numr_opcao (descendente)
        df_ssn = df_ssn.sort_values(by=['numr_inscricao', 'data_inicio_opcao', 'numr_opcao'], ascending=[True, True, False])

        # Removendo períodos coincidentes dentro do mesmo NUMR_INSCRICAO
        result = []
        for _, group in df_ssn.groupby('numr_inscricao'):
            filtered = []
            for row in group.itertuples(index=False):  # Substituindo iterrows() por itertuples()
                row_final = row.data_final_opcao if pd.notna(row.data_final_opcao) else pd.Timestamp.max

                if not any(
                        (row.data_inicio_opcao >= r.data_inicio_opcao) and
                        (row_final <= (r.data_final_opcao if pd.notna(r.data_final_opcao) else pd.Timestamp.max))
                        for r in filtered
                ):
                    filtered.append(row)

            result.extend(filtered)

        # Criando novo DataFrame sem períodos coincidentes, preservando colunas originais
        df_cleaned = pd.DataFrame(result, columns=df_ssn.columns)
        return df_cleaned

