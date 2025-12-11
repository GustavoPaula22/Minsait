from datetime import datetime, timedelta

import pandas as pd

from funcoes.utilitarios import loga_mensagem, loga_mensagem_erro, baixa_csv
from persistencia.Oraprd import Oraprd


class CCEEClasse:
    etapaProcess = f"class {__name__} - class CCEEClasse."
    # loga_mensagem(etapaProcess)

    def __init__(self):
        var = None

    def busca_inscricoes_ccee(self, p_data_inicio, p_data_fim):
        etapaProcess = f"class {self.__class__.__name__} - def busca_inscricoes_ccee para o período de {p_data_inicio} a {p_data_fim}."
        # loga_mensagem(etapaProcess)

        try:
            db = Oraprd()
            df_ccee = db.select_inscricoes_ccee(p_data_inicio, p_data_fim)
            del db
            return df_ccee

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def carregar_ccee(self, df) -> str:
        etapaProcess = f"class {self.__class__.__name__} - def carregar_ccee."
        # loga_mensagem(etapaProcess)

        dfs = []
        i = 0
        linhas_alteradas = 0
        linhas_inseridas = 0

        try:
            db = Oraprd()
            df_ccee = db.select_inscricoes_ccee(df['numr_inscricao'].tolist())
            if len(df_ccee) > 0:
                # Identifica alterações nas vigencias existentes
                merged_df = df_ccee.merge(df, on=['numr_inscricao'], how='outer')

                # Filtra as datas de inicio de vigencia maior que as novas datas de inicio de vigencia -- Situação de ERRO
                filtro1 = merged_df['data_inicio_vigencia_ccee_x'] > merged_df['data_inicio_vigencia_ccee_y']
                if len(merged_df[filtro1]) > 0:
                    df_erro = merged_df[filtro1].copy()
                    df_erro.name = 'df_ccee_ini_vig'
                    baixa_csv(df_erro)
                    etapaProcess += f' - Existem inconsistências no arquivo CCEE. Verificar arquivo df_cnae_ini_vig.csv.'
                    loga_mensagem(etapaProcess)
                    return etapaProcess

                # Filtra as datas de fim de vigencia preenchida, mas com as novas datas de inicio de vigencia menores que as datas finais de vigencia existentes -- Situação de ERRO
                filtro2 = (~merged_df['data_fim_vigencia_ccee_x'].isna()) & (merged_df['data_fim_vigencia_ccee_x'] > merged_df['data_inicio_vigencia_ccee_y'])
                if len(merged_df[filtro2]) > 0:
                    df_erro = merged_df[filtro1].copy()
                    df_erro.name = 'df_ccee_fim_vig'
                    baixa_csv(df_erro)
                    etapaProcess += f' - Existem inconsistências no arquivo CCEE. Verificar arquivo df_cnae_fim_vig.csv.'
                    loga_mensagem(etapaProcess)
                    return etapaProcess

                # Filtra as datas de fim de vigencia vazias ou com o fim de vigencia menor que as novas datas de inicio de vigencia
                filtro = (merged_df['data_fim_vigencia_ccee_x'].isna()) | (merged_df['data_fim_vigencia_ccee_x'] <= merged_df['data_inicio_vigencia_ccee_y'])
                if len(merged_df[filtro]) > 0:
                    df = merged_df[filtro].copy()
                    for idx, row in df.iterrows():
                        # Encerra vigencia anterior - 1º teste: fechar vigencia, 2º teste: Excluir inscrição ainda não cadastrada
                        if pd.isna(row['data_fim_vigencia_ccee_x']) and not pd.isna(row['data_inicio_vigencia_ccee_x']):
                            if row['data_inicio_vigencia_ccee_x'] != row['data_inicio_vigencia_ccee_y']:
                                nova_linha = {'numr_inscricao': row['numr_inscricao'],
                                              'data_inicio_vigencia_ccee': row['data_inicio_vigencia_ccee_x'],
                                              'data_fim_vigencia_ccee': row['data_inicio_vigencia_ccee_y'] - timedelta(seconds=1),
                                              'tipo_contrib': row['tipo_contrib_x'],
                                              'tipo_operacao': 'A',
                                              }
                                dfs.append(pd.DataFrame(data=nova_linha, index=[i]))
                                i += 1
                                linhas_alteradas += 1

                        # Registra nova vigencia
                        if row['data_inicio_vigencia_ccee_x'] != row['data_inicio_vigencia_ccee_y']:
                            nova_linha = {'numr_inscricao': row['numr_inscricao'],
                                          'data_inicio_vigencia_ccee': row['data_inicio_vigencia_ccee_y'],
                                          'data_fim_vigencia_ccee': row['data_fim_vigencia_ccee_y'],
                                          'tipo_contrib': row['tipo_contrib_y'],
                                          'tipo_operacao': 'I',
                                          }
                            dfs.append(pd.DataFrame(data=nova_linha, index=[i]))
                            i += 1
                            linhas_inseridas += 1
            else:
                # Tratamento para a 1ª inclusão na tabela
                for idx, row in df.iterrows():
                    # Registra nova vigencia
                    nova_linha = {'numr_inscricao': row['numr_inscricao'],
                                  'data_inicio_vigencia_ccee': row['data_inicio_vigencia'],
                                  'data_fim_vigencia_ccee': row['data_fim_vigencia'],
                                  'tipo_contrib': row['tipo_contrib'],
                                  'tipo_operacao': 'I',
                                  }
                    dfs.append(pd.DataFrame(data=nova_linha, index=[i]))
                    i += 1
                    linhas_inseridas += 1

            if len(dfs) > 0:
                df_ccee_novo = pd.concat(dfs, ignore_index=True)
                del dfs

                etapaProcess += f' Qdade de linhas a inserir: {linhas_inseridas} - Linhas alteradas : {linhas_alteradas}'
                qdade_erro = db.insert_inscricoes_ccee(df_ccee_novo)
                if qdade_erro > 0:
                    etapaProcess += f' - Linhas com ERRO: {qdade_erro}'
            else:
                etapaProcess += f' - Não foram selecionadas inscições para inclusão no cadastro CCEE'

            return etapaProcess

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise

    def excluir_ccee(self) -> str:
        etapaProcess = f"class {self.__class__.__name__} - def excluir_ccee."
        # loga_mensagem(etapaProcess)

        linhas_excluidas = 0

        try:
            db = Oraprd()
            linhas_excluidas = db.delete_inscricoes_ccee()
            del db

            etapaProcess += f' - Exclusão de linhas existentes - {linhas_excluidas} excluidas.'
            loga_mensagem(etapaProcess)
            return etapaProcess

        except Exception as err:
            etapaProcess += " - ERRO - " + str(err)
            loga_mensagem_erro(etapaProcess)
            raise
