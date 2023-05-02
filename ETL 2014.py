import pandas as pd 
import psycopg2
from datetime import datetime
import glob
import os
import numpy as np 
#Funções 

def transform(linhaAtual, tipodacoluna):
    counter = 0
    
    if tipodacoluna == 1:  
        for columnName in linhaAtual.index:
            if counter == 14:
                break
            elif type(linhaAtual[columnName]) != np.int64 and type(linhaAtual[columnName]) != float and type(linhaAtual[columnName]) != str:
                print(linhaAtual[columnName] ,type(linhaAtual[columnName]))
            elif columnName == "NU_DOC_ESTRANGEIRO":
                print(linhaAtual[columnName] ,type(linhaAtual[columnName]))
            print(linhaAtual[columnName] ,type(linhaAtual[columnName]))

def print_row(linha):
    print('----------')
    print(f'NOME_ALUNO: {linha["NOME_ALUNO"]} \n, NOME_MAE_ALUNO: {linha["NOME_MAE_ALUNO"]} \n, NU_CPF: {linha["NU_CPF"]} \n, NU_DOC_ESTRANGEIRO: {linha["NU_DOC_ESTRANGEIRO"]} \n,  DATA_NASCIMENTO: {linha["DATA_NASCIMENTO"]} \n ,COR_RACA: {linha["COR_RACA"]} \n, SEXO: {linha["SEXO"]} \n,  ALUNO_DEFICIENCIA: {linha["ALUNO_DEFICIENCIA"]} \n, NO_NECESSIDADE_ESPECIAL: {linha["NO_NECESSIDADE_ESPECIAL"]} \n, COD_NACIONALIDADE: {linha["COD_NACIONALIDADE"]} \n,  NOME_NACIONALIDADE: {linha["NOME_NACIONALIDADE"]} \n, COD_PAIS_ORIGEM: {linha["COD_PAIS_ORIGEM"]} \n, NOME_PAIS_ORIGEM: {linha["NOME_PAIS_ORIGEM"]} \n, COD_UF_NASCIMENTO: int({linha["COD_UF_NASCIMENTO"]} \n),  COD_UF_NASCIMENTO: {linha["COD_UF_NASCIMENTO"]} \n, NOME_UF_NASCIMENTO: {linha["NOME_UF_NASCIMENTO"]} \n, COD_MUNIC_NASCIMENTO: {linha["COD_MUNIC_NASCIMENTO"]} \n, NOME_MUNIC_NASCIMENTO: {linha["NOME_MUNIC_NASCIMENTO"]}')

arquivo = 'Aluno 2014.xlsx'

dictRef = pd.read_csv("DEPLAG 2021.csv", low_memory=False)

df = pd.read_excel(arquivo, sheet_name= 0)

print("Carreguei DF")

# Define a conexão com o banco de dados
conn = psycopg2.connect(
    host="localhost",
    port="32771",
    database="postgres",
    user="postgres",
    password="docker"
)


#criar coluna nome Curso
codNomeCurso = {}

for codCurso in dictRef["CODIGO_CURSO"]:
    if codCurso not in codNomeCurso:
        codNomeCurso[codCurso] =  dictRef.loc[dictRef["CODIGO_CURSO"] == codCurso, "NOME_CURSO"].iloc[0]

codNomeCurso[13603] = "Letras"

codNomeCurso[313603] = "Letras"

codNomeCurso[322461] = "Letras"

codNomeCurso[13578] = "Geografia"

codNomeCurso[46078] = "Ciências Sociais"

codNomeCurso[13613] = "Desenho e Plástica"

codNomeCurso[150192] = "Educação Intercultural"

df["NOME_CURSO"] = df["q3"].map(codNomeCurso) 




#Criar a coluna Deficiencia

Deficiencias = ["Cegueira", "Baixa visão", "Surdez", "auditiva", "física", "Surdocegueira", "Múltipla", "Intelectual", "Autismo", "Síndrome de Asperger", "Síndrome de RETT", "Transtorno desintegrativo da infância", "Superdotação"]

counterColumn = 16

for deficiencia in Deficiencias:
    counterColumn = str(counterColumn)
    df.loc[df['q'+counterColumn] == 1, "NO_NECESSIDADE_ESPECIAL"] = deficiencia
    counterColumn = int(counterColumn) + 1

#mapear quantas linhas a pessoa tem

idIndexDict = {}
indexAnterior = 1
quantidadeDeLinhas = 0

for Aluno in df['id']:
    if Aluno == indexAnterior:
        quantidadeDeLinhas += 1
        idIndexDict[Aluno] = quantidadeDeLinhas
        
    else:
        quantidadeDeLinhas = 1
        idIndexDict[Aluno] = quantidadeDeLinhas
        indexAnterior += 1

#1º estado são os dados do aluno
#2º estado é o período de referência 1
#3º estado é o período de referência 2
#3º estado é o período de referência 1

primeiraVezRodando = True

co = 1
co2 = 1
for idAluno in idIndexDict:
    qtdDeCiclos = idIndexDict[idAluno]
    inserirDados = 1
    
    while inserirDados <= qtdDeCiclos:
        
        if inserirDados == 1: #Linha ALUNO


        #TRANSFORMAR AS COLUNAS

            #Renomear

            if primeiraVezRodando:

                nomesColunasAluno = {"q2": "ID_ALUNO","q4": 'NOME_ALUNO', "q5": 'NU_CPF', "q6": 'NU_DOC_ESTRANGEIRO', "q7": 'DATA_NASCIMENTO', "q8": 'SEXO', "q9": 'COR_RACA', "q10":'NOME_MAE_ALUNO', 'q11': 'COD_NACIONALIDADE', 'q12': 'COD_UF_NASCIMENTO', 'q13': 'COD_MUNIC_NASCIMENTO', 'q14': 'NOME_PAIS_ORIGEM', 'q15': 'ALUNO_DEFICIENCIA'}
                primeiraVezRodando = False
                
            else:
                nomesColunasAluno = {"PERIODO_REFERENCIA": "ID_ALUNO", "TURNO": 'NU_CPF', "SITUACAO_VINCULO": 'NU_DOC_ESTRANGEIRO', "SEMESTRE_CONCLUSAO": 'SEXO', "ALUNO_PARFOR": 'COR_RACA', "SEMESTRE_INGRESSO":'NOME_MAE_ALUNO', "TP_ESCOLA_ENS_MEDIO": 'COD_NACIONALIDADE', "INVESTIBULAR": 'COD_UF_NASCIMENTO', "INENEM": 'COD_MUNIC_NASCIMENTO', "INAVALIACAOSERIADA": 'NOME_PAIS_ORIGEM', "INSELECAOSIMPLIFICADA": 'ALUNO_DEFICIENCIA'}


            df = df.rename(columns=nomesColunasAluno)
            


            linhaAlunoAtual = df.loc[df['id'] == idAluno, :].iloc[0]
            linhaAlunoAtual["DATA_NASCIMENTO"] = int(linhaAlunoAtual["DATA_NASCIMENTO"])
            linhaAlunoAtual["DATA_NASCIMENTO"] = str(linhaAlunoAtual["DATA_NASCIMENTO"])
            linhaAlunoAtual["NU_CPF"] = int(linhaAlunoAtual["NU_CPF"])
            linhaAlunoAtual["NU_CPF"] = str(linhaAlunoAtual["NU_CPF"])


            for i in range(len(linhaAlunoAtual)):
                if pd.isna(linhaAlunoAtual[i]):
                    linhaAlunoAtual[i] = 0
                elif type(linhaAlunoAtual[i]) == str:
                    pass
                elif type(linhaAlunoAtual[i]) == int or type(linhaAlunoAtual[i]) == np.int32 or type(linhaAlunoAtual[i]) == np.int64 or type(linhaAlunoAtual[i]) == float or type(linhaAlunoAtual[i]) == np.float64 or type(linhaAlunoAtual[i]) == np.float32:
                    linhaAlunoAtual[i] = int(linhaAlunoAtual[i])
                else:
                    linhaAlunoAtual[i] = 0

                 
            #Deixando todos os cpfs com 11 dígitos
            
            numeroCpfSemZero = linhaAlunoAtual['NU_CPF']
            
            

            while len(linhaAlunoAtual['NU_CPF']) < 11:
                linhaAlunoAtual['NU_CPF'] = '0' + linhaAlunoAtual['NU_CPF']

               
   
            
            # Obtém o nome do arquivo
            nome_arquivo = os.path.split(arquivo)[-1] 
            #print(nome_arquivo)

            # Obtém o ano 
            ano_referencia = 2014
            #print(ano_referencia) 

            # Obtém o timestamp atual
            timestamp = datetime.now()
            #print(timestamp)  

            # Tabela LOTE - executa a consulta parametrizada
            with conn.cursor() as cur:
                comando = "INSERT INTO LOTE (nome_arquivo, ano_referencia, timestamp) VALUES (%s, %s, %s)"
                dados = (nome_arquivo, ano_referencia, timestamp)
                cur.execute(comando, dados)
                
            
            #Tabela IES 
                comando = """INSERT INTO IES(CODIGO_IES,NOME_IES)
                            VALUES (%s, %s)
                            ON CONFLICT (CODIGO_IES) DO NOTHING;
                            """
                cur.execute(comando,(580, "Universidade Federal de Pernambuco"))

                conn.commit()

                
        # Tabela ALUNO

        # Se o aluno já existe no banco de dados, atualiza seus dados. Caso contrário, insere um novo registro.
            with conn.cursor() as cur:
                linhaAlunoAtual['NU_CPF'] = str(linhaAlunoAtual['NU_CPF'])
                query = "SELECT NU_CPF FROM ALUNO WHERE NU_CPF = %s"
                cur.execute(query, (linhaAlunoAtual['NU_CPF'],))
                results = cur.fetchall()
                
            
            #Arrumando a data de nascimento das planilhas antigas 
                                        
            dataDeNascimento = str(linhaAlunoAtual["DATA_NASCIMENTO"])

            if len(dataDeNascimento) < 8:
                dataDeNascimento = f'0{dataDeNascimento[0]}/{dataDeNascimento[1:3]}/{dataDeNascimento[3:7]} 00:00:00'
            else:
                dataDeNascimento = f'{dataDeNascimento[0:2]}/{dataDeNascimento[2:4]}/{dataDeNascimento[4:8]} 00:00:00'
            
            if(len(dataDeNascimento) == 1):
                dataDeNascimento = '00/00/00 00:00:00'


            linhaAlunoAtual['DATA_NASCIMENTO'] = datetime.strptime(dataDeNascimento, '%d/%m/%Y %H:%M:%S')
            
            #Atualiza os dados
            
            nuCpf = linhaAlunoAtual['NU_CPF']
            corAluno = linhaAlunoAtual['COR_RACA']
            
            if results: 
                for result in results:

                    with conn.cursor() as cur:
                        comando = "UPDATE aluno SET nome_aluno = %s, nome_mae_aluno = %s, data_nascimento = %s, cor_raca = %s, sexo = %s, aluno_deficiencia = %s, no_necessidade_especial = %s, cod_nacionalidade = %s,  nome_pais_origem = %s, cod_uf_nascimento = %s,  cod_munic_nascimento =%s WHERE nu_cpf = %s"
                        cur.execute(comando, (
                            linhaAlunoAtual['NOME_ALUNO'], linhaAlunoAtual['NOME_MAE_ALUNO'], linhaAlunoAtual['DATA_NASCIMENTO'], linhaAlunoAtual['COR_RACA'], linhaAlunoAtual['SEXO'], linhaAlunoAtual['ALUNO_DEFICIENCIA'],
                            linhaAlunoAtual['NO_NECESSIDADE_ESPECIAL'], linhaAlunoAtual['COD_NACIONALIDADE'], linhaAlunoAtual['NOME_PAIS_ORIGEM'],
                            int(linhaAlunoAtual['COD_UF_NASCIMENTO']) if pd.notnull(linhaAlunoAtual['COD_UF_NASCIMENTO']) else 0,  linhaAlunoAtual['COD_MUNIC_NASCIMENTO'],
                             result[0]
                            ))
                        
                        conn.commit()
                        
                        print("Atualizei uma pessoa", co2)
                        co2 += 1
                #Insere novos dados 
            else:
                with conn.cursor() as cur:           #TIREI O NOME NACIONALIDADE    COD_PAIS_ORIGEM      NOME_UF_NASCIMENTO  NOME_MUNIC_NASCIMENTO        
                    comando = """INSERT INTO ALUNO (NOME_ALUNO, NOME_MAE_ALUNO, NU_CPF, NU_DOC_ESTRANGEIRO, DATA_NASCIMENTO, COR_RACA, SEXO,
                        ALUNO_DEFICIENCIA, NO_NECESSIDADE_ESPECIAL, COD_NACIONALIDADE,
                        NOME_PAIS_ORIGEM, COD_UF_NASCIMENTO, COD_MUNIC_NASCIMENTO)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                    
                    cur.execute(comando, (
                        linhaAlunoAtual['NOME_ALUNO'], linhaAlunoAtual['NOME_MAE_ALUNO'], linhaAlunoAtual['NU_CPF'], linhaAlunoAtual['NU_DOC_ESTRANGEIRO'], linhaAlunoAtual['DATA_NASCIMENTO'],
                        linhaAlunoAtual['COR_RACA'], linhaAlunoAtual['SEXO'], int(linhaAlunoAtual['ALUNO_DEFICIENCIA']), linhaAlunoAtual['NO_NECESSIDADE_ESPECIAL'], int(linhaAlunoAtual['COD_NACIONALIDADE']),
                        linhaAlunoAtual['NOME_PAIS_ORIGEM'], int(linhaAlunoAtual['COD_UF_NASCIMENTO']) if pd.notnull(int(linhaAlunoAtual['COD_UF_NASCIMENTO'])) else 0,
                        linhaAlunoAtual['COD_MUNIC_NASCIMENTO']
                    ))

                    conn.commit()

                print("Inseriu nova pessoa", co)
                co += 1

            

        elif inserirDados == 2:
            #insere os dados Período referência 1
            
            #Renomear as colunas

            nomesColunasAluno = {"ID_ALUNO": "PERIODO_REFERENCIA",'q3':'CODIGO_CURSO',   'NU_CPF': "TURNO",  'NU_DOC_ESTRANGEIRO': "SITUACAO_VINCULO", 'SEXO': "SEMESTRE_CONCLUSAO",  'COR_RACA': "ALUNO_PARFOR", 'NOME_MAE_ALUNO': "SEMESTRE_INGRESSO",  'COD_NACIONALIDADE': "TP_ESCOLA_ENS_MEDIO",  'COD_UF_NASCIMENTO': "INVESTIBULAR",  'COD_MUNIC_NASCIMENTO': "INENEM",  'NOME_PAIS_ORIGEM': "INAVALIACAOSERIADA",  'ALUNO_DEFICIENCIA': "INSELECAOSIMPLIFICADA", "q16": "INCONVENIOPECG", "q17":"INTRANFERENCIAEXOFFICIO", "q18": "INDECISAOJUDICIAL", "q19": "INSELECAOREMANESCENTE", "q20": "INSELECAOPROGRAMAESPECIAL",  "q21": "MOBILIDADE_ACADEMICA", "q22": "MOB_ACAD_NACIONAL_MOB_ACAD_INTERNACIONAL", "q23": "MOB_ACAD_IESDESTINO", "q24": "TIPOMOBILIDADE000000", "q25": "MOB_ACAD_PAISDESTINO", 'q26':'PROG_RESERVA_VAGA', 'q27':'RESVAGA_ETNICO', 'q28':'RESVAGA_PESSOADEFIC', 'q29':'RESVAGA_ESTUDPROCEDEESCPUB', 'q30':'RESVAGA_RENDAFAM', 'q31':'RESVAGA_OUTRO', 'q44': 'ALUNO_APOIO_SOCIAL', 'q45': 'APOIOSOCIAL_ALIMENTACAO', 'q46': 'APOIOSOCIAL_MORADIA', 'q47': 'APOIOSOCIAL_TRANSPORTE', 'q48': 'APOIOSOCIAL_MATLDIDATICO', 'q49': 'APOIOSOCIAL_BOLSATRABALHO', 'q50': 'APOIOSOCIAL_BOLSAPERMANEN', 'q51': 'ATIV_EXTRACURRICULAR', 'q52': 'ATIVEXTRA_PESQUISA', 'q53': 'BOLSAREMUN_PESQUISA', 'q54': 'ATIVEXTRA_EXTENSAO', 'q55': 'BOLSAREMUN_EXTENSAO', 'q56': 'ATIVEXTRA_MONITORIA', 'q57': 'BOLSAREMUN_MONITORIA', 'q58': 'ATIVEXTRA_ESTAGIO', 'q59': 'BOLSAREMUN_ESTAGIO', 'q60': 'CARGA_HORARIA_TOTAL', 'q61': 'CARGA_HORARIA_INTEGRALIZADA'}


            df = df.rename(columns=nomesColunasAluno)

            linhaAlunoAtual = df.loc[df['id'] == idAluno, :].iloc[1]
            
            # FALTA NOME_POLO
            periodoReferencia = linhaAlunoAtual['PERIODO_REFERENCIA']
            semestreIngresso = linhaAlunoAtual['SEMESTRE_INGRESSO']
            semestreIngresso = str(semestreIngresso)
            semestreIngresso = semestreIngresso[0] + 'º/' + semestreIngresso[1:5]
            
            for i in range(len(linhaAlunoAtual)):
                if pd.isna(linhaAlunoAtual[i]):
                    linhaAlunoAtual[i] = 0
                elif type(linhaAlunoAtual[i]) == str:
                    pass
                elif type(linhaAlunoAtual[i]) == int or type(linhaAlunoAtual[i]) == np.int32 or type(linhaAlunoAtual[i]) == np.int64 or type(linhaAlunoAtual[i]) == float or type(linhaAlunoAtual[i]) == np.float64 or type(linhaAlunoAtual[i]) == np.float32:
                    linhaAlunoAtual[i] = int(linhaAlunoAtual[i])
                else:
                    linhaAlunoAtual[i] = 0
                
            #Tabela curso 

            
            #Falta Modalidade Ensino Grau ACADEMICO NÍVEL ACADEMICO
            with conn.cursor() as cur:
                comando = """INSERT INTO CURSO(CODIGO_CURSO, NOME_CURSO, GRAU_ACADEMICO, NIVEL_ACADEMICO, MODALIDADE_ENSINO)
                        VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (CODIGO_CURSO) DO NOTHING;
                        """
                cur.execute(comando,(linhaAlunoAtual['CODIGO_CURSO'], linhaAlunoAtual['NOME_CURSO'], 0, 0, 0))

                conn.commit()

            #tabela VINCULO PERIODO REFERENCIA 1
            with conn.cursor() as cur:            
                comando = """INSERT INTO VINCULO (cod_curso, nu_cpf, situacao_vinculo, cod_polo, nome_polo, turno, semestre_ingresso, periodo_referencia, semestre_conclusao, 
                        tp_escola_ens_medio, id_aluno_ies, carga_horaria_total, carga_horaria_integralizada,investibular, inenem, inavaliacaoseriada, inselecaosimplificada, 
                        inconveniopecg, indecisaojudicial,  inselecaoprogramaespecial, inselecaoremanescente, intranferenciaexofficio, aluno_parfor, formacao_pedag, 
                        segunda_licenc) VALUES (%s, %s, %s,  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING vinculo_id"""
                


                
                cur.execute(comando, (linhaAlunoAtual['CODIGO_CURSO'],nuCpf,linhaAlunoAtual['SITUACAO_VINCULO'], 0, "Não informado", linhaAlunoAtual['TURNO'],semestreIngresso,
                                    str(ano_referencia)+"."+str(periodoReferencia), linhaAlunoAtual['SEMESTRE_CONCLUSAO'], linhaAlunoAtual['TP_ESCOLA_ENS_MEDIO'], 0, 0,
                                    0, linhaAlunoAtual['INVESTIBULAR'], linhaAlunoAtual['INENEM'], linhaAlunoAtual['INAVALIACAOSERIADA'], linhaAlunoAtual['INSELECAOSIMPLIFICADA'],
                                    linhaAlunoAtual['INCONVENIOPECG'], linhaAlunoAtual['INDECISAOJUDICIAL'], linhaAlunoAtual['INSELECAOPROGRAMAESPECIAL'], linhaAlunoAtual['INSELECAOREMANESCENTE'], 
                                    linhaAlunoAtual['INTRANFERENCIAEXOFFICIO'], linhaAlunoAtual['ALUNO_PARFOR'], 0, 0))
                id_of_new_row = cur.fetchone()[0]
                conn.commit()
                apoiosocial_alimentacao = 0
                apoiosocial_per = 0
                apoiosocial_maltl = 0
                apoiosocial_morad = 0
                apoiosocial_trans = 0
                if linhaAlunoAtual['ALUNO_APOIO_SOCIAL'] == 1:
                    if linhaAlunoAtual['APOIOSOCIAL_ALIMENTACAO'] == 1:
                        apoiosocial_alimentacao = 1 
                    if linhaAlunoAtual['APOIOSOCIAL_BOLSAPERMANEN'] == 1:
                        apoiosocial_per = 2
                    if linhaAlunoAtual['APOIOSOCIAL_MATLDIDATICO'] == 1:
                        apoiosocial_maltl = 3
                    if linhaAlunoAtual['APOIOSOCIAL_MORADIA'] == 1:
                        apoiosocial_morad = 4
                    if linhaAlunoAtual['APOIOSOCIAL_TRANSPORTE'] == 1:
                        apoiosocial_trans = 5
                arrayApoio = [apoiosocial_trans, apoiosocial_alimentacao, apoiosocial_maltl, apoiosocial_morad, apoiosocial_per]
                
                for apoio in arrayApoio:
                    if apoio != 0:
                        with conn.cursor() as cur:
                            comando = """ insert into vinculo_apoio_social (vinculo_id, apoio_social_id ) values(%s, %s)"""
                            
                            cur.execute(comando, (id_of_new_row, apoio))

                            conn.commit()

                ativextra_estagio = 0
                bolsaremun_estagio = 0
                ativextra_extensao = 0
                bolsaremun_extensao = 0
                ativextra_monitoria = 0
                bolsaremun_monitoria = 0
                ativextra_pesquisa = 0
                bolsaremun_pesquisa = 0

                if linhaAlunoAtual['ATIV_EXTRACURRICULAR'] == 1:
                    if linhaAlunoAtual['ATIVEXTRA_ESTAGIO'] == 1:
                        ativextra_estagio = 1 
                    if linhaAlunoAtual['BOLSAREMUN_ESTAGIO'] == 1:
                        bolsaremun_estagio = 2
                    if linhaAlunoAtual['ATIVEXTRA_EXTENSAO'] == 1:
                        ativextra_extensao = 3
                    if linhaAlunoAtual['BOLSAREMUN_EXTENSAO'] == 1:
                        bolsaremun_extensao = 4
                    if linhaAlunoAtual['ATIVEXTRA_MONITORIA'] == 1:
                        ativextra_monitoria = 5
                    if linhaAlunoAtual['BOLSAREMUN_MONITORIA'] == 1:
                        bolsaremun_monitoria = 6
                    if linhaAlunoAtual['ATIVEXTRA_PESQUISA']== 1:
                        ativextra_pesquisa = 7
                    if linhaAlunoAtual['BOLSAREMUN_PESQUISA'] == 1:
                        bolsaremun_pesquisa = 8
                arrayAtiv = [ativextra_estagio, bolsaremun_estagio,ativextra_extensao,bolsaremun_extensao,ativextra_monitoria,bolsaremun_monitoria,ativextra_pesquisa,bolsaremun_pesquisa]
                
                for ativ in arrayAtiv:
                    if ativ != 0:
                        with conn.cursor() as cur:
                            comando = """ insert into vinculo_ativ_extracur (vinculo_id, ativ_extracurricular_id ) values(%s, %s)"""
                            
                            cur.execute(comando, (id_of_new_row, ativ))

                            conn.commit()

                resvaga_estudprocedeescpub = 0
                resvaga_etnico = 0
                resvaga_pessoadefic = 0
                resvaga_rendafam = 0
                resvaga_outro = 0

                if linhaAlunoAtual['PROG_RESERVA_VAGA'] == 1:
                    if linhaAlunoAtual['RESVAGA_ESTUDPROCEDEESCPUB'] == 1:
                        resvaga_estudprocedeescpub = 1 
                    if linhaAlunoAtual['RESVAGA_ETNICO'] == 1:
                        resvaga_etnico = 2
                    if linhaAlunoAtual['RESVAGA_PESSOADEFIC'] == 1:
                        resvaga_pessoadefic = 3
                    if linhaAlunoAtual['RESVAGA_RENDAFAM'] == 1:
                        resvaga_rendafam = 4
                    if linhaAlunoAtual['RESVAGA_OUTRO'] == 1:
                        resvaga_outro = 5
                arrayvaga = [resvaga_estudprocedeescpub, resvaga_etnico,resvaga_pessoadefic,resvaga_rendafam,resvaga_outro]
                
                for vaga in arrayvaga:
                    if vaga != 0:
                        with conn.cursor() as cur:
                            comando = """ insert into vinculo_reserva_vaga (vinculo_id, prog_reserva_vaga_id ) values(%s, %s)"""
                            
                            cur.execute(comando, (id_of_new_row, vaga))

                            conn.commit()



                Nao_quis_declarar = 0
                Branca = 0
                Preta = 0
                Parda = 0
                Amarela = 0
                Indigena = 0
                Não_dispoe_da_informacao = 0

                if corAluno == 0:
                    Nao_quis_declarar = 0 
                if corAluno == 1:
                    Branca = 1
                if corAluno == 2:
                    Preta = 2
                if corAluno == 3:
                    Parda = 3
                if corAluno == 4:
                    Amarela = 4
                if corAluno == 5:
                    Indigena = 5
                if corAluno == 6:
                    Não_dispoe_da_informacao = 6
                        
                arrayraca = [Nao_quis_declarar, Branca,Preta,Parda,Amarela,Indigena,Não_dispoe_da_informacao]
                
                for raca in arrayraca:
                    if raca != 0:
                        with conn.cursor() as cur:
                            comando = """ insert into vinculo_cor_raca (vinculo_id, raca_id ) values(%s, %s)"""
                            
                            cur.execute(comando, (id_of_new_row, raca))
                            conn.commit()
                
                

        elif inserirDados == 3:
            #insere os dados Período referência 2
            linhaAlunoAtual = df.loc[df['id'] == idAluno, :].iloc[2]
            
            #Renomear as colunas

            nomesColunasAluno = {"ID_ALUNO": "PERIODO_REFERENCIA",'q3':'CODIGO_CURSO',   'NU_CPF': "TURNO",  'NU_DOC_ESTRANGEIRO': "SITUACAO_VINCULO", 'SEXO': "SEMESTRE_CONCLUSAO",  'COR_RACA': "ALUNO_PARFOR", 'NOME_MAE_ALUNO': "SEMESTRE_INGRESSO",  'COD_NACIONALIDADE': "TP_ESCOLA_ENS_MEDIO",  'COD_UF_NASCIMENTO': "INVESTIBULAR",  'COD_MUNIC_NASCIMENTO': "INENEM",  'NOME_PAIS_ORIGEM': "INAVALIACAOSERIADA",  'ALUNO_DEFICIENCIA': "INSELECAOSIMPLIFICADA", "q16": "INCONVENIOPECG", "q17":"INTRANFERENCIAEXOFFICIO", "q18": "INDECISAOJUDICIAL", "q19": "INSELECAOREMANESCENTE", "q20": "INSELECAOPROGRAMAESPECIAL",  "q21": "MOBILIDADE_ACADEMICA", "q22": "MOB_ACAD_NACIONAL_MOB_ACAD_INTERNACIONAL", "q23": "MOB_ACAD_IESDESTINO", "q24": "TIPOMOBILIDADE000000", "q25": "MOB_ACAD_PAISDESTINO", 'q26':'PROG_RESERVA_VAGA', 'q27':'RESVAGA_ETNICO', 'q28':'RESVAGA_PESSOADEFIC', 'q29':'RESVAGA_ESTUDPROCEDEESCPUB', 'q30':'RESVAGA_RENDAFAM', 'q31':'RESVAGA_OUTRO', 'q44': 'ALUNO_APOIO_SOCIAL', 'q45': 'APOIOSOCIAL_ALIMENTACAO', 'q46': 'APOIOSOCIAL_MORADIA', 'q47': 'APOIOSOCIAL_TRANSPORTE', 'q48': 'APOIOSOCIAL_MATLDIDATICO', 'q49': 'APOIOSOCIAL_BOLSATRABALHO', 'q50': 'APOIOSOCIAL_BOLSAPERMANEN', 'q51': 'ATIV_EXTRACURRICULAR', 'q52': 'ATIVEXTRA_PESQUISA', 'q53': 'BOLSAREMUN_PESQUISA', 'q54': 'ATIVEXTRA_EXTENSAO', 'q55': 'BOLSAREMUN_EXTENSAO', 'q56': 'ATIVEXTRA_MONITORIA', 'q57': 'BOLSAREMUN_MONITORIA', 'q58': 'ATIVEXTRA_ESTAGIO', 'q59': 'BOLSAREMUN_ESTAGIO', 'q60': 'CARGA_HORARIA_TOTAL', 'q61': 'CARGA_HORARIA_INTEGRALIZADA'}


            df = df.rename(columns=nomesColunasAluno)
            
            periodoReferencia = linhaAlunoAtual['PERIODO_REFERENCIA']

            semestreIngresso = linhaAlunoAtual['SEMESTRE_INGRESSO']
            semestreIngresso = str(semestreIngresso)
            semestreIngresso = semestreIngresso[0] + 'º/' + semestreIngresso[1:5]

            
            for i in range(len(linhaAlunoAtual)):
                if pd.isna(linhaAlunoAtual[i]):
                    linhaAlunoAtual[i] = 0
                elif type(linhaAlunoAtual[i]) == str:
                    pass
                elif type(linhaAlunoAtual[i]) == int or type(linhaAlunoAtual[i]) == np.int32 or type(linhaAlunoAtual[i]) == np.int64 or type(linhaAlunoAtual[i]) == float or type(linhaAlunoAtual[i]) == np.float64 or type(linhaAlunoAtual[i]) == np.float32:
                    linhaAlunoAtual[i] = int(linhaAlunoAtual[i])
                else:
                    linhaAlunoAtual[i] = 0
                

            #Tabela curso 
            
            #Falta Modalidade Ensino Grau ACADEMICO NÍVEL ACADEMICO
            with conn.cursor() as cur:
                comando = """INSERT INTO CURSO(CODIGO_CURSO, NOME_CURSO, GRAU_ACADEMICO, NIVEL_ACADEMICO, MODALIDADE_ENSINO)
                        VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (CODIGO_CURSO) DO NOTHING;
                        """
                cur.execute(comando,(linhaAlunoAtual['CODIGO_CURSO'], linhaAlunoAtual['NOME_CURSO'], 0, 0, 0))

                conn.commit()
                

            #tabela VINCULO PERIODO REFERENCIA 1
            with conn.cursor() as cur:            
                comando = """INSERT INTO VINCULO (cod_curso, nu_cpf, situacao_vinculo, cod_polo, nome_polo, turno, semestre_ingresso, periodo_referencia, semestre_conclusao, 
                        tp_escola_ens_medio, id_aluno_ies, carga_horaria_total, carga_horaria_integralizada,investibular, inenem, inavaliacaoseriada, inselecaosimplificada, 
                        inconveniopecg, indecisaojudicial, inselecaoprogramaespecial, inselecaoremanescente, intranferenciaexofficio, aluno_parfor, formacao_pedag, 
                        segunda_licenc) VALUES (%s, %s, %s,  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING vinculo_id"""
                
                
                cur.execute(comando, (linhaAlunoAtual['CODIGO_CURSO'],nuCpf,linhaAlunoAtual['SITUACAO_VINCULO'], 0, "Não informado", linhaAlunoAtual['TURNO'],semestreIngresso,
                                    str(ano_referencia)+"."+str(periodoReferencia), linhaAlunoAtual['SEMESTRE_CONCLUSAO'], linhaAlunoAtual['TP_ESCOLA_ENS_MEDIO'], 0, 0,
                                    0, linhaAlunoAtual['INVESTIBULAR'], linhaAlunoAtual['INENEM'], linhaAlunoAtual['INAVALIACAOSERIADA'], linhaAlunoAtual['INSELECAOSIMPLIFICADA'],
                                    linhaAlunoAtual['INCONVENIOPECG'], linhaAlunoAtual['INDECISAOJUDICIAL'], linhaAlunoAtual['INSELECAOPROGRAMAESPECIAL'], linhaAlunoAtual['INSELECAOREMANESCENTE'], 
                                    linhaAlunoAtual['INTRANFERENCIAEXOFFICIO'], linhaAlunoAtual['ALUNO_PARFOR'], 0, 0))
                id_of_new_row = cur.fetchone()[0]
                conn.commit()
                apoiosocial_alimentacao = 0
                apoiosocial_per = 0
                apoiosocial_maltl = 0
                apoiosocial_morad = 0
                apoiosocial_trans = 0
                if linhaAlunoAtual['ALUNO_APOIO_SOCIAL'] == 1:
                    if linhaAlunoAtual['APOIOSOCIAL_ALIMENTACAO'] == 1:
                        apoiosocial_alimentacao = 1 
                    if linhaAlunoAtual['APOIOSOCIAL_BOLSAPERMANEN'] == 1:
                        apoiosocial_per = 2
                    if linhaAlunoAtual['APOIOSOCIAL_MATLDIDATICO'] == 1:
                        apoiosocial_maltl = 3
                    if linhaAlunoAtual['APOIOSOCIAL_MORADIA'] == 1:
                        apoiosocial_morad = 4
                    if linhaAlunoAtual['APOIOSOCIAL_TRANSPORTE'] == 1:
                        apoiosocial_trans = 5
                arrayApoio = [apoiosocial_trans, apoiosocial_alimentacao, apoiosocial_maltl, apoiosocial_morad, apoiosocial_per]
                
                for apoio in arrayApoio:
                    if apoio != 0:
                        with conn.cursor() as cur:
                            comando = """ insert into vinculo_apoio_social (vinculo_id, apoio_social_id ) values(%s, %s)"""
                            
                            cur.execute(comando, (id_of_new_row, apoio))

                            conn.commit()

                ativextra_estagio = 0
                bolsaremun_estagio = 0
                ativextra_extensao = 0
                bolsaremun_extensao = 0
                ativextra_monitoria = 0
                bolsaremun_monitoria = 0
                ativextra_pesquisa = 0
                bolsaremun_pesquisa = 0

                if linhaAlunoAtual['ATIV_EXTRACURRICULAR'] == 1:
                    if linhaAlunoAtual['ATIVEXTRA_ESTAGIO'] == 1:
                        ativextra_estagio = 1 
                    if linhaAlunoAtual['BOLSAREMUN_ESTAGIO'] == 1:
                        bolsaremun_estagio = 2
                    if linhaAlunoAtual['ATIVEXTRA_EXTENSAO'] == 1:
                        ativextra_extensao = 3
                    if linhaAlunoAtual['BOLSAREMUN_EXTENSAO'] == 1:
                        bolsaremun_extensao = 4
                    if linhaAlunoAtual['ATIVEXTRA_MONITORIA'] == 1:
                        ativextra_monitoria = 5
                    if linhaAlunoAtual['BOLSAREMUN_MONITORIA'] == 1:
                        bolsaremun_monitoria = 6
                    if linhaAlunoAtual['ATIVEXTRA_PESQUISA']== 1:
                        ativextra_pesquisa = 7
                    if linhaAlunoAtual['BOLSAREMUN_PESQUISA'] == 1:
                        bolsaremun_pesquisa = 8
                arrayAtiv = [ativextra_estagio, bolsaremun_estagio,ativextra_extensao,bolsaremun_extensao,ativextra_monitoria,bolsaremun_monitoria,ativextra_pesquisa,bolsaremun_pesquisa]
                
                for ativ in arrayAtiv:
                    if ativ != 0:
                        with conn.cursor() as cur:
                            comando = """ insert into vinculo_ativ_extracur (vinculo_id, ativ_extracurricular_id ) values(%s, %s)"""
                            
                            cur.execute(comando, (id_of_new_row, ativ))

                            conn.commit()

                resvaga_estudprocedeescpub = 0
                resvaga_etnico = 0
                resvaga_pessoadefic = 0
                resvaga_rendafam = 0
                resvaga_outro = 0

                if linhaAlunoAtual['PROG_RESERVA_VAGA'] == 1:
                    if linhaAlunoAtual['RESVAGA_ESTUDPROCEDEESCPUB'] == 1:
                        resvaga_estudprocedeescpub = 1 
                    if linhaAlunoAtual['RESVAGA_ETNICO'] == 1:
                        resvaga_etnico = 2
                    if linhaAlunoAtual['RESVAGA_PESSOADEFIC'] == 1:
                        resvaga_pessoadefic = 3
                    if linhaAlunoAtual['RESVAGA_RENDAFAM'] == 1:
                        resvaga_rendafam = 4
                    if linhaAlunoAtual['RESVAGA_OUTRO'] == 1:
                        resvaga_outro = 5
                arrayvaga = [resvaga_estudprocedeescpub, resvaga_etnico,resvaga_pessoadefic,resvaga_rendafam,resvaga_outro]
                
                for vaga in arrayvaga:
                    if vaga != 0:
                        with conn.cursor() as cur:
                            comando = """ insert into vinculo_reserva_vaga (vinculo_id, prog_reserva_vaga_id ) values(%s, %s)"""
                            
                            cur.execute(comando, (id_of_new_row, vaga))

                            conn.commit()



                Nao_quis_declarar = 0
                Branca = 0
                Preta = 0
                Parda = 0
                Amarela = 0
                Indigena = 0
                Não_dispoe_da_informacao = 0

                if corAluno == 0:
                    Nao_quis_declarar = 0 
                if corAluno == 1:
                    Branca = 1
                if corAluno == 2:
                    Preta = 2
                if corAluno == 3:
                    Parda = 3
                if corAluno == 4:
                    Amarela = 4
                if corAluno == 5:
                    Indigena = 5
                if corAluno == 6:
                    Não_dispoe_da_informacao = 6
                        
                arrayraca = [Nao_quis_declarar, Branca,Preta,Parda,Amarela,Indigena,Não_dispoe_da_informacao]
                
                for raca in arrayraca:
                    if raca != 0:
                        with conn.cursor() as cur:
                            comando = """ insert into vinculo_cor_raca (vinculo_id, raca_id ) values(%s, %s)"""
                            
                            cur.execute(comando, (id_of_new_row, raca))
                            conn.commit()
        elif inserirDados == 4:
            #insere os dados Período referência 2
            linhaAlunoAtual = df.loc[df['id'] == idAluno, :].iloc[3]
            
            #Renomear as colunas

            nomesColunasAluno = {"ID_ALUNO": "PERIODO_REFERENCIA",'q3':'CODIGO_CURSO',   'NU_CPF': "TURNO",  'NU_DOC_ESTRANGEIRO': "SITUACAO_VINCULO", 'SEXO': "SEMESTRE_CONCLUSAO",  'COR_RACA': "ALUNO_PARFOR", 'NOME_MAE_ALUNO': "SEMESTRE_INGRESSO",  'COD_NACIONALIDADE': "TP_ESCOLA_ENS_MEDIO",  'COD_UF_NASCIMENTO': "INVESTIBULAR",  'COD_MUNIC_NASCIMENTO': "INENEM",  'NOME_PAIS_ORIGEM': "INAVALIACAOSERIADA",  'ALUNO_DEFICIENCIA': "INSELECAOSIMPLIFICADA", "q16": "INCONVENIOPECG", "q17":"INTRANFERENCIAEXOFFICIO", "q18": "INDECISAOJUDICIAL", "q19": "INSELECAOREMANESCENTE", "q20": "INSELECAOPROGRAMAESPECIAL",  "q21": "MOBILIDADE_ACADEMICA", "q22": "MOB_ACAD_NACIONAL_MOB_ACAD_INTERNACIONAL", "q23": "MOB_ACAD_IESDESTINO", "q24": "TIPOMOBILIDADE000000", "q25": "MOB_ACAD_PAISDESTINO", 'q26':'PROG_RESERVA_VAGA', 'q27':'RESVAGA_ETNICO', 'q28':'RESVAGA_PESSOADEFIC', 'q29':'RESVAGA_ESTUDPROCEDEESCPUB', 'q30':'RESVAGA_RENDAFAM', 'q31':'RESVAGA_OUTRO', 'q44': 'ALUNO_APOIO_SOCIAL', 'q45': 'APOIOSOCIAL_ALIMENTACAO', 'q46': 'APOIOSOCIAL_MORADIA', 'q47': 'APOIOSOCIAL_TRANSPORTE', 'q48': 'APOIOSOCIAL_MATLDIDATICO', 'q49': 'APOIOSOCIAL_BOLSATRABALHO', 'q50': 'APOIOSOCIAL_BOLSAPERMANEN', 'q51': 'ATIV_EXTRACURRICULAR', 'q52': 'ATIVEXTRA_PESQUISA', 'q53': 'BOLSAREMUN_PESQUISA', 'q54': 'ATIVEXTRA_EXTENSAO', 'q55': 'BOLSAREMUN_EXTENSAO', 'q56': 'ATIVEXTRA_MONITORIA', 'q57': 'BOLSAREMUN_MONITORIA', 'q58': 'ATIVEXTRA_ESTAGIO', 'q59': 'BOLSAREMUN_ESTAGIO', 'q60': 'CARGA_HORARIA_TOTAL', 'q61': 'CARGA_HORARIA_INTEGRALIZADA'}


            df = df.rename(columns=nomesColunasAluno)

            periodoReferencia = linhaAlunoAtual['PERIODO_REFERENCIA']

            semestreIngresso = linhaAlunoAtual['SEMESTRE_INGRESSO']
            semestreIngresso = str(semestreIngresso)
            semestreIngresso = semestreIngresso[0] + 'º/' + semestreIngresso[1:5] 
            
            for i in range(len(linhaAlunoAtual)):
                if pd.isna(linhaAlunoAtual[i]):
                    linhaAlunoAtual[i] = 0
                elif type(linhaAlunoAtual[i]) == str:
                    pass
                elif type(linhaAlunoAtual[i]) == int or type(linhaAlunoAtual[i]) == np.int32 or type(linhaAlunoAtual[i]) == np.int64 or type(linhaAlunoAtual[i]) == float or type(linhaAlunoAtual[i]) == np.float64 or type(linhaAlunoAtual[i]) == np.float32:
                    linhaAlunoAtual[i] = int(linhaAlunoAtual[i])
                else:
                    linhaAlunoAtual[i] = 0
                

            #Tabela curso 
            
            #Falta Modalidade Ensino Grau ACADEMICO NÍVEL ACADEMICO
            with conn.cursor() as cur:
                comando = """INSERT INTO CURSO(CODIGO_CURSO, NOME_CURSO, GRAU_ACADEMICO, NIVEL_ACADEMICO, MODALIDADE_ENSINO)
                        VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (CODIGO_CURSO) DO NOTHING;
                        """
                cur.execute(comando,(linhaAlunoAtual['CODIGO_CURSO'], linhaAlunoAtual['NOME_CURSO'], 0, 0, 0))

                conn.commit()
                


            #tabela VINCULO PERIODO REFERENCIA 1
            with conn.cursor() as cur:            
                comando = """INSERT INTO VINCULO (cod_curso, nu_cpf, situacao_vinculo, cod_polo, nome_polo, turno, semestre_ingresso, periodo_referencia, semestre_conclusao, 
                        tp_escola_ens_medio, id_aluno_ies, carga_horaria_total, carga_horaria_integralizada,investibular, inenem, inavaliacaoseriada, inselecaosimplificada, 
                        inconveniopecg, indecisaojudicial, inselecaoprogramaespecial, inselecaoremanescente, intranferenciaexofficio, aluno_parfor, formacao_pedag, 
                        segunda_licenc) VALUES (%s, %s, %s,  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING vinculo_id"""
                
                
                cur.execute(comando, (linhaAlunoAtual['CODIGO_CURSO'],nuCpf,linhaAlunoAtual['SITUACAO_VINCULO'], 0, "Não informado", linhaAlunoAtual['TURNO'],semestreIngresso,
                                    str(ano_referencia)+"."+str(periodoReferencia), linhaAlunoAtual['SEMESTRE_CONCLUSAO'], linhaAlunoAtual['TP_ESCOLA_ENS_MEDIO'], 0, 0,
                                    0, linhaAlunoAtual['INVESTIBULAR'], linhaAlunoAtual['INENEM'], linhaAlunoAtual['INAVALIACAOSERIADA'], linhaAlunoAtual['INSELECAOSIMPLIFICADA'],
                                    linhaAlunoAtual['INCONVENIOPECG'], linhaAlunoAtual['INDECISAOJUDICIAL'], linhaAlunoAtual['INSELECAOPROGRAMAESPECIAL'], linhaAlunoAtual['INSELECAOREMANESCENTE'], 
                                    linhaAlunoAtual['INTRANFERENCIAEXOFFICIO'], linhaAlunoAtual['ALUNO_PARFOR'], 0, 0))
                id_of_new_row = cur.fetchone()[0]
                conn.commit()
                apoiosocial_alimentacao = 0
                apoiosocial_per = 0
                apoiosocial_maltl = 0
                apoiosocial_morad = 0
                apoiosocial_trans = 0
                if linhaAlunoAtual['ALUNO_APOIO_SOCIAL'] == 1:
                    if linhaAlunoAtual['APOIOSOCIAL_ALIMENTACAO'] == 1:
                        apoiosocial_alimentacao = 1 
                    if linhaAlunoAtual['APOIOSOCIAL_BOLSAPERMANEN'] == 1:
                        apoiosocial_per = 2
                    if linhaAlunoAtual['APOIOSOCIAL_MATLDIDATICO'] == 1:
                        apoiosocial_maltl = 3
                    if linhaAlunoAtual['APOIOSOCIAL_MORADIA'] == 1:
                        apoiosocial_morad = 4
                    if linhaAlunoAtual['APOIOSOCIAL_TRANSPORTE'] == 1:
                        apoiosocial_trans = 5
                arrayApoio = [apoiosocial_trans, apoiosocial_alimentacao, apoiosocial_maltl, apoiosocial_morad, apoiosocial_per]
                
                for apoio in arrayApoio:
                    if apoio != 0:
                        with conn.cursor() as cur:
                            comando = """ insert into vinculo_apoio_social (vinculo_id, apoio_social_id ) values(%s, %s)"""
                            
                            cur.execute(comando, (id_of_new_row, apoio))

                            conn.commit()

                ativextra_estagio = 0
                bolsaremun_estagio = 0
                ativextra_extensao = 0
                bolsaremun_extensao = 0
                ativextra_monitoria = 0
                bolsaremun_monitoria = 0
                ativextra_pesquisa = 0
                bolsaremun_pesquisa = 0

                if linhaAlunoAtual['ATIV_EXTRACURRICULAR'] == 1:
                    if linhaAlunoAtual['ATIVEXTRA_ESTAGIO'] == 1:
                        ativextra_estagio = 1 
                    if linhaAlunoAtual['BOLSAREMUN_ESTAGIO'] == 1:
                        bolsaremun_estagio = 2
                    if linhaAlunoAtual['ATIVEXTRA_EXTENSAO'] == 1:
                        ativextra_extensao = 3
                    if linhaAlunoAtual['BOLSAREMUN_EXTENSAO'] == 1:
                        bolsaremun_extensao = 4
                    if linhaAlunoAtual['ATIVEXTRA_MONITORIA'] == 1:
                        ativextra_monitoria = 5
                    if linhaAlunoAtual['BOLSAREMUN_MONITORIA'] == 1:
                        bolsaremun_monitoria = 6
                    if linhaAlunoAtual['ATIVEXTRA_PESQUISA']== 1:
                        ativextra_pesquisa = 7
                    if linhaAlunoAtual['BOLSAREMUN_PESQUISA'] == 1:
                        bolsaremun_pesquisa = 8
                arrayAtiv = [ativextra_estagio, bolsaremun_estagio,ativextra_extensao,bolsaremun_extensao,ativextra_monitoria,bolsaremun_monitoria,ativextra_pesquisa,bolsaremun_pesquisa]
                
                for ativ in arrayAtiv:
                    if ativ != 0:
                        with conn.cursor() as cur:
                            comando = """ insert into vinculo_ativ_extracur (vinculo_id, ativ_extracurricular_id ) values(%s, %s)"""
                            
                            cur.execute(comando, (id_of_new_row, ativ))

                            conn.commit()

                resvaga_estudprocedeescpub = 0
                resvaga_etnico = 0
                resvaga_pessoadefic = 0
                resvaga_rendafam = 0
                resvaga_outro = 0

                if linhaAlunoAtual['PROG_RESERVA_VAGA'] == 1:
                    if linhaAlunoAtual['RESVAGA_ESTUDPROCEDEESCPUB'] == 1:
                        resvaga_estudprocedeescpub = 1 
                    if linhaAlunoAtual['RESVAGA_ETNICO'] == 1:
                        resvaga_etnico = 2
                    if linhaAlunoAtual['RESVAGA_PESSOADEFIC'] == 1:
                        resvaga_pessoadefic = 3
                    if linhaAlunoAtual['RESVAGA_RENDAFAM'] == 1:
                        resvaga_rendafam = 4
                    if linhaAlunoAtual['RESVAGA_OUTRO'] == 1:
                        resvaga_outro = 5
                arrayvaga = [resvaga_estudprocedeescpub, resvaga_etnico,resvaga_pessoadefic,resvaga_rendafam,resvaga_outro]
                
                for vaga in arrayvaga:
                    if vaga != 0:
                        with conn.cursor() as cur:
                            comando = """ insert into vinculo_reserva_vaga (vinculo_id, prog_reserva_vaga_id ) values(%s, %s)"""
                            
                            cur.execute(comando, (id_of_new_row, vaga))

                            conn.commit()



                Nao_quis_declarar = 0
                Branca = 0
                Preta = 0
                Parda = 0
                Amarela = 0
                Indigena = 0
                Não_dispoe_da_informacao = 0

                if corAluno == 0:
                    Nao_quis_declarar = 0 
                if corAluno == 1:
                    Branca = 1
                if corAluno == 2:
                    Preta = 2
                if corAluno == 3:
                    Parda = 3
                if corAluno == 4:
                    Amarela = 4
                if corAluno == 5:
                    Indigena = 5
                if corAluno == 6:
                    Não_dispoe_da_informacao = 6
                        
                arrayraca = [Nao_quis_declarar, Branca,Preta,Parda,Amarela,Indigena,Não_dispoe_da_informacao]
                
                for raca in arrayraca:
                    if raca != 0:
                        with conn.cursor() as cur:
                            comando = """ insert into vinculo_cor_raca (vinculo_id, raca_id ) values(%s, %s)"""
                            
                            cur.execute(comando, (id_of_new_row, raca))
                            conn.commit()
               
                
        else:
            #Atualiza os dados Período referência 1
            
            linhaAlunoAtual = df.loc[df['id'] == idAluno, :].iloc[4]
            #Renomear as colunas

            nomesColunasAluno = {"ID_ALUNO": "PERIODO_REFERENCIA",'q3':'CODIGO_CURSO',   'NU_CPF': "TURNO",  'NU_DOC_ESTRANGEIRO': "SITUACAO_VINCULO", 'SEXO': "SEMESTRE_CONCLUSAO",  'COR_RACA': "ALUNO_PARFOR", 'NOME_MAE_ALUNO': "SEMESTRE_INGRESSO",  'COD_NACIONALIDADE': "TP_ESCOLA_ENS_MEDIO",  'COD_UF_NASCIMENTO': "INVESTIBULAR",  'COD_MUNIC_NASCIMENTO': "INENEM",  'NOME_PAIS_ORIGEM': "INAVALIACAOSERIADA",  'ALUNO_DEFICIENCIA': "INSELECAOSIMPLIFICADA", "q16": "INCONVENIOPECG", "q17":"INTRANFERENCIAEXOFFICIO", "q18": "INDECISAOJUDICIAL", "q19": "INSELECAOREMANESCENTE", "q20": "INSELECAOPROGRAMAESPECIAL",  "q21": "MOBILIDADE_ACADEMICA", "q22": "MOB_ACAD_NACIONAL_MOB_ACAD_INTERNACIONAL", "q23": "MOB_ACAD_IESDESTINO", "q24": "TIPOMOBILIDADE000000", "q25": "MOB_ACAD_PAISDESTINO", 'q26':'PROG_RESERVA_VAGA', 'q27':'RESVAGA_ETNICO', 'q28':'RESVAGA_PESSOADEFIC', 'q29':'RESVAGA_ESTUDPROCEDEESCPUB', 'q30':'RESVAGA_RENDAFAM', 'q31':'RESVAGA_OUTRO', 'q44': 'ALUNO_APOIO_SOCIAL', 'q45': 'APOIOSOCIAL_ALIMENTACAO', 'q46': 'APOIOSOCIAL_MORADIA', 'q47': 'APOIOSOCIAL_TRANSPORTE', 'q48': 'APOIOSOCIAL_MATLDIDATICO', 'q49': 'APOIOSOCIAL_BOLSATRABALHO', 'q50': 'APOIOSOCIAL_BOLSAPERMANEN', 'q51': 'ATIV_EXTRACURRICULAR', 'q52': 'ATIVEXTRA_PESQUISA', 'q53': 'BOLSAREMUN_PESQUISA', 'q54': 'ATIVEXTRA_EXTENSAO', 'q55': 'BOLSAREMUN_EXTENSAO', 'q56': 'ATIVEXTRA_MONITORIA', 'q57': 'BOLSAREMUN_MONITORIA', 'q58': 'ATIVEXTRA_ESTAGIO', 'q59': 'BOLSAREMUN_ESTAGIO', 'q60': 'CARGA_HORARIA_TOTAL', 'q61': 'CARGA_HORARIA_INTEGRALIZADA'}


            df = df.rename(columns=nomesColunasAluno)

            periodoReferencia = linhaAlunoAtual['PERIODO_REFERENCIA']

            semestreIngresso = linhaAlunoAtual['SEMESTRE_INGRESSO']
            semestreIngresso = str(semestreIngresso)
            semestreIngresso = semestreIngresso[0] + 'º/' + semestreIngresso[1:5]
            
            for i in range(len(linhaAlunoAtual)):
                if pd.isna(linhaAlunoAtual[i]):
                    linhaAlunoAtual[i] = 0
                elif type(linhaAlunoAtual[i]) == str:
                    pass
                elif type(linhaAlunoAtual[i]) == int or type(linhaAlunoAtual[i]) == np.int32 or type(linhaAlunoAtual[i]) == np.int64 or type(linhaAlunoAtual[i]) == float or type(linhaAlunoAtual[i]) == np.float64 or type(linhaAlunoAtual[i]) == np.float32:
                    linhaAlunoAtual[i] = int(linhaAlunoAtual[i])
                else:
                    linhaAlunoAtual[i] = 0
                

            #Tabela curso 
            
            #Falta Modalidade Ensino Grau ACADEMICO NÍVEL ACADEMICO
            with conn.cursor() as cur:
                comando = """INSERT INTO CURSO(CODIGO_CURSO, NOME_CURSO, GRAU_ACADEMICO, NIVEL_ACADEMICO, MODALIDADE_ENSINO)
                        VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (CODIGO_CURSO) DO NOTHING;
                        """
                cur.execute(comando,(linhaAlunoAtual['CODIGO_CURSO'], linhaAlunoAtual['NOME_CURSO'], 0, 0, 0))

                conn.commit()
                


            #tabela VINCULO PERIODO REFERENCIA 1
            with conn.cursor() as cur:            
                comando = """INSERT INTO VINCULO (cod_curso, nu_cpf, situacao_vinculo, cod_polo, nome_polo, turno, semestre_ingresso, periodo_referencia, semestre_conclusao, 
                        tp_escola_ens_medio, id_aluno_ies, carga_horaria_total, carga_horaria_integralizada,investibular, inenem, inavaliacaoseriada, inselecaosimplificada, 
                        inconveniopecg, indecisaojudicial, inselecaoprogramaespecial, inselecaoremanescente, intranferenciaexofficio, aluno_parfor, formacao_pedag, 
                        segunda_licenc) VALUES (%s, %s, %s,  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING vinculo_id"""
                
                
                cur.execute(comando, (linhaAlunoAtual['CODIGO_CURSO'],nuCpf,linhaAlunoAtual['SITUACAO_VINCULO'], 0, "Não informado", linhaAlunoAtual['TURNO'],semestreIngresso,
                                    str(ano_referencia)+"."+str(periodoReferencia), linhaAlunoAtual['SEMESTRE_CONCLUSAO'], linhaAlunoAtual['TP_ESCOLA_ENS_MEDIO'], 0, 0,
                                    0, linhaAlunoAtual['INVESTIBULAR'], linhaAlunoAtual['INENEM'], linhaAlunoAtual['INAVALIACAOSERIADA'], linhaAlunoAtual['INSELECAOSIMPLIFICADA'],
                                    linhaAlunoAtual['INCONVENIOPECG'], linhaAlunoAtual['INDECISAOJUDICIAL'], linhaAlunoAtual['INSELECAOPROGRAMAESPECIAL'], linhaAlunoAtual['INSELECAOREMANESCENTE'], 
                                    linhaAlunoAtual['INTRANFERENCIAEXOFFICIO'], linhaAlunoAtual['ALUNO_PARFOR'], 0, 0))
                id_of_new_row = cur.fetchone()[0]
                conn.commit()
                apoiosocial_alimentacao = 0
                apoiosocial_per = 0
                apoiosocial_maltl = 0
                apoiosocial_morad = 0
                apoiosocial_trans = 0
                if linhaAlunoAtual['ALUNO_APOIO_SOCIAL'] == 1:
                    if linhaAlunoAtual['APOIOSOCIAL_ALIMENTACAO'] == 1:
                        apoiosocial_alimentacao = 1 
                    if linhaAlunoAtual['APOIOSOCIAL_BOLSAPERMANEN'] == 1:
                        apoiosocial_per = 2
                    if linhaAlunoAtual['APOIOSOCIAL_MATLDIDATICO'] == 1:
                        apoiosocial_maltl = 3
                    if linhaAlunoAtual['APOIOSOCIAL_MORADIA'] == 1:
                        apoiosocial_morad = 4
                    if linhaAlunoAtual['APOIOSOCIAL_TRANSPORTE'] == 1:
                        apoiosocial_trans = 5
                arrayApoio = [apoiosocial_trans, apoiosocial_alimentacao, apoiosocial_maltl, apoiosocial_morad, apoiosocial_per]
                
                for apoio in arrayApoio:
                    if apoio != 0:
                        with conn.cursor() as cur:
                            comando = """ insert into vinculo_apoio_social (vinculo_id, apoio_social_id ) values(%s, %s)"""
                            
                            cur.execute(comando, (id_of_new_row, apoio))

                            conn.commit()

                ativextra_estagio = 0
                bolsaremun_estagio = 0
                ativextra_extensao = 0
                bolsaremun_extensao = 0
                ativextra_monitoria = 0
                bolsaremun_monitoria = 0
                ativextra_pesquisa = 0
                bolsaremun_pesquisa = 0

                if linhaAlunoAtual['ATIV_EXTRACURRICULAR'] == 1:
                    if linhaAlunoAtual['ATIVEXTRA_ESTAGIO'] == 1:
                        ativextra_estagio = 1 
                    if linhaAlunoAtual['BOLSAREMUN_ESTAGIO'] == 1:
                        bolsaremun_estagio = 2
                    if linhaAlunoAtual['ATIVEXTRA_EXTENSAO'] == 1:
                        ativextra_extensao = 3
                    if linhaAlunoAtual['BOLSAREMUN_EXTENSAO'] == 1:
                        bolsaremun_extensao = 4
                    if linhaAlunoAtual['ATIVEXTRA_MONITORIA'] == 1:
                        ativextra_monitoria = 5
                    if linhaAlunoAtual['BOLSAREMUN_MONITORIA'] == 1:
                        bolsaremun_monitoria = 6
                    if linhaAlunoAtual['ATIVEXTRA_PESQUISA']== 1:
                        ativextra_pesquisa = 7
                    if linhaAlunoAtual['BOLSAREMUN_PESQUISA'] == 1:
                        bolsaremun_pesquisa = 8
                arrayAtiv = [ativextra_estagio, bolsaremun_estagio,ativextra_extensao,bolsaremun_extensao,ativextra_monitoria,bolsaremun_monitoria,ativextra_pesquisa,bolsaremun_pesquisa]
                
                for ativ in arrayAtiv:
                    if ativ != 0:
                        with conn.cursor() as cur:
                            comando = """ insert into vinculo_ativ_extracur (vinculo_id, ativ_extracurricular_id ) values(%s, %s)"""
                            
                            cur.execute(comando, (id_of_new_row, ativ))

                            conn.commit()

                resvaga_estudprocedeescpub = 0
                resvaga_etnico = 0
                resvaga_pessoadefic = 0
                resvaga_rendafam = 0
                resvaga_outro = 0

                if linhaAlunoAtual['PROG_RESERVA_VAGA'] == 1:
                    if linhaAlunoAtual['RESVAGA_ESTUDPROCEDEESCPUB'] == 1:
                        resvaga_estudprocedeescpub = 1 
                    if linhaAlunoAtual['RESVAGA_ETNICO'] == 1:
                        resvaga_etnico = 2
                    if linhaAlunoAtual['RESVAGA_PESSOADEFIC'] == 1:
                        resvaga_pessoadefic = 3
                    if linhaAlunoAtual['RESVAGA_RENDAFAM'] == 1:
                        resvaga_rendafam = 4
                    if linhaAlunoAtual['RESVAGA_OUTRO'] == 1:
                        resvaga_outro = 5
                arrayvaga = [resvaga_estudprocedeescpub, resvaga_etnico,resvaga_pessoadefic,resvaga_rendafam,resvaga_outro]
                
                for vaga in arrayvaga:
                    if vaga != 0:
                        with conn.cursor() as cur:
                            comando = """ insert into vinculo_reserva_vaga (vinculo_id, prog_reserva_vaga_id ) values(%s, %s)"""
                            
                            cur.execute(comando, (id_of_new_row, vaga))

                            conn.commit()



                Nao_quis_declarar = 0
                Branca = 0
                Preta = 0
                Parda = 0
                Amarela = 0
                Indigena = 0
                Não_dispoe_da_informacao = 0

                if corAluno == 0:
                    Nao_quis_declarar = 0 
                if corAluno == 1:
                    Branca = 1
                if corAluno == 2:
                    Preta = 2
                if corAluno == 3:
                    Parda = 3
                if corAluno == 4:
                    Amarela = 4
                if corAluno == 5:
                    Indigena = 5
                if corAluno == 6:
                    Não_dispoe_da_informacao = 6
                        
                arrayraca = [Nao_quis_declarar, Branca,Preta,Parda,Amarela,Indigena,Não_dispoe_da_informacao]
                
                for raca in arrayraca:
                    if raca != 0:
                        with conn.cursor() as cur:
                            comando = """ insert into vinculo_cor_raca (vinculo_id, raca_id ) values(%s, %s)"""
                            
                            cur.execute(comando, (id_of_new_row, raca))
                            conn.commit()
        
        inserirDados += 1
