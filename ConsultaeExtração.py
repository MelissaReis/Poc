from selenium import webdriver
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from sqlalchemy import create_engine, Column, String, Integer
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class TabelaUm(Base):
    __tablename__ = 'TabelaUm'

    id = Column(Integer, primary_key=True)
    Num = Column(String(500))
    Campo = Column(String(500))
    Descricao = Column(String(500))
    Tipo = Column(String(500))
    Tam = Column(String(500))
    Dec = Column(String(500))
    Entr = Column(String(500))
    Saida = Column(String(500))

class TabelaDois(Base):
    __tablename__ = 'TabelaDois'

    id = Column(Integer, primary_key=True)
    Num = Column(String(500))
    Data = Column(String(500))
    Vigencia = Column(String(500))
    Alteracao = Column(String(500))

def conexao_banco():
    engine = create_engine('mssql+pymssql://sa:sa132@MiAkamine/C100')
    Session = sessionmaker(bind=engine)
    return Session()

def ConsultaeExtracaodetabelas():
    servico = Service(ChromeDriverManager().install())
    navegador = webdriver.Chrome(service=servico)

    navegador.get('https://www.vriconsulting.com.br/guias/guiasIndex.php?idGuia=22')
    tabelaUm = navegador.find_element(By.XPATH, '/html/body/section/section[2]/table[1]')
    tabelaDois = navegador.find_element(By.XPATH, '/html/body/section/section[2]/table[2]')

    data = {'TabelaUm': [], 'TabelaDois': []}
    for tr in tabelaUm.find_elements(By.XPATH, './/tr'):
        row = [item.text for item in tr.find_elements(By.XPATH, './/td')]
        data['TabelaUm'].append(row)

    for tr in tabelaDois.find_elements(By.XPATH, './/tr'):
        row = [item.text for item in tr.find_elements(By.XPATH, './/td')]
        data['TabelaDois'].append(row)

    return data

def insert():
    dados_da_tabela = ConsultaeExtracaodetabelas()

    if dados_da_tabela:
        session = conexao_banco()
        if session:
            try:

                for linha in dados_da_tabela['TabelaUm']:
                    if len(linha) == 8:
                        session.add(TabelaUm(
                            id=linha[0],
                            Num=linha[0],
                            Campo=linha[1],
                            Descricao=linha[2],
                            Tipo=linha[3],
                            Tam=linha[4],
                            Dec=linha[5],
                            Entr=linha[6],
                            Saida=linha[7]
                        ))

                for linha in dados_da_tabela['TabelaDois']:
                    if len(linha) == 4:
                        session.add(TabelaDois(
                            id=linha[0],
                            Num=linha[0],
                            Data=linha[1],
                            Vigencia=linha[2],
                            Alteracao=linha[3],
                        ))

                session.commit()
                print("Dados inseridos com sucesso.")
            except Exception as e:
                session.rollback()
                print(f"Erro ao inserir dados: {str(e)}")
            finally:
                session.close()
        else:
            print("Erro na conexão com o banco de dados.")



