import os
from enum import Enum

# Constantes
NFe = "55"
NFCe = "65"

inicio_referencia = os.environ.get('INICIO_REFERENCIA')
fim_referencia = os.environ.get('FIM_REFERENCIA')


# Enumeradores
class EnumStatusProcessamento(Enum):
    iniciado = 1
    sucesso = 2
    erro = 3
    sucessoSemDados = 4


class EnumMotivoExclusao(Enum):
    CFOPNaoParticipante = 1
    CClassNaoParticipante = 2
    ItemAtivoImobilizado = 3
    OperacaoDuplicada = 4
    DocumentoCancelado = 10
    ContribsNaoCadOuSimples = 11
    DocumentoNaoContemItemParticipante = 12
    DocumentoSubstituido = 13
    ValoresInconsistentes = 14


class EnumTipoExclusao(Enum):
    Item = 1
    Documento = 2


class EnumEtapaIndice(Enum):
    Provisorio = 'P'
    Definitivo = 'D'


class EnumTipoIndice(Enum):
    CemPorCento = '1'
    OitentaECincoPorCento = '5'
    SetentaPorCento = '7'
    MediaDoisAnos = 'M'
    Igualitario = 'I'
    Ecologico = 'V'
    Educacao = 'E'
    Saude = 'S'
    Final = 'F'


class EnumTipoDocumento(Enum):
    NFCe = 1
    NFeRecebida = 2
    NF3e = 3
    NFA = 4
    EFD = 5
    TelecomConv115 = 6
    BPe = 7
    CTe = 8
    AutoInfracao = 9
    NFe = 10
    Simples = 11
    MEI = 12
    OperacoesEspeciais = 13


class EnumTipoProcessamento(Enum):
    importacaoNFCe = 1
    importacaoNFeRecebida = 2
    importacaoNF3e = 3
    importacaoNFA = 4
    importacaoEFD = 5
    importacaoTelecomConv115 = 6
    importacaoBPe = 7
    importacaoCTe = 8
    importacaoPAT = 9
    importacaoNFe = 10
    importacaoSimples = 11
    importacaoMEI = 12
    importacaoOpcaoSimplesSIMEI = 13
    importacaoArquivoVA = 14
    importacaoArquivoIndice = 15

    processaAtivoImobilizado = 20
    processaOperProdRurais = 21
    processaCCEE = 22
    processaValorAdicionado = 30

    # processaConv115 = 15
    # processaBPe = 16
    # processaCTe = 17
    # processaEFD = 18
    # processaPAT = 19
    #
    # calculaNFe = 21
    # calculaNFeRecebida = 22
    # calculaNF3e = 23
    # calculaNFA = 24
    # calculaConv115 = 25
    # calculaBPe = 26
    # calculaCTe = 27
    # calculaEFD = 28
    # calculaPAT = 29

    processaIndicePartct = 50

    processaAcertoNFe = 90
    processaAcertoNFCe = 91
    processaAcertoNFeRecebida = 92

    processaAcertoContrib = 93


class EnumParametros(Enum):
    atrasoProcessNFe = 'atrasoProcessNFe'
    periodProcessNFe = 'periodProcessNFe'

    ultimaCargaNFCe = 'ultimaCargaNFCe'
    ultimaCargaNFeRecebida = 'ultimaCargaNFeRecebida'
    ultimaCargaNF3e = 'ultimaCargaNF3e'
    ultimaCargaNFA = 'ultimaCargaNFA'
    ultimaCargaEFD = 'ultimaCargaEFD'
    ultimaCargaConv115 = 'ultimaCargaConv115'
    ultimaCargaBPe = 'ultimaCargaBPe'
    ultimaCargaCTe = 'ultimaCargaCTe'
    ultimaCargaPAT = 'ultimaCargaPAT'
    ultimaCargaNFe = 'ultimaCargaNFe'
    ultimaCargaAutoInfracao = 'ultimaCargaAutoInfracao'
    ultimaCargaSimples = 'ultimaCargaSimples'
    ultimaCargaMEI = 'ultimaCargaMEI'
    ultimaCargaOpcaoSimplesSIMEI = 'ultimaCargaOpcaoSimplesSIMEI'

    ultimoProcessVA = 'ultimoProcessVA'

    ultimoAcertoCFOPNFA = 'ultimoAcertoCFOPNFA'
    ultimoAcertoCFOPNFe = 'ultimoAcertoCFOPNFe'
    ultimoAcertoCFOPNFCe = 'ultimoAcertoCFOPNFCe'
    ultimoAcertoCFOPNFeRecebida = 'ultimoAcertoCFOPNFeRecebida'

    ultimoAcertoCadNFA = 'ultimoAcertoCadNFA'
    ultimoAcertoCadNFe = 'ultimoAcertoCadNFe'
    ultimoAcertoCadNFCe = 'ultimoAcertoCadNFCe'
    ultimoAcertoCadNFeRecebida = 'ultimoAcertoCadNFeRecebida'


class EnumOutliers(Enum):
    outlierMaiorPrecoMaximo = 1
    outlierDistIncoerente = 2
    outlierValrForaMedia = 3
    outlierValrForaMediana = 4
    outlierValrForaQ1 = 5
    outlierValrForaQ3 = 6
    outlierNaoCombustivel = 7
    outlierValrMuitoBaixo = 8
    outlierItemRepetidoNFe = 9


class TiposTreinos(Enum):
    GaussianNB = 'GaussianNB'
    GradientBoostingClassifier = 'GradientBoostingClassifier'
    DecisionTreeClassifier = 'DecisionTreeClassifier'
    RandomForestClassifier = 'RandomForestClassifier'
    SVC = 'SVC'
    DummyClassifier = 'DummyClassifier'
    LinearRegression = 'LinearRegression'
    KNeighborsRegressor = 'KNeighborsRegressor'
    PassiveAggressiveClassifier = 'PassiveAggressiveClassifier'
