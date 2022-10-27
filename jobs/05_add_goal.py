import pandas as pd


DATALAKE_RAW = 'data/raw/'
DATALAKE_WORK = 'data/work/'
DATALAKE_TRUSTED = 'data/trusted/'




def create_median_meta_empresa(df_meta_acoes:pd.DataFrame):
    # Organizando meta de peso das acoes
    empresas_not_meta = []
    empresas_with_meta = []
    for i in range(len(df_meta_acoes)):
        empresa = df_meta_acoes.iloc[i,0]
        meta = df_meta_acoes.iloc[i,1]
        if str(meta)=='nan':
            empresas_not_meta.append(empresa)
        else:
            empresas_with_meta.append((empresa,meta))


    # Definindo media das empresas sem meta
    goal_total_empresas_with_meta = sum(map(lambda x: x[1],empresas_with_meta))
    remainder_goal = 100 - goal_total_empresas_with_meta
    goal_empresa_without_meta = remainder_goal / len(empresas_not_meta)


    # Adicionando a media nas acoes sem meta
    for i in range(len(df_meta_acoes)):
        serie_empresa = df_meta_acoes.iloc[i,0]
        for empresa in empresas_not_meta:
            if serie_empresa in empresa:
                df_meta_acoes.iloc[i,1] = round(goal_empresa_without_meta,2)


    return df_meta_acoes


def create_meta_and_focus(df_peso_acoes:pd.DataFrame,df_carteira:pd.DataFrame):
    df_carteira = pd.merge(df_carteira,df_peso_acoes,how='inner',on=['EMPRESA'])
    df_carteira['DEFICT'] = round(df_carteira.POSICAO_ATUAL - df_carteira.META,2)
    df_carteira['OBJETIVO'] =''
    df_carteira['OBJETIVO'] = df_carteira['DEFICT'].apply(lambda x: 'DIMINUIR' if x > 0 else 'AUMENTAR')


    return df_carteira


if __name__ == '__main__':
    df_carteira = pd.read_csv(DATALAKE_WORK + 'carteira_fase_04.csv')
    df_meta_acoes = pd.read_csv(DATALAKE_RAW + 'meta_acoes.csv')

    df_meta_acoes = create_median_meta_empresa(df_meta_acoes)
    df_carteira = create_meta_and_focus(df_meta_acoes,df_carteira)
    df_carteira.to_csv(DATALAKE_WORK + 'carteira_fase_05' + '.csv',index=None)



