#!/usr/bin/env python
# coding: utf-8

import pandas as pd

dataframe = pd.read_csv("ocorrencia_2010_2020_v2.csv")
dataframe

# separating the file by ";"
df = pd.read_csv("ocorrencia_2010_2020_v2.csv", sep=";")
df

# checking dataset data types
df.dtypes

# classifying column 'oocorrencia_dia' as type datetime
df2 = pd.read_csv("ocorrencia_2010_2020_v2.csv", sep=";", parse_dates=['ocorrencia_dia'])
df2.dtypes


df2.ocorrencia_dia.dt.month


# Step 2 - Data Validation
df2.head(10)

# validation of date and month order in the dataset
df3 = pd.read_csv("ocorrencia_2010_2020_v2.csv", sep=";", parse_dates=['ocorrencia_dia'], dayfirst=True)
df3.head(10)

# The Pandera library allows you to perform data validation, returning an error if the data does not agree with what we need.
import pandera as pa

schema = pa.DataFrameSchema(
    columns = {
        "codigo_ocorrencia":pa.Column(pa.DateTime)
        
    }
)
schema.validate(df3)

# The error above returns that there is some invalid data in the day_occurrence column of the Dataframe.

# next, we will validate only one "codigo_ocorrencia" column for type Int
schema = pa.DataFrameSchema(
    columns = {
        "codigo_ocorrencia":pa.Column(pa.Int)
        
    }
)

schema.validate(df3)

# after verifying the successful validation, we can validate all columns of our DataFrame
schema = pa.DataFrameSchema(
    columns = {
        "codigo_ocorrencia": pa.Column(pa.Int),
        "codigo_ocorrencia2": pa.Column(pa.Int),
        "ocorrencia_classificacao": pa.Column(pa.String),
        "ocorrencia_cidade": pa.Column(pa.String),
        "ocorrencia_uf": pa.Column(pa.String),
        "ocorrencia_aerodromo": pa.Column(pa.String),
        "ocorrencia_dia": pa.Column(pa.DateTime),
        "ocorrencia_hora": pa.Column(pa.String),
        "total_recomendacoes": pa.Column(pa.Int),
    }
)

schema.validate(df3)

# correcting "ocorrencia_hora" column error / allowing null values

schema = pa.DataFrameSchema(
    columns = {
        "codigo_ocorrencia": pa.Column(pa.Int),
        "codigo_ocorrencia2": pa.Column(pa.Int),
        "ocorrencia_classificacao": pa.Column(pa.String),
        "ocorrencia_cidade": pa.Column(pa.String),
        "ocorrencia_uf": pa.Column(pa.String),
        "ocorrencia_aerodromo": pa.Column(pa.String),
        "ocorrencia_dia": pa.Column(pa.DateTime),
        "ocorrencia_hora": pa.Column(pa.String, nullable=True),
        "total_recomendacoes": pa.Column(pa.Int),
    }
)

schema.validate(df3)

# Validating 24-hour time format

# creating a regular expression to handle the "ocorrencia_hora" column
schema = pa.DataFrameSchema(
    columns = {
        "codigo_ocorrencia": pa.Column(pa.Int),
        "codigo_ocorrencia2": pa.Column(pa.Int),
        "ocorrencia_classificacao": pa.Column(pa.String),
        "ocorrencia_cidade": pa.Column(pa.String),
        "ocorrencia_uf": pa.Column(pa.String),
        "ocorrencia_aerodromo": pa.Column(pa.String),
        "ocorrencia_dia": pa.Column(pa.DateTime),
        "ocorrencia_hora": pa.Column(pa.String, pa.Check.str_matches(r'^([0-1]?[0-9]|[2][0-3]):([0-5][0-9])(:[0-5][0-9])?$'), nullable=True),
        "total_recomendacoes": pa.Column(pa.Int),
    }
)

schema.validate(df3)

# ## Validating Character Size in a Column
'''
Some columns may require a limited number of values,
for example, the "ocorrencia_uf" column where is the abbreviation of a State.
'''
schema = pa.DataFrameSchema(
    columns = {
        "codigo": pa.Column(pa.Int),
        "codigo_ocorrencia": pa.Column(pa.Int),
        "codigo_ocorrencia2": pa.Column(pa.Int),
        "ocorrencia_classificacao": pa.Column(pa.String),
        "ocorrencia_cidade": pa.Column(pa.String),
        "ocorrencia_uf": pa.Column(pa.String, pa.Check.str_length(2,2)),
        "ocorrencia_aerodromo": pa.Column(pa.String),
        "ocorrencia_dia": pa.Column(pa.DateTime),
        "ocorrencia_hora": pa.Column(pa.String, pa.Check.str_matches(r'^([0-1]?[0-9]|[2][0-3]):([0-5][0-9])(:[0-5][0-9])?$'), nullable=True),
        "total_recomendacoes": pa.Column(pa.Int),
    }
)
schema.validate(df3)

# handling previous error with Require parameter: 
schema = pa.DataFrameSchema(
    columns = {
        "codigo": pa.Column(pa.Int, required=False),
        "codigo_ocorrencia": pa.Column(pa.Int),
        "codigo_ocorrencia2": pa.Column(pa.Int),
        "ocorrencia_classificacao": pa.Column(pa.String),
        "ocorrencia_cidade": pa.Column(pa.String),
        "ocorrencia_uf": pa.Column(pa.String, pa.Check.str_length(2,2)),
        "ocorrencia_aerodromo": pa.Column(pa.String),
        "ocorrencia_dia": pa.Column(pa.DateTime),
        "ocorrencia_hora": pa.Column(pa.String, pa.Check.str_matches(r'^([0-1]?[0-9]|[2][0-3]):([0-5][0-9])(:[0-5][0-9])?$'), nullable=True),
        "total_recomendacoes": pa.Column(pa.Int),
    }
)
schema.validate(df3)

# Step 3 - Data Cleaning

# - After validation, there is still data that is not ready to be Transformed.
# Data such as #, ?, ! * must be removed for our DataFrame as they are not logically useful.
# After checking the cvs spreadsheet, we identified that the files to be cleaned are:
# ocorrencia_uf: (**)
# ocorrencia_aerodromo: (##! - #### - ** - ***)
# ocorrencia_hora: (NULL)

df3.head()

'''
Clearing all ocorrencia_aerodromo lines that contain (****)
pa.NA = manipulates not evaluable values in a cell
'''
df3.loc[df3.ocorrencia_aerodromo == '****', ['ocorrencia_aerodromo']] = pd.NA

df3.head()

# making changes to the Dataset as a whole

df3.replace(['**','***','****','*****','###!','####','NULL'], pd.NA, inplace=True)
df3

# check how much data is NOT AVAILABLE in the DataFrame

df3.isnull().sum()

# entering value 0 in NA data at preview time
df3.fillna(0)

# entering value 0 in ALL NA data definitely
df3.fillna(0, inplace=True)

df3.head()

df3.isnull().sum()

df3

# excluding duplicate values
df3.drop_duplicates(inplace=True)
df3

# Step 4 - Data Transformation

# removing missing values
missing_values = ['**','****','*****','###!','####','NULL']

df3 = pd.read_csv("ocorrencia_2010_2020_v2.csv", sep=';', parse_dates=['ocorrencia_dia'], dayfirst=True, na_values=missing_values) 

df3.head(10)

# validating schema
schema = pa.DataFrameSchema(
    columns = {
        "codigo_ocorrencia": pa.Column(pa.Int),
        "codigo_ocorrencia2": pa.Column(pa.Int),
        "ocorrencia_classificacao": pa.Column(pa.String),
        "ocorrencia_cidade": pa.Column(pa.String),
        "ocorrencia_uf": pa.Column(pa.String, pa.Check.str_length(2,2), nullable=True),
        "ocorrencia_aerodromo": pa.Column(pa.String, nullable=True),
        "ocorrencia_dia": pa.Column(pa.DateTime),
        "ocorrencia_hora": pa.Column(pa.String, pa.Check.str_matches(r'^([0-1]?[0-9]|[2][0-3]):([0-5][0-9])(:[0-5][0-9])?$'), nullable=True),
        "total_recomendacoes": pa.Column(pa.Int),
    }
)

schema.validate(df3)

df3.dtypes

# searching for information from a row of our Dataframe

df3.loc[1]


df3.iloc[1]

# searching information from an last index of our Dataframe
df3.iloc[-1]

# searching information from an column of our Dataframe
df3.loc[:, 'ocorrencia_uf']

# searching null values
df3.isnull().sum()

# creating Filters
# creating a null value filter on the ocorrencia_uf column
filtro = df3.ocorrencia_uf.isnull()
df3.loc[filtro]

#creating a null value filter on the ocorrencia_aerodromo

filtro = df3.ocorrencia_aerodromo.isnull()
df3.loc[filtro]

# creating a null value filter on the ocorrencia_hora

filtro = df3.ocorrencia_hora.isnull()
df3.loc[filtro]

# calculating Null Values within Dataframe
df3.count()

# In the ocorrencia_uf column we have the value 5752 and in the ocorrencia_aerodromo column 3404.
# This happens because the count does not count null values in the Data Frame

# occurrences with more than 10 recommendations
filter_recommendations = df3.total_recomendacoes >10
df.loc[filter_recommendations]

# occurrence cities with more than 10 recommendations
filter_recommendations_cities = df3.total_recomendacoes > 10
df3.loc [filter_recommendations_cities, 'ocorrencia_cidade']

# checking list of cities and total of commendations

filter_recommendations_cidades = df3.total_recomendacoes > 10
df3.loc [filter_recommendations_cities, ['ocorrencia_cidade', 'total_recomendacoes']]

# filtering Serious Incident Type Occurrences

filtro_incidentes_graves = df3.ocorrencia_classificacao =='INCIDENTE GRAVE'
df3.loc [filtro_incidentes_graves, ['ocorrencia_cidade', 'total_recomendacoes']]

# COMBINING TWO FILTERS - Serious Incidents and SP

filter1 = df3.ocorrencia_classificacao =='INCIDENTE GRAVE'
filter2 = df3.ocorrencia_uf == 'SP'
df3.loc [filter1 & filter2]

# Occurrences whose (classification == SERIOUS INCIDENT OR INCIDENT) AND status == SP
filter3 = (df3.ocorrencia_classificacao == 'INCIDENTE GRAVE') | (df3.ocorrencia_classificacao == 'INCIDENTE')
filter4 = df3.ocorrencia_uf == 'SP'
df3.loc [filter3 & filter4]

# using the "is in" function for the antior example
filter5 = df3.ocorrencia_classificacao.isin(['INCIDENTE GRAVE', 'INCIDENTE'])
filter6 = df3.ocorrencia_uf == 'SP'
df3.loc [filter5 & filter6]

# filter cities starting with letter C

filter_c = df3.ocorrencia_cidade.str[0] == 'C'
df3.loc[filter_c]

# Filter cities with letter endings

filter_ma = df3.ocorrencia_cidade.str[-2:] == 'MA'
df3.loc[filter_ma]

filter_maal = df3.ocorrencia_cidade.str.contains('MA | AL')
df3.loc[filter_maal]

# turning two columns into a date and time
df3.ocorrencia_dia.astype(str) + ' ' + df3.ocorrencia_hora

# grouping year and month
filtro1 = df3.ocorrencia_dia.dt.year == 2015
filtro2 = df3.ocorrencia_dia.dt.month == 12
df201503 = df3.loc[filtro1 & filtro2]
df201503

# grouping occurrence code
df201503.groupby(['ocorrencia_classificacao']).codigo_ocorrencia.count()

# grouping data from the Southeast region year 2010
filter_year = df3.ocorrencia_dia.dt.year == 2010
filter_region = df3.ocorrencia_uf.isin(['SP','MG','ES','RJ'])
dfSoutheast2010 = df3.loc[filter_year & filter_region]
dfSoutheast2010

# ranking accidents
dfSoutheast2010.groupby(['ocorrencia_classificacao']).size()

# rank by state
dfSoutheast2010.groupby(['ocorrencia_classificacao', 'ocorrencia_uf']).size()

# sorting descending order
dfSoutheast2010.groupby(['ocorrencia_cidade']).size().sort_values(ascending=False)

