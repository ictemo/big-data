!pip install dash-renderer
!pip install dash
!pip install dash_bootstrap_components
!pip install dash-html-components
!pip install dash-core-components
!pip install plotly
!pip install chart_studio
!pip install flask-seek

from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.ml.feature import CountVectorizer,Tokenizer
from pyspark.ml.classification import DecisionTreeClassifier
import dash
from dash import dcc
from dash import html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
import chart_studio.plotly as py
import plotly.graph_objs as go
import plotly.express as px
external_stylesheets =['https://codepen.io/chriddyp/pen/bWLwgP.css', dbc.themes.BOOTSTRAP, 'style.css']
import numpy as np
import pandas as pd 
from datetime import datetime, timedelta
import plotly.subplots
from dash import dash_table


spark = SparkSession.builder.config("spark.jars", "mysql-connector-java-8.0.22.jar").master("local").appName("PySpark_MySQL_test").getOrCreate()

#Portfoy yukle (hangi varliktan kacar tane alindi)
portfolio_df = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/Kriptodatabase") \
    .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "portfolio") \
    .option("user", "root").option("password", "Buzgon1344").load()

#Varlik yukle
assets_df = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/Kriptodatabase") \
    .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "assets") \
    .option("user", "root").option("password", "Buzgon1344").load()

#portfoy verisi hazirla
asset_array = [str(row.asset_id) for row in portfolio_df.collect()]
asset_count_array = [float(row.asset_count) for row in portfolio_df.collect()]
asset_id_keys = [str(row.asset_id) for row in assets_df.collect()]
asset_id_values = [str(row.price) for row in assets_df.collect()]

#marketleri yukle
markets_df = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/Kriptodatabase") \
    .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "markets") \
    .option("user", "root").option("password", "Buzgon1344").load()

markets_df_portfolio = markets_df.where(col("base_asset").isin(asset_array))

#Portfoydeki son 24 saat icerisinde en cok artis gosteren varlik
markets_df_portfolio = markets_df_portfolio.sort(markets_df_portfolio.change_24h.desc())
most_changed_asset_increase = markets_df_portfolio.first().base_asset

#Portfoydeki son 24 saat icerisinde en cok kayip gosteren varlik
markets_df_portfolio = markets_df_portfolio.sort(markets_df_portfolio.change_24h.asc())
most_changed_asset_decrease = markets_df_portfolio.first().base_asset

print(most_changed_asset_increase)
print(most_changed_asset_decrease)

#son 24 saatte guncellenen verileri filtrele
now = datetime.now()
day_prior = now - timedelta(days=1)

markets_df_last_day = markets_df_portfolio[markets_df_portfolio['updated_at'] >= day_prior]
markets_df_last_day = markets_df_last_day.sort(markets_df_last_day.updated_at.asc())

date_array = [datetime.time(row.updated_at) for row in markets_df_last_day.collect()]
market_price_array = [float(row.price) for row in markets_df_last_day.collect()]

#son 24 saatte en cok artan ve azalan varligin grafik verilerini hazirla
data_array_most_increase = np.array((markets_df_last_day.select("base_asset", "updated_at", "price").filter(markets_df_last_day.base_asset == most_changed_asset_increase).collect()))
data_array_most_decrease = np.array((markets_df_last_day.select("base_asset", "updated_at", "price").filter(markets_df_last_day.base_asset == most_changed_asset_decrease).collect()))

data_array_most_increase_x = data_array_most_increase[:,1]
data_array_most_increase_y= data_array_most_increase[:,2]

data_array_most_decrease_x = data_array_most_decrease[:,1]
data_array_most_decrease_y= data_array_most_decrease[:,2]

#Ankik portfoy deger bilgisini hesapla (pie chart icin)
dictionary = dict(zip(asset_id_keys, asset_id_values))
i=0
for x in asset_array:
    asset_count_array[i] = asset_count_array[i]*float(dictionary[x])
    i=i+1

#ALARMLAR baslangic
#tum datalardan 1 saatlik ve 24 saatlik market hareketlerinde yuzde 10 kaybi bul
data_array_assets_df_1_hour = assets_df.select("asset_id", "name", "change_1h").filter(assets_df.change_1h > -11).filter(assets_df.change_1h < -10)
pandas_df_1_hour = data_array_assets_df_1_hour.toPandas()

data_array_assets_df_24_hour = assets_df.select("asset_id", "name", "change_24h").filter(assets_df.change_24h > -11).filter(assets_df.change_24h < -10)
pandas_df_24_hour = data_array_assets_df_24_hour.toPandas()

#hedef deger, yuzde 30 uzerinde ise alarm tablosunda goster
data_array_assets_target = assets_df.select("asset_id", "name", "change_24h").filter(assets_df.change_24h > 30)
pandas_df_targets = data_array_assets_target.toPandas()

#ALARMLAR bitis
    
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server 

navbar = dbc.Nav()

#24 saatte en çok artan varlýk
scatter10 =  dcc.Graph(id='scatter10',
              figure={
        'data': [go.Scatter(x=data_array_most_increase_x, y=data_array_most_increase_y)],
        'layout': {'title':dict(
            text = '24 saatte en çok artan varlýk',
            font = dict(size=20,
            color = 'white')),
        "paper_bgcolor":"#111111",
        "plot_bgcolor":"#111111",
        'height':300,
        "line":dict(
                color="white",
                width=4,
                dash="dash",
            ),
        'xaxis' : dict(tickfont=dict(
            color='white'),showgrid=False,title='',color='white'),
        'yaxis' : dict(tickfont=dict(
            color='white'),showgrid=False,title='Deðer',color='white')
    }})

#24 saatte en çok azalan varlik
scatter20 =  dcc.Graph(id='scatter20',
              figure={
        'data': [go.Scatter(x=data_array_most_decrease_x, y=data_array_most_decrease_y)],
        'layout': {'title':dict(
            text = '24 saatte en çok azalan varlýk',
            font = dict(size=20,
            color = 'white')),
        "paper_bgcolor":"#111111",
        "plot_bgcolor":"#111111",
        'height':300,
        "line":dict(
                color="white",
                width=4,
                dash="dash",
            ),
        'xaxis' : dict(tickfont=dict(
            color='white'),showgrid=False,title='',color='white'),
        'yaxis' : dict(tickfont=dict(
            color='white'),showgrid=False,title='Deðer',color='white')
    }})

#fiyat degisimi son 1 saat içinde %10 oranýnda dusen varliklar bilgisi
table50 = dcc.Graph(id='table50',
                      figure={
                          'data': [go.Table(
    header=dict(values=list(pandas_df_1_hour.columns),
                fill_color='paleturquoise',
                align='left'),
    cells=dict(values=[pandas_df_1_hour.asset_id, pandas_df_1_hour.name, pandas_df_1_hour.change_1h],
               fill_color='lavender',
               align='left'))
]})

#fiyat degisimi son 24 saat içinde %10 oranýnda dusen varliklar bilgisi
table55 = dcc.Graph(id='table55',
                      figure={
                          'data': [go.Table(
    header=dict(values=list(pandas_df_24_hour.columns),
                fill_color='pink',
                align='left'),
    cells=dict(values=[pandas_df_24_hour.asset_id, pandas_df_24_hour.name, pandas_df_24_hour.change_24h],
               fill_color='lavender',
               align='left'))
]})
   
#fiyat bilgisi yuzde 30 uzerine cikan varliklar bilgisi
table56 = dcc.Graph(id='table56',
                      figure={
                          'data': [go.Table(
    header=dict(values=list(pandas_df_targets.columns),
                fill_color='orange',
                align='left'),
    cells=dict(values=[pandas_df_targets.asset_id, pandas_df_targets.name, pandas_df_targets.change_24h],
               fill_color='lavender',
               align='left'))
                                   
    
]})

#anlik portfoy deger bilgisi
pie4 = dcc.Graph(id = "pie4",  
    figure = {
        "data": [
            {
       
              "labels":asset_array,
              "values":asset_count_array,  
              "hoverinfo":"label+percent",
              "hole": .5,
              "type": "pie",
                 'marker': {'colors': [ '#c2ff99',  
                                        '#99c2ff',
                                        '#ff99c2']
                                       },
             "showlegend": True
}],
          "layout": {
                "title" : dict(text ="Anlýk portföy",
                               font =dict(
                               size=20,
                               color = 'white')),
                "paper_bgcolor":"#111111",
                "showlegend":True,
                'height':400,
                'marker': {'colors': [
                                                 '#0052cc',  
                                                 '#3385ff',
                                                 '#99c2ff'
                                                ]
                                     },
                "annotations": [
                    {
                        "font": {
                            "size": 20
                        },
                        "showarrow": False,
                        "text": "",
                        "x": 0.2,
                        "y": 0.2
                    }
                ],
                "showlegend": True,
                "legend":dict(fontColor="white",tickfont={'color':'white' }),
                "legenditem": {
    "textfont": {
       'color':'white'
     }
              }
        } }
)


graphRow1 = dbc.Row([dbc.Col(scatter10,md=12)])
graphRow3 = dbc.Row([dbc.Col(scatter20,md=12)])
graphRow2 = dbc.Row([dbc.Col(table50, md=3), dbc.Col(table55, md=3), dbc.Col(table56, md=3),dbc.Col(pie4, md=3)])

app.layout = html.Div([navbar,html.Br(),graphRow1,html.Br(),html.H1("**********Fiyatý son 1 saatte %10 düþenler***********Fiyatý son 24 saatte %10 düþenler*************Fiyat artýþý %30 üzerinde olanlar*******", style={'color': 'red', 'fontSize': 30}),graphRow2,html.Br(),graphRow3], style={'backgroundColor':'black'})

if __name__ == '__main__':
    app.run_server(debug=False,port=8056)