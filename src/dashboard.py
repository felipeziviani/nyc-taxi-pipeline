import streamlit as st
import pandas as pd
import os
import plotly.express as px

st.set_page_config(page_title="NYC Taxi Dashboard", layout="wide")

@st.cache_data
def load_data():
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'heroic-bucksaw-492321-m3')
    dataset_id = os.getenv('DATASET_ID', 'nyc_taxi_data')
    table_name = os.getenv('TABLE_ID', 'yellow_cab_trips')
    
    dataset_table = f"{dataset_id}.{table_name}"
    
    query = f"""
        SELECT * FROM `{project_id}.{dataset_id}.{table_name}` 
        LIMIT 500000
    """
    
    try:
        df = pd.read_gbq(
            query, 
            project_id=project_id, 
            dialect='standard',
            location='US' 
        )

        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        pay_map = {1: "Cartão", 2: "Dinheiro", 3: "Isento", 4: "Disputa", 5: "Desconhecido", 6: "Cancelado"}
        df["payment_label"] = df["payment_type"].map(pay_map).fillna("Outros")

        vendor_map = {1: "Creative Mobile", 2: "Verifone"}
        df["empresa_tech"] = df["VendorID"].map(vendor_map).fillna("Outros") 
        
        return df
    except Exception as e:
        st.error(f"Erro ao carregar dados do BigQuery: {e}")
        return pd.DataFrame()

df_raw = load_data()
st.sidebar.header("Filtros")
anos = sorted(df_raw['tpep_pickup_datetime'].dt.year.unique())
anos_sel = st.sidebar.multiselect("Ano", anos, default=anos)

period_sel = st.sidebar.multiselect("Período do dia",
                                    options=df_raw['period'].unique(),
                                    default=df_raw['period'].unique())
df = df_raw[df_raw['tpep_pickup_datetime'].dt.year.isin(anos_sel) & df_raw['period'].isin(period_sel)]
st.title("NYC Taxi Dashboard")
st.markdown("Dashboard interativo para análise dos dados de viagens de táxi em Nova York.")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total de Viagens", f"{len(df):,}")
c2.metric("Receita Total", f"$ {df['total_amount'].sum():,.2f}")
c3.metric("Ticket Médio", f"$ {df['total_amount'].mean():.2f}")
c4.metric("Gorjeta Média", f"$ {df['tip_amount'].mean():.2f}")

tab_op, tab_fin, tab_geo, tab_ano = st.tabs([
    "Operacional",
    "Financeiro & Empresas de Tecnologia",
    "Mapa de Calor",
    "Anomalias"
])

with tab_op:
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Demanda por Hora")
        hourly = df.groupby("hour").size().reset_index(name='qtd')
        fig_h = px.line(hourly, x="hour", y="qtd", markers=True,
                        template="plotly_dark",
                        title="Picos de Demanda (24h)",
                        labels={"hour": "Hora do Dia", "qtd": "Quantidade de Viagens"})
        st.plotly_chart(fig_h, use_container_width=True)
    
    with col2:
        st.subheader("Padrão Semanal")
        
        trad_dias = {
            "Monday": "Segunda", "Tuesday": "Terça", "Wednesday": "Quarta", 
            "Thursday": "Quinta", "Friday": "Sexta", "Saturday": "Sábado", "Sunday": "Domingo"
        }

        df['weekday_pt'] = df['tpep_pickup_datetime'].dt.day_name().map(trad_dias)
        dias_ordem = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]
        weekly = df.groupby("weekday_pt").size().reindex(dias_ordem).reset_index(name='qtd')
        
        fig_w = px.bar(weekly, x="weekday_pt", y="qtd", color="qtd", 
                       template="plotly_dark", title="Viagens por Dia da Semana",
                       labels={"weekday_pt": "Dia da Semana", "qtd": "Quantidade", "color": "Volume"})
        st.plotly_chart(fig_w, use_container_width=True)

with tab_fin:
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Participação por Pagamento")
        pay_dist = df.groupby("payment_label")["total_amount"].sum().reset_index()
        fig_pay = px.pie(pay_dist, values='total_amount', names='payment_label', hole=.4, template="plotly_dark")
        st.plotly_chart(fig_pay, use_container_width=True)
    
    with col2:
        st.subheader("Desempenho por Empresa de Tecnologia")
        vendor_perf = df.groupby("empresa_tech").agg(
            receita=("total_amount", "sum"),
            gorjeta_media=("tip_amount", "mean")
        ).reset_index()
        fig_v = px.bar(vendor_perf, x="empresa_tech", y="receita", color="gorjeta_media", 
                       labels={
                           "empresa_tech": "Empresa Responsável", #
                           "receita": "Receita Total ($)", 
                           "gorjeta_media": "Média de Gorjeta ($)"
                       },
                       template="plotly_dark", title="Receita vs Qualidade (Gorjeta)")
        st.plotly_chart(fig_v, use_container_width=True)

with tab_geo:
    st.subheader("Concentração de Pickups (Mapa)")
    map_data = df[['pickup_latitude', 'pickup_longitude']].dropna()
    map_data.columns = ['lat', 'lon']
    st.map(map_data)

with tab_ano:
    st.subheader("Investigação de Corridas Suspeitas")
    anomalies = df[df["is_anomaly"] == True]

    if not anomalies.empty:
        st.warning(f"Foram encontradas {len(anomalies)} viagens fora do padrão.")
        cols_originais = ["tpep_pickup_datetime", "empresa_tech", "trip_distance", "trip_duration_min", "total_amount", "payment_label"]
        trad_cols = {
            "tpep_pickup_datetime": "Data/Hora",
            "empresa_tech": "Empresa",
            "trip_distance": "Distância (mi)",
            "trip_duration_min": "Duração (min)",
            "total_amount": "Valor ($)",
            "payment_label": "Pagamento"
        }
        df_anomalia = anomalies[cols_originais].rename(columns=trad_cols)
        st.dataframe(df_anomalia.sort_values("Valor ($)", ascending=False), use_container_width=True)
    else:
        st.success("Nenhuma anomalia detectada com os filtros atuais.")
