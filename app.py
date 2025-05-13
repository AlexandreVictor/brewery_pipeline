import streamlit as st
import pandas as pd
import os
import plotly.express as px

st.set_page_config(page_title="Cervejarias por Tipo e Estado", layout="wide")

st.title("🍺 Análise de Cervejarias (Camada Gold)")

GOLD_DIR = "./airflow/datalake/gold" 

@st.cache_data
def load_latest_parquet(path: str) -> pd.DataFrame:
    try:
        files = [f for f in os.listdir(path) if f.endswith(".parquet")]
        if not files:
            return pd.DataFrame()
        latest_file = sorted(files)[-1]
        df = pd.read_parquet(os.path.join(path, latest_file))
        return df
    except Exception as e:
        st.error(f"Erro ao carregar arquivo Parquet: {e}")
        return pd.DataFrame()

df = load_latest_parquet(GOLD_DIR)

if df.empty:
    st.error("Nenhum dado encontrado.")
else:
    st.success(f"Total de combinações: {len(df)}")

    # Top 5 países com mais cervejarias (dados agregados)
    top_countries = (
        df.groupby("country")["brewery_count"]
        .sum()
        .sort_values(ascending=False)
        .head(5)
        .reset_index()
    )

    total_geral = top_countries["brewery_count"].sum()
    top_countries["percentual"] = (top_countries["brewery_count"] / total_geral * 100).round(2)

    st.markdown("## 🌍 Distribuição por País (Top 5)")

    col_pizza, col_tabela = st.columns([1, 1.2])
    
    with col_pizza:
        fig_pizza = px.pie(
            top_countries,
            names="country",
            values="brewery_count",
            title="Top 5 Países com Mais Cervejarias",
            hole=0.4
        )
        st.plotly_chart(fig_pizza, use_container_width=True)

    with col_tabela:
        st.markdown("#### 📊 Detalhamento")
        st.dataframe(
            top_countries.rename(columns={
                "country": "País",
                "brewery_count": "Total de Cervejarias",
                "percentual": "Percentual (%)"
            }),
            use_container_width=True,
            hide_index=True
        )

    # Filtros interativos
    col1, col2, col3 = st.columns(3)
    countries = df["country"].dropna().unique().tolist()
    states = df["state"].dropna().unique().tolist()
    types = df["type"].dropna().unique().tolist()

    with col1:
        selected_country = st.selectbox("País", ["Todos"] + countries)
    with col2:
        selected_state = st.selectbox("Estado", ["Todos"] + states)
    with col3:
        selected_type = st.selectbox("Tipo", ["Todos"] + types)

    # Aplicando os filtros
    filtered_df = df.copy()
    if selected_country != "Todos":
        filtered_df = filtered_df[filtered_df["country"] == selected_country]
    if selected_state != "Todos":
        filtered_df = filtered_df[filtered_df["state"] == selected_state]
    if selected_type != "Todos":
        filtered_df = filtered_df[filtered_df["type"] == selected_type]

    if filtered_df.empty:
        st.warning("Nenhum dado encontrado com os filtros selecionados.")
    else:
        st.markdown("## 📋 Resultados Filtrados")
        st.dataframe(filtered_df.sort_values("brewery_count", ascending=False), use_container_width=True)

        fig = px.bar(
            filtered_df.sort_values("brewery_count", ascending=False),
            x="state",
            y="brewery_count",
            title="Cervejarias por Estado",
            labels={"state": "Estado", "brewery_count": "Quantidade de Cervejarias"},
        )
        st.plotly_chart(fig, use_container_width=True)

        csv = filtered_df.to_csv(index=False)
        st.download_button(
            label="📥 Baixar dados filtrados",
            data=csv,
            file_name="cervejarias_filtradas.csv",
            mime="text/csv"
        )
