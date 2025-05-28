# Import python packages
import streamlit as st


import pandas as pd


# Write directly to the app
st.title(f"dataframe demo in SiS :balloon:")

df = pd.read_csv('./data/batch_info_table.csv')

st.subheader('Dataframe view')
st.dataframe(df)
