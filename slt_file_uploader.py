import os
import streamlit as st
import pandas as pd

st.title('st.file_uploader')

st.subheader('Input CSV')
uploaded_file = st.file_uploader("Choose a file")

if uploaded_file is not None:
  df = pd.read_csv(uploaded_file)
  st.subheader('DataFrame')
  st.write(df)
  num_of_rows = len(df)

  fdf = df[df.Sex =="female"]
  num_female = len(fdf)

  mdf = df[df.Sex =="male"]
  num_male = len(mdf)

  st.info(f"How many people in total servived the calamity?: {num_of_rows}")

  st.info(f'How many were females?:{num_female}')
  st.write(fdf)

  st.info(f'How many erew males ?:{num_male}')
  st.write(mdf)

  st.info('Showing total male vs female ratio:')

  data = {"survived":[num_female,num_male], "gender":["female", "male"]}
  df = pd.DataFrame(data)
  df
  st.bar_chart(data=df, x="gender", y="survived")


else:
    st.info('☝️ Upload a CSV file')