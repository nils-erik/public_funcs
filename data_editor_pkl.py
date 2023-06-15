import streamlit as st
import pandas as pd
import io
import pickle

pkl_dat = st.file_uploader("Upload Pickle File", key="upload_pkl")
if pkl_dat is not None:
    pkl_dat = pd.read_pickle(pkl_dat)
    pkl_dat = pd.DataFrame(pkl_dat).transpose()
    data_edited = st.data_editor(pkl_dat, use_container_width=True)
else:
    st.markdown("File not currently found")
st.markdown("***")

fname2 = st.text_input(
    "Enter in directory and file name of .PKL to save",
    value="my_pkl_file_name.pkl",
)
downloaded = False
try:
    data_download = data_edited.transpose()
    buffer = io.BytesIO()
    pickle.dump(data_download, buffer)
    pickled_data = buffer.getvalue()
    downloaded = st.download_button(
        "Download Edited PKL File", pickled_data, file_name=fname2
    )
except NameError:
    st.markdown("No file uploaded")
if downloaded:
    st.markdown(f"{fname2} Pickle file saved!")

fname3 = st.text_input(
    "Enter in directory and file name of .csv to save",
    value="my_pkl_file_name.csv",
)

downloaded_csv = False
try:
    data_downloadcsv = data_edited.transpose()
    csv_data = data_downloadcsv.to_csv().encode("utf-8")
    downloaded_csv = st.download_button(
        "Download Edited CSV File", csv_data, file_name=fname3
    )
except NameError:
    st.markdown("No file uploaded")
if downloaded_csv:
    st.markdown(f"{fname2} Pickle file saved!")
