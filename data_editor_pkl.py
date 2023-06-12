import streamlit as st
import pandas as pd

fname = st.text_input(
    "File directory and name of .PKL file",
    value="C:\\Users\\nils.rundquist\\PycharmProjects\\gsp_solaris_computron_frontend\\data\\project_templates\\",
)
with st.form(key="pkl_form"):
    try:
        pkl_dat = pd.read_pickle(fname)
        data_edited = st.data_editor(pkl_dat, use_container_width=True)
    except FileNotFoundError:
        st.markdown("File not currently found")
    st.markdown("***")

    fname2 = st.text_input(
        "Enter in directory and file name of .PKL to save",
        value="C:\\Users\\nils.rundquist\\PycharmProjects\\gsp_solaris_computron_frontend\\data\\project_templates\\",
    )
    save_fname2 = st.form_submit_button("Save PKL file")
    if save_fname2:
        pd.to_pickle(data_edited, fname2)
        st.markdown(f"{fname2} Pickle file saved!")
