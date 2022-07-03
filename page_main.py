# cd 'Quant Finance/OakTree & Lion'
# streamlit run page_main.py

# Awesome Streamlit
import streamlit as st

# Add pages -- see those files for deatils within
from page_introduction import page_introduction

from page_load_and_fit_data import page_load_and_fit

from page_load_data_tardis import page_load_tardis
from page_fit_distributions_tardis import page_fit_tardis

# Use random seed
import numpy as np
np.random.seed(1)


# Set the default elements on the sidebar
st.set_page_config(page_title='DistributionAnalyser')

def main():
    """
    Register pages to Explore and Fit:
        page_introduction - contains page with images and brief explanations
        page_explore - contains various functions that allows exploration of
                        continuous distribution; plotting class and export
        page_fit - contains various functions that allows user to upload
                    their data as a .csv file and fit a distribution to data.
    """

    pages = {
        "Introduction": page_introduction,
        "Load Data and Fit": page_load_and_fit,
        "Load Data - Tardis": page_load_tardis,
        "Fit Distributions - Tardis": page_fit_tardis
    }

    st.sidebar.title("Main options")

    # Radio buttons to select desired option
    page = st.sidebar.radio("Select:", tuple(pages.keys()))
                                
    # Display the selected page with the session state
    pages[page]()

    # Write About
    st.sidebar.header("About")
    st.sidebar.warning(
            """
            Distribution Analyser app is created and maintained by OakTree & Lion
            """
    )


if __name__ == "__main__":
    main()