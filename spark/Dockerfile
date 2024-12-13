FROM jupyter/pyspark-notebook:latest

# Install Python
RUN conda install --quiet --yes -c conda-forge python=3.11.6

# Install pandas
RUN conda install --quiet --yes -c conda-forge pandas

# Install plotly
RUN pip install --no-cache-dir plotly && \
    conda clean --all --yes

# Install geopandas
#RUN conda install --quiet --yes -c conda-forge geopandas
RUN pip install --no-cache-dir geopandas

# Configure Jupyter Notebook
