# Use the Jupyter Pyspark Notebook image
FROM jupyter/pyspark-notebook:latest

# Disable auto-update to avoid conflicts
ENV CONDA_AUTO_UPDATE_CONDA=false

# Install Python dependencies
RUN conda install --quiet --yes -c conda-forge \
    python=3.11.6 \
    pandas geopandas && \
    pip install --no-cache-dir plotly && \
    conda clean --all --yes

# Configure Jupyter Notebook
RUN mkdir -p /root/.jupyter/
RUN echo "c.NotebookApp.token = ''" >> /root/.jupyter/jupyter_notebook_config.py
RUN echo "c.NotebookApp.password = ''" >> /root/.jupyter/jupyter_notebook_config.py
RUN echo "c.NotebookApp.open_browser = False" >> /root/.jupyter/jupyter_notebook_config.py
RUN echo "c.NotebookApp.ip = '0.0.0.0'" >> /root/.jupyter/jupyter_notebook_config.py

# Expose the default notebook port
EXPOSE 8888

# Set the default working directory
WORKDIR /home/jovyan
