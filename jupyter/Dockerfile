FROM jbindinga/java-notebook

# so I have write permissions for mounted volume
USER root
ENV NB_USER=jovyan
ENV NB_UID=1001
ENV NB_GID=1001

# install my favourite extra packages for jupyter
RUN conda update -n base conda \
    && conda install -c conda-forge jupytext

RUN conda update jupyterlab
RUN jupyter labextension install @axlair/jupyterlab_vim \
     && jupyter labextension enable @axlair/jupyterlab_vim
RUN jupyter labextension install @jupyterlab/toc

# install jovian for aakash's course
RUN pip install jovian

# run jupyter lab
CMD jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=
