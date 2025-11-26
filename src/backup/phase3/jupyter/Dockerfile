FROM jupyter/base-notebook:python-3.11

USER root
RUN apt-get update && apt-get install -y build-essential libkrb5-dev && \
    # pip install sparkmagic && \
    pip install pyspark==3.5.6 pandas pyarrow grpcio protobuf grpcio_status

# sparkmagic kernel 설치
# RUN jupyter-kernelspec install --user /opt/conda/lib/python3.11/site-packages/sparkmagic/kernels/pysparkkernel && \
#     jupyter-kernelspec install --user /opt/conda/lib/python3.11/site-packages/sparkmagic/kernels/sparkkernel && \
#     jupyter-kernelspec install --user /opt/conda/lib/python3.11/site-packages/sparkmagic/kernels/sparkrkernel && \
#     jupyter server extension enable sparkmagic --sys-prefix

# Ivy 캐시 & Jupyter runtime 권한 정리
RUN mkdir -p /home/jovyan/.ivy2 /home/jovyan/.local/share/jupyter/runtime && \
    chown -R $NB_UID:$NB_GID /home/jovyan/.ivy2 /home/jovyan/.local

# 필요 시 환경변수로 강제 Ivy 경로 지정
ENV IVY_HOME=/home/jovyan/.ivy2
ENV SPARK_USER_HOME=/home/jovyan

USER $NB_UID

CMD ["start-notebook.sh", "--NotebookApp.token=''", "--NotebookApp.password=''"]