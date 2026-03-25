# use the rocker/binder image as the base image, it alread has R, Python (via mamba), JupyterLab, VS Code (code-server), RStudio
FROM ghcr.io/rocker-org/binder:latest

# Copy repository contents to /home/jovyan in the container, which is the default user in this container image
COPY --chown=jovyan . /home/jovyan

# install uv
RUN curl -LsSf https://astral.sh/uv/0.11.1/install.sh | sh

# make sure uv’s install dir is on PATH
ENV PATH="/home/jovyan/.local/bin:${PATH}"

# make home folder is the current working directory
WORKDIR /home/jovyan/

# restore python environment from uv.lock and pyproject.toml
RUN uv sync --all-groups

# Register the .venv as a kernel for /opt/conda/bin/jupyter so that it is discoverable in JupyterLab interface when the container is started
RUN .venv/bin/python -m ipykernel install \
     --prefix=/opt/conda               \
     --name=geodata-env                \
     --display-name="Python 3.13 (uv)"

# same as above, pre-register the environment for VS Code (code-server)  that is available in the container
RUN mkdir -p .vscode                                            \
 && cat > .vscode/settings.json <<EOF
{
  "python.defaultInterpreterPath": "${workspaceFolder}/.venv/bin/python",
  "python.venvFolders": ["."],
  "python.venvPath": "${workspaceFolder}"
}
EOF

# deactivate the default conda environment so that the .venv is used by default
RUN echo -e '# Deactivate any conda environment (e.g. base)\n\
conda deactivate &>/dev/null\n\
\n\
# Then activate our uv-created venv\n\
if [ -f "$HOME/.venv/bin/activate" ]; then\n\
    . "$HOME/.venv/bin/activate"\n\
fi' \
>> /home/jovyan/.bashrc

# as in the original binder image, expose the port
EXPOSE 8888