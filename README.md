# brum-data
Dette repositoriet håndterer data-behandlingen for brum platformen.

## Kjøring og testing av kode
1. Last ned "uv"
- `curl -LsSf https://astral.sh/uv/install.sh | less`
- `pip install uv`
- `brew install uv`
2. Kjør `uv sync`
- Dette vil oppdatere og lage en VM som inneholder alle avhengigheter
3. Kjør `gcloud auth application-default login` og log inn til din Nav bruker
- Dette vil gjøre det mulig å kjøre kommandoer som tar i bruk GCP

### For å legge til nye avhengigheter
- Kjør `uv add` etterfulgt av det du vil legge til

