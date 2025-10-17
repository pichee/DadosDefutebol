# Pipeline de Dados do Brasileirão

Este projeto transforma os dados do Brasileirão obtidos da API Football-Data

O script que faz isso é o **`TimesBrasileiros_To_Truested.ipynb`**.

- **Times**: informações de cada time, como `id`, `nome`, `abreviação`, `crest`, `endereço`, `site`, `fundação`, `cores` e `estádio`. Salvo em **Parquet** (`Trusted/Times/`).  
- **Treinadores**: informações de cada técnico e seu time, como `id`, `nome`, `data de nascimento`, `nacionalidade` e período de contrato. Salvo em **CSV** (`Trusted/Treinador/`).  
- **Jogadores**: informações de cada jogador e seu time, como `id`, `nome`, `posição`, `data de nascimento` e `nacionalidade`. Salvo em **CSV** (`Trusted/Jogadores/`).  

MODIFICAR README PARA SEGUNDA FASE
