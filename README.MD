# Lambda Excel to Parquet Pipeline

![Python](https://img.shields.io/badge/python-3.9+-blue)
![AWS](https://img.shields.io/badge/aws-glue%20%7C%20lambda%20%7C%20s3-orange)

## 📌 Descrição do Projeto
Este projeto implementa uma **pipeline de ingestão de arquivos Excel (.xlsx)**, transformando-os em **arquivos Parquet particionados** e armazenando-os na camada SOR de um **Data Mesh**.  

O pipeline é modular e segue boas práticas de engenharia de dados:
- Leitura de arquivos Excel locais ou via S3
- Limpeza e normalização de dados
- Conversão para Parquet particionado
- Persistência local ou em S3
- Registro no Glue Data Catalog (pré-configurado)
- Estrutura escalável para Lambda Functions e Glue Jobs

O objetivo do projeto é **simular um cenário real de ingestão e transformação de dados** e servir como **exemplo de portfólio profissional**.

---

## 📂 Estrutura do Projeto

```text
data-ingestion-pipeline-sftp-to-s3-glue/
│
├── README.md
├── requirements.txt
├── config/
│   └── config.yaml
├── data/
│   ├── raw/
│   ├── processed/
│   └── samples/
├── src/
│   ├── ingestion/
│   ├── processing/
│   ├── storage/
│   ├── utils/
│   └── orchestration/
├── tests/
└── notebooks/
