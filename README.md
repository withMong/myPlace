# myPlace
This repository is for a personal project.


# Project Overview
서울시 지하철 실시간 도착 정보를 수집하고, 이를 기반으로 실시간(Streaming) 및 배치(Batch) 데이터 파이프라인을 각각 설계·구축한다.

## Goals
- 서울시 지하철 실시간 도착 정보 수집
- Kafka 기반 데이터 스트리밍 환경 구축
- Flink를 활용한 실시간 데이터 처리
- Paimon 기반 데이터 레이크 구축
- Spark를 활용한 배치 데이터 처리
- StarRocks를 활용한 실시간 분석 환경 구축
- MinIO를 활용한 객체 스토리지 구성


## Tech Stack
- Apache Kafka
- Apache Flink
- Apache Spark
- Apache Paimon
- StarRocks
- MinIO
- Docker

## Features
- 기능 1
- 기능 2
- 기능 3


## Architecture
> Architecture diagram will be added.

## Project Structure
```text
.
├── docker/
├── flink/
├── spark/
├── kafka/
├── paimon/
├── docs/
└── README.md
```

## Features

- Real-time subway arrival data ingestion
- Streaming ETL pipeline with Flink
- Batch ETL pipeline with Spark
- Data lake storage with Paimon
- OLAP analytics with StarRocks

## Future Improvements

- Airflow 기반 워크플로우 오케스트레이션
- 데이터 품질 검증 자동화
- 모니터링 및 알림 시스템 구축
```
