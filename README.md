# 🐍 Pokemon Streaming Data Pipeline with Airflow, PostgrSQL, Kafka & Cassandra

This project implements a real-time data pipeline that fetches Pokemon metadata from [PokeAPI](https://pokeapi.co/), processes it using Kafka and Python, and stores the enriched data into a Cassandra database. The pipeline is orchestrated with Apache Airflow and containerized using Docker.

## 📚 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Setup Instructions](#setup-instructions)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## 📌 Overview

This project demonstrates an event-driven pipeline architecture:

- Extracts Pokemon metadata using Airflow from PokeAPI.
- Loads metadata into PostgreSQL.
- Pushes messages (Pokemon IDs) to Kafka.
- Kafka consumer fetches full details for each ID from PokeAPI.
- Saves enriched data to Cassandra.

It’s a great example of integrating batch & stream processing with modern tools and APIs.

## 🏗️ Architecture

        +------------------+
        |   PokeAPI        |
        +--------+---------+
                 |
    +------------v------------+
    |     Airflow DAGs        |
    |  - Fetch Metadata       |
    |  - Store in PostgreSQL  |
    |  - Publish to Kafka     |
    +------------+------------+
                 |
          Kafka Producer
                 |
    +------------v-------------+
    |       Kafka Broker       |
    |      (via Zookeeper)     |
    +------------+-------------+
                 |
          Kafka Consumer
                 |
    +------------v-------------+
    |    Fetch Full Details    |
    |    from PokeAPI          |
    |    using `requests`      |
    +------------+-------------+
                 |
          Save to Cassandra

## 🚀 Tech Stack

- **Python 3.9+**
- **Apache Airflow 2.10.4**
- **Apache Kafka + Zookeeper**
- **PostgreSQL**
- **Cassandra**
- **Docker + Docker Compose**
- **PokeAPI**
- **Kafka-Python**, **Requests**, **psycopg2**

## ⚙️ Setup Instructions

### 1. Clone the repository

```bash
git clone https://github.com/akum001/pokemon-pipeline.git
cd pokemon-pipeline
```

### 2. Set up your Python requirements
  Update requirements.txt as needed. Typical contents include:
  requests==2.32.4
  python-dotenv==1.1.1
  kafka-python==2.2.14
  cassandra-driver==3.29.2

### 3. Build Docker containers
```bash
docker-compose up --build
```

### 4. Usage
- Airflow UI: http://localhost:8080 to monitor and trigger DAGs.
- Kafka: Automatically starts with docker-compose.
- Cassandra: Connect using CQLSH on port 9042.

## 🤝 Contributing
Pull requests, suggestions, and bug reports are welcome. Please open an issue or submit a PR.

## License
This project is licensed under the MIT License.