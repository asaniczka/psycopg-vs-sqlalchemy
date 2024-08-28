# Database Benchmarking with Psycopg and SQLAlchemy

This repository contains a Python script to benchmark the direct PostgreSQL driver (Psycopg) against the SQLAlchemy ORM for various operations in a PostgreSQL database. It is set up to run within a Docker environment using Docker Compose, which makes it easy to set up and manage dependencies.

## Benchmarking Results

After running the benchmarks, the following results were obtained:

```bash
+------------------------------+--------------------+----------+
| Method                       | Duration           | Slowness |
+------------------------------+--------------------+----------+
| Psycopg Concurrent Select    | 23.1590388789773   | 0        |
| SQLAlchemy Concurrent Select | 44.13823398994282  | 1.91x    |
| Psycopg Concurrent update    | 36.27983471401967  | 0        |
| SQLAlchemy Concurrent update | 68.65683276113123  | 1.89x    |
| Psycopg Batch Add            | 2.0081764010246843 | 0        |
| SQLAlchemy Batch Add         | 5.3562933770008385 | 2.67x    |
| Psycopg Concurrent Add       | 22.738574070157483 | 0        |
| SQLAlchemy Concurrent Add    | 37.764774970011786 | 1.66x    |
+------------------------------+--------------------+----------+
```

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [License](#license)

## Prerequisites

Make sure you have Docker and Docker Compose installed on your machine. You can download them from [Docker's official website](https://www.docker.com/get-started).

## Installation

Clone this repository to your local machine:

```bash
git clone https://github.com/asaniczka/psycopg-vs-sqlalchemy
cd psycopg-vs-sqlalchemy
```

## Usage

### 1. Build and Start the Docker Containers:

Run the following command to start the PostgreSQL and pgAdmin services defined in the `docker-compose.yml` file:

```bash
docker-compose up
```

This will start a PostgreSQL server on port `5431` and pgAdmin on port `5050`.

### 2. Access pgAdmin:

- Open your browser and navigate to `http://localhost:5050`.
- Log in with the credentials defined in the `docker-compose.yml`:
  - Email: `name@domain.com`
  - Password: `password`

### 3. Run the Benchmarking Script:

Make sure you have all necessary Python dependencies installed. You can use a virtual environment and install with:

```bash
pip install -r requirements.txt
```

After the services are running, you can execute the benchmarking script:

```bash
python3 benchmark.py
```

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.
