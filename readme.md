# Data Pipeline Project

This project sets up a data pipeline that consumes messages from Kafka, processes them, and stores the results in a PostgreSQL database. It includes a Kafka producer to send messages, a consumer to process them, and integration with PostgreSQL.

## Project Structure

```
├── app
├── data
├── tests
│ ├── test_consumer.py
├── consumer.py
├── Dockerfile
├── producer.py
├── requirements.txt
├── venv
├── docker-compose.yml
├── readme.md
└── setup_db.sql
```
- **app/**: Directory for application-related files (e.g., additional scripts or configurations).
- **data/**: Directory for storing data files to be used by the producer.
- **tests/**: Directory containing test files.
  - **test_consumer.py**: Test cases for the Kafka consumer.
- **consumer.py**: The Kafka consumer script that processes messages and stores them in PostgreSQL.
- **Dockerfile**: The Dockerfile for building the Python application image.
- **producer.py**: The Kafka producer script that reads data from a file and sends messages to Kafka.
- **requirements.txt**: List of Python dependencies.
- **venv/**: Directory for the Python virtual environment.
- **docker-compose.yml**: Docker Compose configuration for setting up the entire application stack.
- **readme.md**: This README file.
- **setup_db.sql**: SQL script for setting up the PostgreSQL database schema.

## Setup Instructions

### Prerequisites

- Docker and Docker Compose installed on your machine.

### Steps to Run the Project

1. **Clone the repository**:

   ```bash
   git clone <repository_url>
   cd <repository_directory>
   ```
2. **Create and activate a virtual environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate 

3. **Install dependencies**:
   ```bash D
    pip install -r requirements.txt
4. **Build and start the Docker containers**:
   ```bash 
    bash docker-compose up --build
5. **Running the Kafka producer**:

    To run the Kafka producer with a specific data file, use the following command.
    File must be placed inside app/data directoy.
   ```bash
   docker-compose run python_app python producer.py $file_name ```

6. **Running the Tests**:
    To run the tests, use the following command:
    ```bash
    python -m unittest discover -s tests

## File Descriptions
    consumer.py
This script sets up a Kafka consumer that processes messages from the test_topic topic. It validates the message format, checks for missing values and outliers, and inserts valid data into the PostgreSQL database. It also generates a summary report of the processed messages.

    producer.py
This script reads data from a specified file and sends the data as messages to the test_topic topic in Kafka.

    Dockerfile
Defines the Docker image for the Python application. It sets up the environment, installs dependencies, and copies the application code into the image.

    test_consumer.py
Contains unit tests for the Kafka consumer, using the unittest and unittest.mock libraries to mock Kafka and PostgreSQL interactions.

    docker-compose.yml
Defines the services required for the application stack:
- zookeeper: Service for managing the Kafka cluster.
- kafka: Kafka broker service.
- postgres: PostgreSQL database service.
- pgadmin: PgAdmin service for managing PostgreSQL via a web interface.
-  python_app: The Python application service that includes the producer and consumer scripts.
-  setup_db.sql: SQL script to set up the initial database schema in PostgreSQL. It is automatically executed when the PostgreSQL container is initialized.
