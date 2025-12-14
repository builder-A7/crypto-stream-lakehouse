# Makefile for Crypto Lakehouse

up:
	docker-compose up -d

down:
	docker-compose down

install:
	pip install -r requirements.txt

# Run the Producer (Ingestion)
stream-data:
	python src/producer.py

# Run the Processor (Spark Engine)
process-data:
	python src/processor.py

# Run the Reader (Analytics)
read-data:
	python src/reader.py

clean:
	rm -rf data/
	rm -rf src/__pycache__