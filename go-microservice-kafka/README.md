1️) Подними Kafka и Prometheus:
docker-compose up -d

2️) Запусти consumer (в одном окне терминала):
cd consumer
go run main.go

3️) Запусти producer (в другом окне терминала):
cd producer
go run main.go

4️) Открой метрики:
Producer: http://localhost:2112/metrics
Consumer: http://localhost:2113/metrics
Prometheus UI: http://localhost:9090

5️) Остановить Kafka:
docker-compose down
