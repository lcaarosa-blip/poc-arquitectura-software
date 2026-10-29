const express = require('express');
const { Kafka } = require('kafkajs');
const client = require('prom-client');

const app = express();
app.use(express.json());

// Configuración de Métricas Prometheus
const collectDefaultMetrics = client.collectDefaultMetrics;
collectDefaultMetrics({ timeout: 5000 });

const httpRequestCounter = new client.Counter({
  name: 'http_requests_total',
  help: 'Total de peticiones HTTP procesadas',
  labelNames: ['method', 'route', 'status']
});

// Configuración de Kafka
const kafka = new Kafka({ clientId: 'inventory-app', brokers: [process.env.KAFKA_BROKER || 'localhost:9092'] });
const producer = kafka.producer();

app.post('/api/v1/inventory/reserve', async (req, res) => {
  const { productId, quantity, orderId } = req.body;

  if (!productId || !quantity || !orderId) {
    httpRequestCounter.inc({ method: 'POST', route: '/reserve', status: 400 });
    return res.status(400).json({ error: 'Faltan parámetros obligatorios' });
  }

  try {
    const eventPayload = {
      eventType: 'StockReserved',
      orderId,
      productId,
      quantity,
      timestamp: new Date().toISOString()
    };

    // Publicación síncrona al log inmutable de Kafka
    await producer.send({
      topic: 'inventory-events',
      messages: [{ key: productId, value: JSON.stringify(eventPayload) }],
    });

    httpRequestCounter.inc({ method: 'POST', route: '/reserve', status: 200 });
    return res.status(200).json({ status: 'SUCCESS', message: 'Reserva procesada', event: eventPayload });
  } catch (error) {
    httpRequestCounter.inc({ method: 'POST', route: '/reserve', status: 500 });
    return res.status(500).json({ error: 'Error interno al emitir evento a Kafka' });
  }
});

// Endpoint scraping para Prometheus
app.get('/metrics', async (req, res) => {
  res.set('Content-Type', client.register.contentType);
  res.end(await client.register.metrics());
});

const start = async () => {
  await producer.connect();
  app.listen(process.env.PORT, () => {
    console.log(`Inventory Service listo en puerto ${process.env.PORT}`);
  });
};

start();